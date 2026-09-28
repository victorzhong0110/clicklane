"""两阶段提交之后的故障重跑，以及不限速加压。

不写 docs/benchmark-realistic.md。数字进 docs/measurements-followup.json。
恢复秒数用 JobManager 日志，任务行不要求带 job id。
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import benchmark as bench  # noqa: E402
import benchmark_realistic as realistic  # noqa: E402

OUT = ROOT / "docs" / "measurements-followup.json"
EVENTS = realistic.EVENTS


def save(doc: dict) -> None:
    OUT.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    bench.log(f"wrote {OUT.name}")


def load() -> dict:
    if OUT.exists():
        return json.loads(OUT.read_text(encoding="utf-8"))
    return {"runs": {}}


def ensure_tm() -> None:
    overview = bench.http_json(bench.FLINK + "/overview")
    if int(overview.get("taskmanagers") or 0) >= 1:
        return
    bench.docker("start", "pulseboard-taskmanager", check=False)
    bench.wait_until(
        lambda: int(bench.http_json(bench.FLINK + "/overview").get("taskmanagers") or 0) >= 1,
        120,
        what="tm-up",
    )


def recreate(topic: str, partitions: int) -> None:
    bench.docker(
        "exec",
        "pulseboard-kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--delete",
        "--topic",
        topic,
        check=False,
    )
    time.sleep(1)
    bench.docker(
        "exec",
        "pulseboard-kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        str(partitions),
        "--replication-factor",
        "1",
        check=False,
    )


def submit(
    run_id: str,
    *,
    parallelism: int = 2,
    batch_size: int = 1000,
    flush_ms: int = 200,
    checkpoint_ms: int = 10000,
    dim: str = "/opt/data/dim_item_realistic.csv",
    forward_duplicates: bool = True,
) -> str:
    topic = f"events-{run_id}"
    completed = bench.docker(
        "exec",
        "pulseboard-jobmanager",
        "/opt/flink/bin/flink",
        "run",
        "-d",
        "-c",
        "io.pulseboard.pipeline.ClickstreamJob",
        "/opt/flink/jobs/pulseboard-pipeline.jar",
        "--kafka.bootstrap",
        "kafka:9092",
        "--kafka.topic",
        topic,
        "--kafka.group",
        f"pulseboard-{run_id}",
        "--run.id",
        run_id,
        "--clickhouse.endpoint",
        "http://clickhouse:8123",
        "--clickhouse.database",
        "pulseboard",
        "--dim.path",
        dim,
        "--window.ms",
        str(bench.WINDOW_MS),
        "--watermark.ms",
        str(bench.WATERMARK_MS),
        "--watermark.mode",
        "punctuated",
        "--watermark.interval.ms",
        "200",
        "--idle.sec",
        "20",
        "--topn",
        str(bench.TOP_N),
        "--parallelism",
        str(parallelism),
        "--batch.size",
        str(batch_size),
        "--flush.ms",
        str(flush_ms),
        "--forward.duplicates",
        "true" if forward_duplicates else "false",
        "--mode",
        "full",
        "--checkpoint.dir",
        "file:///opt/flink/checkpoints",
        "--checkpoint.ms",
        str(checkpoint_ms),
        timeout=180,
    )
    text = (completed.stdout or "") + (completed.stderr or "")
    match = re.search(r"JobID ([0-9a-f]+)", text)
    if not match:
        raise RuntimeError(f"flink submit failed: {text[-1500:]}")
    jid = match.group(1)
    ok = bench.wait_until(
        lambda: any(job.get("jid") == jid and job.get("state") == "RUNNING" for job in bench.jobs()),
        120,
        what="running",
    )
    if not ok:
        raise RuntimeError(f"job {jid} did not reach RUNNING: {bench.jobs()} {bench.job_exceptions(jid)}")
    return jid


def group_offsets(group: str) -> dict:
    completed = bench.docker(
        "exec",
        "pulseboard-kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--describe",
        "--group",
        group,
        check=False,
        timeout=30,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    current = 0
    end = 0
    lag = 0
    rows = 0
    pending = 0
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 6 or parts[0] != group:
            continue
        rows += 1
        if parts[3] == "-" or parts[4] == "-" or parts[5] == "-":
            pending += 1
            continue
        try:
            current += int(parts[3])
            end += int(parts[4])
            lag += int(parts[5])
        except ValueError:
            pending += 1
    return {
        "rows": rows,
        "current": current,
        "log_end": end,
        "lag": lag,
        "unassigned": pending,
        "raw_tail": "\n".join(text.splitlines()[-8:]),
    }


def vertex_metrics(jid: str) -> list[dict]:
    try:
        job = bench.http_json(f"{bench.FLINK}/jobs/{jid}")
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc)}]
    found = []
    interesting = (
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "backPressuredTimeMsPerSecond",
        "busyTimeMsPerSecond",
        "idleTimeMsPerSecond",
    )
    for vertex in job.get("vertices") or []:
        vid = vertex["id"]
        try:
            listed = bench.http_json(f"{bench.FLINK}/jobs/{jid}/vertices/{vid}/subtasks/metrics")
        except Exception as exc:  # noqa: BLE001
            found.append({"name": vertex.get("name"), "error": str(exc)})
            continue
        names = []
        for item in listed:
            metric_id = item.get("id") if isinstance(item, dict) else str(item)
            if metric_id.endswith(interesting):
                # 只要聚合值，不要每个 subtask 再来一遍。
                if "." not in metric_id.split(".metrics.")[-1] and metric_id.count(".") <= 2:
                    names.append(metric_id)
        if not names:
            names = [
                item.get("id")
                for item in listed
                if isinstance(item, dict) and item.get("id", "").endswith(interesting) and item.get("id", "")[:1].isdigit() is False
            ]
        names = names[:16]
        values = []
        if names:
            query = "&".join("get=" + urllib.parse.quote(name) for name in names)
            try:
                values = bench.http_json(f"{bench.FLINK}/jobs/{jid}/vertices/{vid}/subtasks/metrics?{query}")
            except Exception as exc:  # noqa: BLE001
                values = [{"error": str(exc)}]
        found.append({"name": vertex.get("name"), "parallelism": vertex.get("parallelism"), "metrics": values})
    return found


def checkpoint_now(jid: str) -> dict:
    payload = bench.checkpoints(jid)
    latest = payload.get("latest") or {}
    completed = latest.get("completed") or {}
    return {
        "counts": payload.get("counts"),
        "duration_ms": (completed.get("end_to_end_duration") if isinstance(completed, dict) else None),
        "state_size": completed.get("state_size") if isinstance(completed, dict) else None,
        "id": completed.get("id") if isinstance(completed, dict) else None,
    }


class DenseSampler:
    def __init__(self, jid: str, group: str):
        self.jid = jid
        self.group = group
        self.samples: list[dict] = []
        self.stop = False
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        next_metrics = 0.0
        while not self.stop:
            now = time.time()
            item = {"t": now, "offsets": group_offsets(self.group), "checkpoint": checkpoint_now(self.jid)}
            try:
                item["docker"] = realistic.docker_stats()
            except Exception as exc:  # noqa: BLE001
                item["docker_error"] = str(exc)
            if now >= next_metrics:
                item["metrics"] = vertex_metrics(self.jid)
                item["backpressure"] = realistic.sample_backpressure(self.jid)
                next_metrics = now + 8
            self.samples.append(item)
            bench.log(
                f"sample lag={item['offsets'].get('lag')} end={item['offsets'].get('log_end')} "
                f"chk={item['checkpoint'].get('id')} {item['checkpoint'].get('duration_ms')}"
            )
            time.sleep(2)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop = True
        self.thread.join(timeout=8)


def summarize_pressure(samples: list[dict]) -> dict:
    series = []
    for sample in samples:
        offsets = sample.get("offsets") or {}
        series.append(
            {
                "t": sample.get("t"),
                "lag": offsets.get("lag"),
                "current": offsets.get("current"),
                "log_end": offsets.get("log_end"),
                "checkpoint_ms": (sample.get("checkpoint") or {}).get("duration_ms"),
                "state_size": (sample.get("checkpoint") or {}).get("state_size"),
            }
        )
    usable = [row for row in series if isinstance(row.get("current"), int) and isinstance(row.get("t"), float)]
    consume = None
    lag_growth = None
    if len(usable) >= 2:
        dt = usable[-1]["t"] - usable[0]["t"]
        if dt > 0:
            consume = round((usable[-1]["current"] - usable[0]["current"]) / dt, 2)
            if isinstance(usable[-1].get("lag"), int) and isinstance(usable[0].get("lag"), int):
                lag_growth = round((usable[-1]["lag"] - usable[0]["lag"]) / dt, 2)
    lags = [row["lag"] for row in usable if isinstance(row.get("lag"), int)]
    docker_peak = realistic.summarize_stats(
        [{"rows": sample.get("docker")} for sample in samples if sample.get("docker")]
    )
    levels = []
    for sample in samples:
        for vertex in sample.get("backpressure") or []:
            if not isinstance(vertex, dict):
                continue
            level = vertex.get("backpressure-level")
            if level:
                levels.append({"name": vertex.get("name"), "level": level})
            for sub in vertex.get("subtasks") or []:
                if sub.get("backpressure-level") or sub.get("ratio") not in (None, 0, 0.0):
                    levels.append({"name": vertex.get("name"), "subtask": sub})
    durations = [
        (sample.get("checkpoint") or {}).get("duration_ms")
        for sample in samples
        if isinstance((sample.get("checkpoint") or {}).get("duration_ms"), (int, float))
    ]
    return {
        "samples": len(samples),
        "consume_events_per_s": consume,
        "lag_growth_per_s": lag_growth,
        "lag_max": max(lags) if lags else None,
        "lag_last": lags[-1] if lags else None,
        "checkpoint_duration_ms_max": max(durations) if durations else None,
        "docker_peak": docker_peak,
        "backpressure_nonzero": levels[:40],
        "series": series,
    }


def quantile_exact(run_id: str) -> dict:
    rows = bench.ch_rows(
        f"""
        SELECT
          count() AS n,
          min(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS min_ms,
          quantileExact(0.5)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS p50_ms,
          quantileExact(0.99)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS p99_ms,
          max(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS max_ms
        FROM pulseboard.ods_events FINAL
        WHERE run_id = '{run_id}'
        """
    )
    return rows[0] if rows else {"error": "no ods rows"}


def smoke(doc: dict) -> bool:
    run_id = "exact3-smoke"
    bench.log(f"=== {run_id}")
    ensure_tm()
    bench.cancel_running()
    recreate(f"events-{run_id}", 3)
    item: dict = {"kind": "smoke"}
    doc["runs"][run_id] = item
    try:
        jid = submit(run_id)
        item["job_id"] = jid
        proc, lines = realistic.start_replay(run_id, speedup=4000, limit=8000)
        item["replay"] = realistic.finish_replay(proc, lines, timeout=180)
        produced = int((item.get("replay") or {}).get("produced") or 0)
        item["settle"] = bench.settle(run_id, produced, timeout=180)
        item["exceptions"] = (bench.job_exceptions(jid) or "")[:1500]
        ods = bench.ch_rows(
            f"SELECT count() AS physical, (SELECT count() FROM pulseboard.ods_events FINAL WHERE run_id = '{run_id}') AS final FROM pulseboard.ods_events WHERE run_id = '{run_id}'"
        )
        item["ods"] = ods[0] if ods else None
        item["checkpoint"] = checkpoint_now(jid)
    except Exception as exc:  # noqa: BLE001
        item["error"] = str(exc)
    save(doc)
    final = int(((item.get("ods") or {}).get("final") or 0))
    item["ok"] = final > 0 and not item.get("error") and not (item.get("exceptions") or "").strip()
    save(doc)
    return bool(item["ok"])


def chaos_once(doc: dict, run_id: str, kind: str, speedup: float, py_rows: dict, file_dup_pv: int) -> None:
    bench.log(f"=== {run_id} {kind}")
    ensure_tm()
    item: dict = {"kind": kind, "speedup": speedup, "sink": "checkpoint-commit"}
    doc["runs"][run_id] = item
    try:
        bench.cancel_running()
        recreate(f"events-{run_id}", 3)
        jid = submit(run_id)
        item["job_id"] = jid
        started = time.perf_counter()
        proc, lines = realistic.start_replay(run_id, speedup, 0)
        ready = realistic.wait_checkpoint(jid, run_id, proc)
        item["checkpoint_before"] = ready
        if ready.get("note"):
            item["chaos"] = {"skipped": ready["note"]}
        else:
            since = time.time()
            states = []
            if kind == "tm":
                bench.docker("kill", "pulseboard-taskmanager", check=False)
                left = False
                t_kill = time.perf_counter()
                while time.perf_counter() - t_kill < 60:
                    state = bench.job_state(jid)
                    states.append({"after_s": round(time.perf_counter() - t_kill, 3), "state": state})
                    if state and state != "RUNNING":
                        left = True
                        break
                    time.sleep(1)
                bench.docker("start", "pulseboard-taskmanager", check=False)
                item["left_running_before_start"] = left
            else:
                bench.docker("restart", "pulseboard-clickhouse", check=False, timeout=180)
            realistic.service_back("tm" if kind == "tm" else "clickhouse")
            deadline = time.time() + 180
            while time.time() < deadline:
                state = bench.job_state(jid)
                states.append({"after_s": round(time.time() - since, 3), "state": state})
                overview = bench.http_json(bench.FLINK + "/overview")
                if state == "RUNNING" and int(overview.get("taskmanagers") or 0) >= 1:
                    break
                time.sleep(2)
            time.sleep(8)
            logs = realistic.jm_since(since)
            item["chaos"] = {
                "poll_states": states[-30:],
                "log": realistic.interpret_recovery(logs, jid),
                "poll_note": "恢复秒数以 log 为准。任务 INITIALIZING→RUNNING 行不要求含 job id。",
            }
        item["replay"] = realistic.finish_replay(proc, lines, timeout=900)
        produced = int((item.get("replay") or {}).get("produced") or 0)
        item["settle"] = bench.settle(run_id, produced, timeout=420)
        item["e2e_s"] = round(time.perf_counter() - started, 3)
        item["checkpoint_after"] = bench.checkpoint_brief(bench.checkpoints(jid))
        rt_full = bench.rt_tables(run_id)
        dup_row = bench.ch_rows(
            f"""
            SELECT count() AS duplicate_events, countIf(behavior = 'pv') AS duplicate_pv
            FROM pulseboard.dwd_duplicate_events FINAL
            WHERE run_id = '{run_id}'
            """
        )
        extra = dup_row[0] if dup_row else None
        rt_full, summary = realistic.slim_rt(rt_full, extra)
        item["rt_summary"] = summary
        item["quality"] = realistic.quality_counts(run_id)
        item["accuracy"] = realistic.accuracy(rt_full, py_rows, file_dup_pv)
        item["exceptions"] = (bench.job_exceptions(jid) or "")[:1500]
    except Exception as exc:  # noqa: BLE001
        item["error"] = str(exc)
        bench.log(f"{run_id} failed: {exc}")
    save(doc)
    try:
        realistic.forget_run(run_id)
    except Exception as exc:  # noqa: BLE001
        doc.setdefault("unfinished", []).append(f"删除 {run_id} 失败：{exc}")
        save(doc)


def run_chaos() -> None:
    doc = load()
    doc["sink"] = "notifyCheckpointComplete 才 INSERT，version=checkpointId"
    meta, py_rows = realistic.load_dataset()
    doc["dataset"] = {
        "rows": meta.get("rows"),
        "file_duplicate_pv": meta.get("file_duplicate_pv"),
        "speedup": meta.get("speedup"),
        "sha256_events": meta.get("sha256_events"),
    }
    save(doc)
    if not (doc["runs"].get("exact3-smoke") or {}).get("ok"):
        if not smoke(doc):
            bench.log("smoke failed, skip chaos")
            return
    speedup = float(meta["speedup"])
    file_dup_pv = int(meta["file_duplicate_pv"])
    for run_id, kind in (
        ("exact3-tm-1", "tm"),
        ("exact3-tm-2", "tm"),
        ("exact3-ch-1", "clickhouse"),
        ("exact3-ch-2", "clickhouse"),
    ):
        if (doc["runs"].get(run_id) or {}).get("accuracy"):
            bench.log(f"skip finished {run_id}")
            continue
        chaos_once(doc, run_id, kind, speedup, py_rows, file_dup_pv)
        doc = load()


def start_blasts(run_id: str, seconds: float, shards: int, loops: int) -> list[subprocess.Popen]:
    procs = []
    for shard in range(shards):
        cmd = [
            bench.PY,
            "-m",
            "replayer.blast",
            "--input",
            str(EVENTS),
            "--topic",
            f"events-{run_id}",
            "--run-id",
            run_id,
            "--shard",
            str(shard),
            "--shards",
            str(shards),
            "--seconds",
            str(seconds),
            "--loops",
            str(loops),
        ]
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(proc)
    return procs


def finish_blasts(procs: list[subprocess.Popen]) -> list[dict]:
    reports = []
    for proc in procs:
        try:
            out, _ = proc.communicate(timeout=180)
        except Exception as exc:  # noqa: BLE001
            proc.kill()
            reports.append({"error": str(exc)})
            continue
        report = {"exit": proc.returncode, "tail": (out or "")[-500:]}
        for line in (out or "").splitlines():
            if line.startswith("PULSEBOARD_BLAST "):
                report["blast"] = json.loads(line[len("PULSEBOARD_BLAST ") :])
        reports.append(report)
    return reports


def saturate(label: str, parallelism: int, batch_size: int, partitions: int, seconds: float, shards: int) -> None:
    doc = load()
    run_id = f"saturate-{label}"
    bench.log(f"=== {run_id}")
    ensure_tm()
    item = {
        "kind": "saturate",
        "parallelism": parallelism,
        "batch_size": batch_size,
        "partitions": partitions,
        "seconds": seconds,
        "shards": shards,
        "sink": "checkpoint-commit",
    }
    doc["runs"][run_id] = item
    save(doc)
    try:
        bench.cancel_running()
        recreate(f"events-{run_id}", partitions)
        jid = submit(run_id, parallelism=parallelism, batch_size=batch_size)
        item["job_id"] = jid
        # 先一轮不重复的分片，看单遍 70 万能不能打满。
        procs = start_blasts(run_id, seconds=seconds, shards=shards, loops=1)
        with DenseSampler(jid, f"pulseboard-{run_id}") as sampler:
            item["pass1"] = finish_blasts(procs)
            time.sleep(12)
            summary1 = summarize_pressure(sampler.samples)
        item["pressure_pass1"] = {key: value for key, value in summary1.items() if key != "series"}
        item["series_pass1"] = summary1["series"]
        lag_max = summary1.get("lag_max") or 0
        saturated = lag_max >= 20000 or bool(summary1.get("backpressure_nonzero"))
        item["pass1_saturated"] = saturated
        if not saturated:
            bench.log("pass1 did not saturate, looping producers")
            procs = start_blasts(run_id, seconds=seconds, shards=shards, loops=0)
            with DenseSampler(jid, f"pulseboard-{run_id}") as sampler:
                time.sleep(seconds)
                # 采样线程在跑；到点后结束生产者。
                for proc in procs:
                    if proc.poll() is None:
                        continue
                item["pass2"] = finish_blasts(procs)
                summary2 = summarize_pressure(sampler.samples)
            item["pressure_pass2"] = {key: value for key, value in summary2.items() if key != "series"}
            item["series_pass2"] = summary2["series"]
        # 等消费把积压吃掉再量延迟，同时保留加压期间的 lag。
        drain_deadline = time.time() + 180
        last_lag = None
        while time.time() < drain_deadline:
            offsets = group_offsets(f"pulseboard-{run_id}")
            last_lag = offsets.get("lag")
            bench.log(f"drain lag={last_lag}")
            if isinstance(last_lag, int) and last_lag < 1000 and offsets.get("log_end", 0) > 0:
                break
            time.sleep(3)
        item["drain_lag"] = last_lag
        time.sleep(15)
        item["latency_quantileExact"] = quantile_exact(run_id)
        item["checkpoint_after"] = checkpoint_now(jid)
        ods = bench.ch_rows(
            f"""
            SELECT count() AS physical,
              (SELECT count() FROM pulseboard.ods_events FINAL WHERE run_id = '{run_id}') AS final
            FROM pulseboard.ods_events WHERE run_id = '{run_id}'
            """
        )
        item["ods"] = ods[0] if ods else None
        item["exceptions"] = (bench.job_exceptions(jid) or "")[:1500]
    except Exception as exc:  # noqa: BLE001
        item["error"] = str(exc)
        bench.log(f"{run_id} failed: {exc}")
    save(doc)
    try:
        realistic.forget_run(run_id)
    except Exception as exc:  # noqa: BLE001
        doc.setdefault("unfinished", []).append(f"删除 {run_id} 失败：{exc}")
        save(doc)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: followup_bench.py chaos|saturate", file=sys.stderr)
        return 2
    cmd = argv[1]
    if cmd == "chaos":
        run_chaos()
        return 0
    if cmd == "saturate":
        label = argv[2] if len(argv) > 2 else "before"
        parallelism = int(argv[3]) if len(argv) > 3 else 2
        batch_size = int(argv[4]) if len(argv) > 4 else 1000
        partitions = int(argv[5]) if len(argv) > 5 else 3
        seconds = float(argv[6]) if len(argv) > 6 else 40
        shards = int(argv[7]) if len(argv) > 7 else 4
        saturate(label, parallelism, batch_size, partitions, seconds, shards)
        return 0
    print(f"unknown {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
