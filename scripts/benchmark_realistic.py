"""按真实事件时间加速回放，测吞吐、突发延迟、对账、混沌和一次优化。

数字只来自这次运行。缺的字段写成「未测到」和原因。
延迟用 quantileExact。恢复时间只认 JobManager 日志里的状态切换。
"""

from __future__ import annotations

import csv
import json
import re
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import benchmark as bench  # noqa: E402
from batch.metrics import compute  # noqa: E402

OUT_JSON = ROOT / "docs" / "measurements-realistic.json"
OUT_MD = ROOT / "docs" / "benchmark-realistic.md"
EVENTS = ROOT / "data" / "realistic" / "events.csv"
DIM = ROOT / "data" / "dim" / "dim_item_realistic.csv"
META = ROOT / "data" / "realistic" / "meta.json"

WALL_S = 150.0
FAST_WALL_S = 70.0
SEED = 11
SERVICES = [
    ("pulseboard-kafka", 0.50),
    ("pulseboard-clickhouse", 1.00),
    ("pulseboard-jobmanager", 0.40),
    ("pulseboard-taskmanager", 1.50),
    ("pulseboard-grafana", 0.25),
]


BIG_TABLES = (
    "ods_events",
    "ads_pv_uv",
    "ads_funnel",
    "ads_top_items",
    "ads_category_pv",
    "dwd_late_events",
    "dwd_invalid_events",
    "dwd_duplicate_events",
    "ads_reconcile",
)


def recreate_topic(topic: str) -> None:
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
    bench.create_topic(topic)


def forget_run(run_id: str) -> None:
    """测完就删掉这个 run 的行。

    ClickHouse 进程上限大约 763 MiB。两轮 70 万行叠在一起时，FINAL 查询会报
    MEMORY_LIMIT_EXCEEDED。指标已经写入 JSON 之后再删，下一轮才能在同一配额下跑完。
    """
    for table in BIG_TABLES:
        bench.ch(
            f"ALTER TABLE pulseboard.{table} DELETE WHERE run_id = '{run_id}' SETTINGS mutations_sync = 1"
        )
        bench.log(f"forgot {run_id} {table}")


def save(doc: dict) -> None:
    OUT_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_MD.write_text(render(doc), encoding="utf-8")
    bench.log(f"wrote {OUT_MD.name}")


def load_dataset() -> tuple[dict, dict]:
    meta = json.loads(META.read_text(encoding="utf-8"))
    events = []
    dim = {}
    seen = set()
    file_dup = 0
    file_dup_pv = 0
    hourly = Counter()
    with DIM.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            dim[int(row["item_id"])] = row["category_code"] or "UNKNOWN"
    with EVENTS.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            behavior = row["behavior"]
            ts = int(row["event_time_ms"])
            key = (int(row["user_id"]), int(row["item_id"]), behavior, ts)
            if key in seen:
                file_dup += 1
                if behavior == "pv":
                    file_dup_pv += 1
            else:
                seen.add(key)
            hour = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%H")
            hourly[hour] += 1
            events.append(
                {
                    "user_id": int(row["user_id"]),
                    "item_id": int(row["item_id"]),
                    "behavior": behavior,
                    "event_time_ms": ts,
                }
            )
    started = time.perf_counter()
    result = compute(events, bench.WINDOW_MS, bench.TOP_N, dim)
    result["elapsed_s"] = round(time.perf_counter() - started, 3)
    meta = dict(meta)
    meta["hourly"] = [{"hour_utc": hour, "events": hourly[hour]} for hour in sorted(hourly)]
    if result["duplicate_events"] != file_dup:
        raise RuntimeError(f"file duplicate count {file_dup} != metrics {result['duplicate_events']}")
    meta["file_duplicate_events"] = file_dup
    meta["file_duplicate_pv"] = file_dup_pv
    meta["wall_s"] = WALL_S
    meta["speedup"] = meta["span_ms"] / (WALL_S * 1000.0)
    meta["fast_wall_s"] = FAST_WALL_S
    meta["fast_speedup"] = meta["span_ms"] / (FAST_WALL_S * 1000.0)
    return meta, bench.normalize_python(result)


def inspect_limits() -> dict:
    out = {}
    for name, requested in SERVICES:
        nano = bench.capture([*bench.DOCKER, "inspect", "-f", "{{.HostConfig.NanoCpus}}", name])
        memory = bench.capture([*bench.DOCKER, "inspect", "-f", "{{.HostConfig.Memory}}", name])
        cpu_max = bench.capture([*bench.DOCKER, "exec", name, "sh", "-c", "cat /sys/fs/cgroup/cpu.max 2>/dev/null || true"])
        quota = bench.capture(
            [*bench.DOCKER, "exec", name, "sh", "-c", "cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null || true"]
        )
        try:
            nano_i = int(str(nano).strip() or "0")
        except ValueError:
            nano_i = None
        try:
            mem_i = int(str(memory).strip() or "0")
        except ValueError:
            mem_i = None
        out[name] = {
            "requested_cpus": requested,
            "nano_cpus": nano_i,
            "enforced_cpus": None if nano_i is None else nano_i / 1e9,
            "memory_bytes": mem_i,
            "cgroup_cpu_max": cpu_max.strip(),
            "cgroup_cpu_quota_us": quota.strip(),
            "cpu_quota_enforced": bool(nano_i),
        }
    return out


def paced_command(run_id: str, speedup: float, limit: int = 0) -> list[str]:
    cmd = [
        bench.PY,
        "-m",
        "replayer.replay",
        "--bootstrap",
        "127.0.0.1:19092",
        "--topic",
        f"events-{run_id}",
        "--run-id",
        run_id,
        "--input",
        str(EVENTS),
        "--speedup",
        f"{speedup:.6f}",
        "--seed",
        str(SEED),
        "--jitter-ratio",
        "0.05",
        "--jitter-ms",
        "30000",
        "--late-ratio",
        "0.005",
        "--late-min-ms",
        "180000",
        "--late-max-ms",
        "600000",
        "--duplicate-ratio",
        "0.002",
        "--malformed",
        "50" if limit == 0 else "10",
        "--partitions",
        "3",
        "--window-ms",
        str(bench.WINDOW_MS),
        "--watermark-ms",
        str(bench.WATERMARK_MS),
        "--retries",
        "20",
        "--delivery-timeout-ms",
        "120000",
        "--request-timeout-ms",
        "30000",
        "--max-block-ms",
        "120000",
        "--kick",
    ]
    if limit:
        cmd.extend(["--limit", str(limit)])
    return cmd


def submit(run_id: str, watermark_mode: str, batch_size: int, flush_ms: int) -> str:
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
        "/opt/data/dim_item_realistic.csv",
        "--window.ms",
        str(bench.WINDOW_MS),
        "--watermark.ms",
        str(bench.WATERMARK_MS),
        "--watermark.mode",
        watermark_mode,
        "--watermark.interval.ms",
        "200",
        "--idle.sec",
        "20",
        "--topn",
        str(bench.TOP_N),
        "--parallelism",
        "2",
        "--batch.size",
        str(batch_size),
        "--flush.ms",
        str(flush_ms),
        "--forward.duplicates",
        "true",
        "--mode",
        "full",
        "--checkpoint.dir",
        "file:///opt/flink/checkpoints",
        "--checkpoint.ms",
        "10000",
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


def start_replay(run_id: str, speedup: float, limit: int = 0):
    lines: list[str] = []
    proc = __import__("subprocess").Popen(
        paced_command(run_id, speedup, limit),
        cwd=ROOT,
        text=True,
        stdout=__import__("subprocess").PIPE,
        stderr=__import__("subprocess").STDOUT,
    )

    def _read() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)
            bench.log(line.rstrip())

    threading.Thread(target=_read, daemon=True).start()
    return proc, lines


class StatSampler:
    def __init__(self) -> None:
        self.samples: list[dict] = []
        self.stop = False
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        while not self.stop:
            self.samples.append({"t": time.time(), "rows": docker_stats()})
            time.sleep(12)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop = True
        self.thread.join(timeout=5)


def docker_stats() -> list[dict]:
    text = bench.capture([*bench.DOCKER, "stats", "--no-stream", "--format", "{{json .}}"], timeout=25)
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def sample_backpressure(jid: str) -> list[dict]:
    try:
        job = bench.http_json(f"{bench.FLINK}/jobs/{jid}")
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc)}]
    found = []
    for vertex in job.get("vertices") or []:
        url = f"{bench.FLINK}/jobs/{jid}/vertices/{vertex['id']}/backpressure"
        try:
            payload = bench.http_json(url)
            for _ in range(6):
                if payload.get("status") in (None, "ok", "deprecated"):
                    break
                time.sleep(1.5)
                payload = bench.http_json(url)
        except Exception as exc:  # noqa: BLE001
            found.append({"name": vertex.get("name"), "error": str(exc)})
            continue
        subs = []
        for sub in payload.get("subtasks") or []:
            subs.append(
                {
                    "subtask": sub.get("subtask"),
                    "backpressure-level": sub.get("backpressure-level") or sub.get("backpressureLevel"),
                    "ratio": sub.get("ratio") if "ratio" in sub else sub.get("backpressure-ratio"),
                    "idleRatio": sub.get("idleRatio"),
                    "busyRatio": sub.get("busyRatio"),
                }
            )
        found.append(
            {
                "name": vertex.get("name"),
                "status": payload.get("status"),
                "backpressure-level": payload.get("backpressure-level"),
                "subtasks": subs,
            }
        )
    return found


def quantile_exact_latency(run_id: str) -> dict:
    per_second = bench.ch_rows(
        f"""
        SELECT toUnixTimestamp(toStartOfSecond(inserted_at)) AS sec, count() AS c
        FROM pulseboard.ods_events FINAL
        WHERE run_id = '{run_id}'
        GROUP BY sec
        ORDER BY sec
        """
    )
    if not per_second:
        return {"error": "ODS 没有行，无法算延迟"}
    # 突发和安静的门槛用 quantileExact，下面的延迟也用 quantileExact。
    burst_min = bench.ch_rows(
        f"""
        SELECT quantileExact(0.9)(c) AS burst_min, quantileExact(0.5)(c) AS quiet_max
        FROM (
          SELECT count() AS c
          FROM pulseboard.ods_events FINAL
          WHERE run_id = '{run_id}'
          GROUP BY toStartOfSecond(inserted_at)
        )
        """
    )[0]
    burst_secs = [int(row["sec"]) for row in per_second if int(row["c"]) >= int(burst_min["burst_min"])]
    quiet_secs = [int(row["sec"]) for row in per_second if int(row["c"]) <= int(burst_min["quiet_max"])]

    def lat(where_secs: list[int] | None) -> dict:
        where = ""
        if where_secs is not None:
            if not where_secs:
                return {"n": 0}
            joined = ",".join(str(sec) for sec in where_secs)
            where = f"AND toUnixTimestamp(toStartOfSecond(inserted_at)) IN ({joined})"
        rows = bench.ch_rows(
            f"""
            SELECT
              count() AS n,
              min(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS min_ms,
              quantileExact(0.5)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS p50_ms,
              quantileExact(0.99)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS p99_ms,
              max(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS max_ms
            FROM pulseboard.ods_events FINAL
            WHERE run_id = '{run_id}' {where}
            """
        )
        return rows[0] if rows else {"n": 0}

    ads = bench.ch_rows(
        f"""
        SELECT
          count() AS windows,
          min(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS min_ms,
          quantileExact(0.5)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS p50_ms,
          quantileExact(0.99)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS p99_ms,
          max(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS max_ms
        FROM pulseboard.ads_pv_uv FINAL
        WHERE run_id = '{run_id}'
        """
    )
    sustain = bench.ch_rows(
        f"""
        SELECT
          count() AS seconds,
          quantileExact(0.5)(c) AS p50,
          round(avg(c), 2) AS avg,
          max(c) AS max
        FROM (
          SELECT count() AS c
          FROM pulseboard.ods_events FINAL
          WHERE run_id = '{run_id}'
          GROUP BY toStartOfSecond(inserted_at)
        )
        """
    )
    return {
        "ods_overall": lat(None),
        "ods_burst": lat(burst_secs),
        "ods_quiet": lat(quiet_secs),
        "burst_seconds": len(burst_secs),
        "quiet_seconds": len(quiet_secs),
        "burst_min_count": burst_min["burst_min"],
        "quiet_max_count": burst_min["quiet_max"],
        "ingest_per_second": sustain[0] if sustain else None,
        "ads_freshness_exact": ads[0] if ads else None,
    }


def quality_counts(run_id: str) -> dict:
    invalid = bench.ch_rows(
        f"""
        SELECT reason, count() AS c
        FROM pulseboard.dwd_invalid_events FINAL
        WHERE run_id = '{run_id}'
        GROUP BY reason
        ORDER BY reason
        """
    )
    dup = bench.ch_rows(
        f"""
        SELECT count() AS duplicate_events, countIf(behavior = 'pv') AS duplicate_pv
        FROM pulseboard.dwd_duplicate_events FINAL
        WHERE run_id = '{run_id}'
        """
    )
    return {"invalid_by_reason": invalid, "duplicates": dup[0] if dup else None}


def pv_identity(rt: dict, batch_pv: float, file_dup_pv: int) -> dict:
    ads_pv = sum(bench.num(row["pv"]) for row in rt["pv_uv"])
    late_pv = bench.num((rt.get("late") or {}).get("late_pv") or 0)
    dup_pv = bench.num((rt.get("duplicate_pv_row") or {}).get("duplicate_pv") or 0)
    # 侧输出里的重复 PV 含文件里原有的自然键重复和注入的重复。
    # 离线 PV 含文件重复、不含注入重复。两边相减后应等于“第一次出现的 PV”。
    left = ads_pv + late_pv - dup_pv
    right = batch_pv - file_dup_pv
    return {
        "ads_pv": ads_pv,
        "late_pv": late_pv,
        "duplicate_pv": dup_pv,
        "batch_pv": batch_pv,
        "file_duplicate_pv": file_dup_pv,
        "first_seen_streaming": left,
        "first_seen_batch": right,
        "gap": left - right,
        "exact": left == right,
    }


def slim_rt(rt: dict, extra_dup: dict | None) -> dict:
    rt = dict(rt)
    rt["duplicate_pv_row"] = extra_dup
    summary = {
        "windows": len(rt.get("pv_uv") or []),
        "late": rt.get("late"),
        "duplicates": rt.get("duplicates"),
        "duplicate_pv": extra_dup,
        "ods_physical": rt.get("ods_physical"),
        "ods_final": rt.get("ods_final"),
    }
    return rt, summary


def accuracy(rt_full: dict, py_rows: dict, file_dup_pv: int) -> dict:
    vs = bench.diff_bundle(rt_full, py_rows)
    batch_pv = sum(bench.num(row["pv"]) for row in py_rows["pv_uv"])
    return {"vs_python": vs, "pv_identity": pv_identity(rt_full, batch_pv, file_dup_pv)}


def finish_replay(proc, lines, timeout: int) -> dict:
    try:
        code = proc.wait(timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        proc.kill()
        return {"error": f"replay timeout/kill: {exc}", "tail": "".join(lines)[-1500:]}
    text = "".join(lines)
    try:
        report = bench.parse_replay(text)
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "exit_code": code, "tail": text[-1500:]}
    report["exit_code"] = code
    return report


def jm_since(since_epoch: float) -> str:
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since_epoch - 2))
    return bench.capture([*bench.DOCKER, "logs", "--timestamps", "--since", stamp, "pulseboard-jobmanager"], timeout=60)


def interpret_recovery(log_text: str, jid: str) -> dict:
    # 作业状态行带 job id。算子从 INITIALIZING 到 RUNNING 的行通常不带 job id，
    # 不能用 jid 把它们滤掉，否则恢复时间只剩作业级的 5 秒重启延迟。
    selected = [
        line
        for line in log_text.splitlines()
        if jid in line or "INITIALIZING to RUNNING" in line
    ]
    interesting = [
        line
        for line in selected
        if any(
            token in line
            for token in (
                "switched from state",
                "Restoring",
                "restor",
                "FAILED",
                "INITIALIZING to RUNNING",
            )
        )
    ]

    def stamp(line: str):
        match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z", line)
        if not match:
            return None
        head, frac = match.group(1).split(".")
        return datetime.fromisoformat(head + "." + frac[:6]).replace(tzinfo=timezone.utc)

    def first_time(pred):
        for line in interesting:
            if pred(line):
                return stamp(line), line
        return None, None

    def last_time(pred):
        found = (None, None)
        for line in interesting:
            if pred(line):
                found = (stamp(line), line)
        return found

    restarting_at, restarting_line = first_time(lambda line: "RUNNING to RESTARTING" in line)
    job_running_at, job_running_line = last_time(lambda line: "RESTARTING to RUNNING" in line)
    restore_at, restore_line = last_time(lambda line: "Restoring" in line)
    task_at, task_line = last_time(lambda line: "INITIALIZING to RUNNING" in line)
    cycles = sum(1 for line in interesting if "RUNNING to RESTARTING" in line)

    def delta(a, b):
        if a is None or b is None:
            return None
        return round((b - a).total_seconds(), 3)

    return {
        "restarting_cycles": cycles,
        "running_to_restarting": None if restarting_at is None else restarting_at.isoformat(),
        "job_restarting_to_running_s": delta(restarting_at, job_running_at),
        "restarting_to_last_task_running_s": delta(restarting_at, task_at),
        "restoring_line": restore_line,
        "restarting_line": restarting_line,
        "job_running_line": job_running_line,
        "last_task_running_line": task_line,
        "interesting_lines": interesting[-80:],
        "log_time_missing": restarting_at is None,
    }


def wait_checkpoint(jid: str, run_id: str, proc, timeout: int = 120) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline and proc.poll() is None:
        payload = bench.checkpoints(jid)
        completed = (payload.get("counts") or {}).get("completed") or 0
        ods = bench.ch_rows(f"SELECT count() AS c FROM pulseboard.ods_events WHERE run_id = '{run_id}'")
        count = int((ods[0]["c"] if ods else 0) or 0)
        last = {"completed": completed, "ods": count, "brief": bench.checkpoint_brief(payload)}
        bench.log(f"chaos wait checkpoint={completed} ods={count}")
        if completed >= 1 and count >= 30000:
            return last
        time.sleep(3)
    last["note"] = "未测到：回放结束前没有同时满足 checkpoint>=1 且 ODS>=30000"
    return last


def service_back(kind: str) -> None:
    if kind == "clickhouse":
        bench.wait_until(lambda: bench.ch("SELECT 1 FORMAT TabSeparated").strip() == "1", 180, what="ch-back")
    elif kind == "kafka":
        bench.wait_until(
            lambda: bench.docker(
                "exec",
                "pulseboard-kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--list",
                check=False,
            ).returncode
            == 0,
            180,
            what="kafka-back",
        )
    elif kind == "tm":
        bench.wait_until(
            lambda: int(bench.http_json(bench.FLINK + "/overview").get("taskmanagers") or 0) >= 1,
            180,
            what="tm-back",
        )


def run_case(
    doc: dict,
    run_id: str,
    speedup: float,
    mode: str,
    batch_size: int,
    flush_ms: int,
    py_rows: dict,
    file_dup_pv: int,
    chaos: str | None = None,
    limit: int = 0,
    latency: bool = True,
) -> dict:
    item: dict = {
        "watermark_mode": mode,
        "batch_size": batch_size,
        "flush_ms": flush_ms,
        "speedup": speedup,
    }
    doc["runs"][run_id] = item
    try:
        bench.cancel_running()
        # 同一个 topic 再跑一遍时，earliest 会把上一轮的消息再读一次。
        # 第二遍的自然键都已经见过，于是整批被标成重复和迟到。每次先删掉 topic。
        recreate_topic(f"events-{run_id}")
        jid = submit(run_id, mode, batch_size, flush_ms)
        item["job_id"] = jid
        started = time.perf_counter()
        proc, lines = start_replay(run_id, speedup, limit)
        sampler = StatSampler()
        sampler.__enter__()
        backpressure = None
        try:
            if chaos:
                item["chaos_kind"] = chaos
                ready = wait_checkpoint(jid, run_id, proc)
                item["checkpoint_before"] = ready
                if ready.get("note"):
                    item["chaos"] = {"skipped": ready["note"]}
                else:
                    since = time.time()
                    states = []
                    if chaos == "tm":
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
                    elif chaos == "clickhouse":
                        bench.docker("restart", "pulseboard-clickhouse", check=False, timeout=180)
                    elif chaos == "kafka":
                        bench.docker("restart", "pulseboard-kafka", check=False, timeout=180)
                    service_back(chaos)
                    deadline = time.time() + 180
                    while time.time() < deadline:
                        state = bench.job_state(jid)
                        states.append({"after_s": round(time.time() - since, 3), "state": state})
                        overview = bench.http_json(bench.FLINK + "/overview")
                        if state == "RUNNING" and int(overview.get("taskmanagers") or 0) >= 1:
                            break
                        time.sleep(2)
                    time.sleep(8)
                    logs = jm_since(since)
                    item["chaos"] = {
                        "poll_states": states[-30:],
                        "log": interpret_recovery(logs, jid),
                    }
                    item["chaos"]["poll_note"] = (
                        "poll_states 只说明 REST 上看到的状态，恢复秒数以 log 里的时间戳为准。"
                    )
            else:
                # 回放到大约四成时抓一次背压。加速回放的墙钟大约是 span/speedup。
                span_s = (doc["dataset"]["span_ms"] / speedup / 1000.0) if limit == 0 else 8
                time.sleep(min(40, max(5, span_s * 0.45)))
                if proc.poll() is None:
                    backpressure = sample_backpressure(jid)
        finally:
            sampler.__exit__(None, None, None)
        item["replay"] = finish_replay(proc, lines, timeout=900 if chaos else 600)
        produced = int((item.get("replay") or {}).get("produced") or 0)
        item["settle"] = bench.settle(run_id, produced, timeout=420)
        item["e2e_s"] = round(time.perf_counter() - started, 3)
        if produced and item["e2e_s"] > 0:
            item["e2e_events_per_s"] = round(produced / item["e2e_s"], 2)
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
        rt_full, summary = slim_rt(rt_full, extra)
        item["rt_summary"] = summary
        item["quality"] = quality_counts(run_id)
        if limit == 0:
            item["accuracy"] = accuracy(rt_full, py_rows, file_dup_pv)
        else:
            item["accuracy"] = {"note": "冒烟只回放文件前缀，不和全量离线结果比"}
        if latency and not chaos:
            item["latency"] = quantile_exact_latency(run_id)
        item["docker_stats"] = summarize_stats(sampler.samples)
        if backpressure is not None:
            item["backpressure"] = backpressure
        exc = bench.job_exceptions(jid)
        if exc:
            item["exceptions"] = exc[:1500]
    except Exception as exc:  # noqa: BLE001
        item["error"] = str(exc)
        bench.log(f"{run_id} failed: {exc}")
    save(doc)
    if limit == 0:
        try:
            forget_run(run_id)
        except Exception as exc:  # noqa: BLE001
            doc.setdefault("unfinished", []).append(f"删除 {run_id} 的行失败：{exc}")
            save(doc)
    return item


def summarize_stats(samples: list[dict]) -> dict:
    peak = {}
    for sample in samples:
        for row in sample.get("rows") or []:
            name = row.get("Name") or row.get("Container")
            if not name:
                continue
            cpu = str(row.get("CPUPerc") or "0").replace("%", "")
            mem = row.get("MemUsage")
            try:
                cpu_f = float(cpu)
            except ValueError:
                continue
            slot = peak.setdefault(name, {"max_cpu_perc": cpu_f, "mem_at_max_cpu": mem, "samples": 0})
            slot["samples"] += 1
            if cpu_f >= slot["max_cpu_perc"]:
                slot["max_cpu_perc"] = cpu_f
                slot["mem_at_max_cpu"] = mem
    return {"peaks": peak, "sample_count": len(samples)}


def rate_of(item: dict | None) -> float | None:
    if not item or item.get("error"):
        return None
    value = item.get("e2e_events_per_s")
    return None if value is None else float(value)


def render_limits(limits: dict) -> str:
    lines = ["| 容器 | compose 请求 CPU | inspect NanoCpus | 折合 CPU | Memory 字节 | cgroup cpu.max | 配额是否写上 |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for name, row in limits.items():
        lines.append(
            f"| {name} | {row['requested_cpus']} | {row['nano_cpus']} | {row['enforced_cpus']} | {row['memory_bytes']} | `{row['cgroup_cpu_max']}` | {row['cpu_quota_enforced']} |"
        )
    return "\n".join(lines)


def render_run(run_id: str, item: dict) -> str:
    if item.get("error"):
        return f"### {run_id}\n\n未测完：{item['error']}\n"
    replay = item.get("replay") or {}
    if replay.get("error"):
        head = f"回放失败：{replay['error']}\n"
    else:
        inj = (replay.get("injection") or {}).get("counts") or {}
        head = (
            f"- 水位 `{item.get('watermark_mode')}`，sink batch {item.get('batch_size')} / flush {item.get('flush_ms')} ms，speedup {item.get('speedup')}\n"
            f"- 作业 `{item.get('job_id')}`\n"
            f"- 生产耗时 {replay.get('elapsed_s')} s，生产速率 {replay.get('produce_rate')} 条/秒，最大落后 {replay.get('max_behind_s')} s\n"
            f"- 端到端 {item.get('e2e_s')} s，端到端速率 {item.get('e2e_events_per_s')} 条/秒（含等窗口落稳）\n"
            f"- 注入计数 {json.dumps(inj, ensure_ascii=False)}，畸形分类 {json.dumps((replay.get('injection') or {}).get('malformed_kinds'), ensure_ascii=False)}\n"
        )
    ident = ((item.get("accuracy") or {}).get("pv_identity")) or {}
    quality = item.get("quality") or {}
    lat = item.get("latency") or {}
    parts = [f"### {run_id}\n", head]
    if ident:
        parts.append(
            "- PV 恒等式：ads_pv + late_pv - duplicate_pv = "
            f"{ident.get('ads_pv')} + {ident.get('late_pv')} - {ident.get('duplicate_pv')} = {ident.get('first_seen_streaming')}；"
            f"离线第一次出现的 PV = {ident.get('first_seen_batch')}；差额 {ident.get('gap')}；exact={ident.get('exact')}\n"
        )
    if quality.get("invalid_by_reason") is not None:
        parts.append(f"- 非法原因计数 {json.dumps(quality.get('invalid_by_reason'), ensure_ascii=False)}\n")
    if item.get("rt_summary"):
        parts.append(f"- 表行数摘要 {json.dumps(item['rt_summary'], ensure_ascii=False)}\n")
    vs = (item.get("accuracy") or {}).get("vs_python") or {}
    if vs:
        parts.append(f"- 相对离线的绝对差 {json.dumps(vs, ensure_ascii=False)}\n")
    if lat:
        parts.append(f"- 延迟（quantileExact）{json.dumps(lat, ensure_ascii=False)}\n")
    if item.get("docker_stats"):
        parts.append(f"- docker stats 峰值 {json.dumps(item['docker_stats'], ensure_ascii=False)}\n")
    if item.get("backpressure"):
        parts.append(f"- 背压采样 {json.dumps(item['backpressure'], ensure_ascii=False)}\n")
    if item.get("chaos"):
        parts.append(f"- 混沌 {json.dumps(item['chaos'], ensure_ascii=False)}\n")
    if item.get("checkpoint_before"):
        parts.append(f"- 动手前 checkpoint {json.dumps(item['checkpoint_before'], ensure_ascii=False)}\n")
    if item.get("checkpoint_after"):
        parts.append(f"- 结束后 checkpoint {json.dumps(item['checkpoint_after'], ensure_ascii=False)}\n")
    if item.get("exceptions"):
        parts.append(f"- 异常摘录：{item['exceptions'][:500]}\n")
    return "".join(parts) + "\n"


def render(doc: dict) -> str:
    ds = doc.get("dataset") or {}
    lines = [
        "# 接近生产的基准",
        "",
        "这一页只记录 `scripts/benchmark_realistic.py` 跑出来的数。没有填上的项写成「未测到」，不是估计。",
        "",
        "## 机器与配额",
        "",
        "```text",
        json.dumps(doc.get("machine") or {}, ensure_ascii=False, indent=2),
        "```",
        "",
        "compose 里写的 CPU 和内存上限，以及运行时 `docker inspect` / cgroup 读到的值：",
        "",
        render_limits(doc.get("limits") or {}),
        "",
        "## 数据",
        "",
        f"- 文件 `{ds.get('events_csv')}`，行数 {ds.get('rows')}，sha256 `{ds.get('sha256_events')}`",
        f"- 事件时间 {ds.get('min_event_time_ms')} 到 {ds.get('max_event_time_ms')}，跨度 {ds.get('span_ms')} ms",
        f"- 行为计数 {json.dumps(ds.get('behaviors'), ensure_ascii=False)}",
        f"- 文件里原有的自然键重复 {ds.get('file_duplicate_events')} 条，其中 PV {ds.get('file_duplicate_pv')}",
        f"- 主回放 speedup {ds.get('speedup')}（把这段事件时间压进约 {ds.get('wall_s')} 秒墙钟）",
        f"- 更快的对照 speedup {ds.get('fast_speedup')}（约 {ds.get('fast_wall_s')} 秒）",
        "",
        "按 UTC 小时的真实条数（不是合成的昼夜曲线）：",
        "",
        "| UTC 小时 | 事件数 |",
        "| --- | --- |",
    ]
    for row in ds.get("hourly") or []:
        lines.append(f"| {row['hour_utc']} | {row['events']} |")
    lines.extend(
        [
            "",
            "小时计数来自这份 CSV 的 event_time，不是事后拟合的昼夜曲线。文件在 70 万行处截断，事件时间停在 2019-11-01 中午附近，没有傍晚和夜间。同一事件秒里的记录 release 时间相同，加速后会连在一起发送。",
            "",
            f"离线 Python 用时 {((doc.get('python_batch') or {}).get('elapsed_s'))} 秒，窗口数 {(doc.get('python_batch') or {}).get('windows')}，文件重复 {(doc.get('python_batch') or {}).get('duplicate_events')}。",
            "",
            "## 各次运行",
            "",
        ]
    )
    for run_id, item in (doc.get("runs") or {}).items():
        lines.append(render_run(run_id, item))
    opt = doc.get("optimization")
    lines.extend(["## 优化前后", "", "```json", json.dumps(opt, ensure_ascii=False, indent=2), "```", ""])
    lines.extend(["## 瓶颈", "", "```json", json.dumps(doc.get("bottleneck"), ensure_ascii=False, indent=2), "```", ""])
    if doc.get("unfinished"):
        lines.extend(["## 测量范围", ""])
        for note in doc["unfinished"]:
            lines.append(f"- {note}")
        lines.append("")
    lines.append(f"开始 {doc.get('started_at')}，结束 {doc.get('finished_at')}。")
    return "\n".join(lines) + "\n"


def choose_optimization(doc: dict) -> None:
    before = rate_of(doc["runs"].get("pace-before"))
    after = rate_of(doc["runs"].get("pace-after"))
    doc["optimization"] = {
        "pace_before_e2e": before,
        "pace_after_e2e": after,
        "pace_ratio": None if not before or not after else round(after / before, 4),
    }
    if before and after and after >= before * 1.05:
        doc["optimization"]["chosen"] = "pace-before vs pace-after（periodic 相对 punctuated，端到端速率提高至少 5%）"
        return
    doc["optimization"]["pace_note"] = "150 秒昼夜回放上，periodic 没有把端到端速率提高 5%。下面改用更短的墙钟，让管道离开回放睡眠的上限。"


def bottleneck_from(doc: dict) -> dict:
    """用 pace-before 的 stats 和背压做判断。原始采样留在 runs 里。"""
    item = doc["runs"].get("pace-before") or {}
    peaks = ((item.get("docker_stats") or {}).get("peaks")) or {}
    limits = doc.get("limits") or {}
    usage = []
    for name, peak in peaks.items():
        # docker stats 的 CPUPerc：1 个核打满大约是 100。
        cores = float(peak["max_cpu_perc"]) / 100.0
        quota = None
        for container, row in limits.items():
            if container == name or name.endswith(container):
                quota = row.get("enforced_cpus") or row.get("requested_cpus")
        ratio = None if not quota else round(cores / quota, 3)
        usage.append({"container": name, "max_cpu_cores": round(cores, 3), "quota": quota, "cores_over_quota": ratio, "mem": peak.get("mem_at_max_cpu")})
    usage.sort(key=lambda row: row["cores_over_quota"] or 0, reverse=True)
    bp_hot = []
    for vertex in item.get("backpressure") or []:
        level = vertex.get("backpressure-level")
        ratios = [sub.get("ratio") for sub in vertex.get("subtasks") or [] if sub.get("ratio") is not None]
        if level not in (None, "ok", "low") or any((r or 0) >= 0.5 for r in ratios):
            bp_hot.append({"name": vertex.get("name"), "level": level, "ratios": ratios})
    return {"cpu_vs_quota": usage, "backpressure_not_low": bp_hot}


def contaminated(item: dict | None) -> bool:
    """topic 里留着上一轮消息时，重复侧输出会接近全量，而不是注入的那一小部分。"""
    if not item or item.get("error"):
        return True
    produced = int((item.get("replay") or {}).get("produced") or 0)
    dups = int(((item.get("rt_summary") or {}).get("duplicates") or {}).get("duplicate_events") or 0)
    return bool(produced) and dups > produced * 0.05


def complete_enough(item: dict | None) -> bool:
    if not item or item.get("error"):
        return False
    ident = ((item.get("accuracy") or {}).get("pv_identity")) or {}
    return ident.get("gap") is not None and item.get("latency")


def main() -> int:
    resume = OUT_JSON.exists() and __import__("os").environ.get("RESUME") == "1"
    if resume:
        doc = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        doc.setdefault("unfinished", [])
        doc.setdefault("runs", {})
    else:
        doc = {
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "machine": bench.machine_info(),
            "runs": {},
            "unfinished": [],
        }
    try:
        bench.compose_up()
        doc["services"] = bench.wait_healthy()
        doc["limits"] = inspect_limits()
    except Exception as exc:  # noqa: BLE001
        doc["unfinished"].append(f"集群没有就绪：{exc}")
        doc["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save(doc)
        return 1
    if not all(row.get("cpu_quota_enforced") for row in doc["limits"].values()):
        doc["unfinished"].append("至少有一个容器的 NanoCpus 为 0，CPU 配额没有真正写上。表里按 inspect 结果为准。")

    dataset, py_rows = load_dataset()
    doc["dataset"] = dataset
    doc["python_batch"] = {
        "elapsed_s": py_rows["elapsed_s"],
        "duplicate_events": py_rows["duplicate_events"],
        "windows": py_rows.get("windows") or len(py_rows["pv_uv"]),
        "pv_sum": sum(bench.num(row["pv"]) for row in py_rows["pv_uv"]),
    }
    file_dup_pv = dataset["file_duplicate_pv"]
    speedup = dataset["speedup"]
    fast = dataset["fast_speedup"]
    save(doc)
    if resume:
        note = (
            "两轮 70 万行同时留在 ClickHouse 时，查询返回 Code 241："
            "would use 790.68 MiB，maximum 762.94 MiB。"
            "之后每轮把指标写入 JSON 就删除该 run_id 的行，下面各次运行是在这个删除策略下测的。"
        )
        if note not in doc["unfinished"]:
            doc["unfinished"].append(note)
        for run_id in (
            "smoke",
            "pace-before",
            "pace-after",
            "fast-before",
            "fast-after",
            "fast-sink",
            "chaos-tm",
            "chaos-clickhouse",
            "chaos-kafka",
        ):
            try:
                forget_run(run_id)
            except Exception as exc:  # noqa: BLE001
                bench.log(f"purge {run_id} failed: {exc}")
                doc["unfinished"].append(f"清理 {run_id} 失败：{exc}")
                save(doc)
                return 1

    if not complete_enough(doc["runs"].get("smoke")) and not (
        resume and (doc["runs"].get("smoke") or {}).get("rt_summary") and not (doc["runs"].get("smoke") or {}).get("error")
    ):
        run_case(doc, "smoke", speedup=8000, mode="punctuated", batch_size=1000, flush_ms=200, py_rows=py_rows, file_dup_pv=file_dup_pv, limit=3000, latency=False)
        smoke = doc["runs"]["smoke"]
        invalid_n = sum(int(row["c"]) for row in (smoke.get("quality") or {}).get("invalid_by_reason") or [])
        ods = int(((smoke.get("rt_summary") or {}).get("ods_final") or {}).get("final_rows") or 0)
        if smoke.get("error") or invalid_n < 1 or ods < 1000:
            doc["unfinished"].append(f"冒烟没有通过，停止后续测量。invalid={invalid_n} ods={ods} error={smoke.get('error')}")
            doc["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            save(doc)
            return 1
    else:
        bench.log("skip smoke")

    if not (resume and (doc["runs"].get("pace-before") or {}).get("latency") and not (doc["runs"].get("pace-before") or {}).get("error")):
        run_case(doc, "pace-before", speedup, "punctuated", 1000, 200, py_rows, file_dup_pv)
    else:
        bench.log("skip pace-before")
    if contaminated(doc["runs"].get("pace-after")) or not (doc["runs"].get("pace-after") or {}).get("latency"):
        run_case(doc, "pace-after", speedup, "periodic", 1000, 200, py_rows, file_dup_pv)
    else:
        bench.log("skip pace-after")
    choose_optimization(doc)
    if "chosen" not in doc["optimization"]:
        if contaminated(doc["runs"].get("fast-before")):
            run_case(doc, "fast-before", fast, "punctuated", 1000, 200, py_rows, file_dup_pv, latency=True)
        else:
            bench.log("skip fast-before")
        if contaminated(doc["runs"].get("fast-after")):
            run_case(doc, "fast-after", fast, "periodic", 1000, 200, py_rows, file_dup_pv, latency=True)
        else:
            bench.log("skip fast-after")
        fb = rate_of(doc["runs"].get("fast-before"))
        fa = rate_of(doc["runs"].get("fast-after"))
        doc["optimization"]["fast_before_e2e"] = fb
        doc["optimization"]["fast_after_e2e"] = fa
        doc["optimization"]["fast_ratio"] = None if not fb or not fa else round(fa / fb, 4)
        if fb and fa and fa >= fb * 1.05:
            doc["optimization"]["chosen"] = "fast-before vs fast-after（只改 periodic / punctuated）"
        else:
            doc["optimization"]["fast_note"] = "更快的 speedup 上 periodic 仍没有提高 5%。再只改 sink 批量。"
            if contaminated(doc["runs"].get("fast-sink")):
                run_case(doc, "fast-sink", fast, "punctuated", 4000, 500, py_rows, file_dup_pv, latency=True)
            else:
                bench.log("skip fast-sink")
            fs = rate_of(doc["runs"].get("fast-sink"))
            doc["optimization"]["fast_sink_e2e"] = fs
            doc["optimization"]["sink_ratio"] = None if not fb or not fs else round(fs / fb, 4)
            doc["optimization"]["chosen"] = "fast-before vs fast-sink（只改 batch 4000 / flush 500，水位仍是 punctuated）"

    for kind in ("tm", "clickhouse", "kafka"):
        run_id = f"chaos-{kind}"
        item = doc["runs"].get(run_id) or {}
        has_log = bool(((item.get("chaos") or {}).get("log")) or item.get("chaos", {}).get("skipped"))
        if item and not contaminated(item) and not item.get("error") and (has_log or item.get("accuracy")):
            bench.log(f"skip {run_id}")
            continue
        run_case(
            doc,
            run_id,
            speedup,
            "punctuated",
            1000,
            200,
            py_rows,
            file_dup_pv,
            chaos=kind,
            latency=False,
        )
    doc["bottleneck"] = bottleneck_from(doc)
    try:
        bench.cancel_running()
    except Exception as exc:  # noqa: BLE001
        doc["unfinished"].append(f"结束时取消作业失败：{exc}")
    doc["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save(doc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
