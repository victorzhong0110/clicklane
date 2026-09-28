"""把 PulseBoard 跑起来，并把测到的数字写进 docs/benchmark.md。

缺的指标写成「未测到」和原因，不补猜测值。
延迟只把 baseline（不乱序、不限速）当作标题数字；recovery 单独记。
"""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable
CH = "http://127.0.0.1:18123/?database=pulseboard"
FLINK = "http://127.0.0.1:18081"
WINDOW_MS = 600_000
WATERMARK_MS = 120_000
TOP_N = 10
OUT_JSON = ROOT / "docs" / "measurements.json"
OUT_MD = ROOT / "docs" / "benchmark.md"

EXPERIMENTS = [
    {"run_id": "baseline", "rate": 0, "ratio": 0.0, "span_ms": 0, "latency": True},
    {"run_id": "disorder-3m", "rate": 0, "ratio": 0.25, "span_ms": 180_000, "latency": False},
    {"run_id": "disorder-15m", "rate": 0, "ratio": 0.25, "span_ms": 900_000, "latency": False},
    {"run_id": "recovery", "rate": 4000, "ratio": 0.0, "span_ms": 0, "latency": False, "recovery": True},
]


def log(message: str) -> None:
    print(message, flush=True)


def run(cmd: list[str], timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    log("+ " + " ".join(cmd))
    return subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=check,
    )


def capture(cmd: list[str], timeout: int = 30) -> str:
    try:
        completed = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=False)
    except Exception as exc:  # noqa: BLE001
        return f"未测到: {exc}"
    text = (completed.stdout or "") + (completed.stderr or "")
    return text.strip()


def docker_base() -> list[str]:
    probe = subprocess.run(["docker", "info"], capture_output=True, text=True, check=False)
    if probe.returncode == 0:
        return ["docker"]
    return ["sudo", "docker"]


DOCKER = docker_base()


def docker(*args: str, timeout: int = 180, check: bool = True) -> subprocess.CompletedProcess:
    return run([*DOCKER, *args], timeout=timeout, check=check)


def http_json(url: str, data: bytes | None = None, timeout: int = 30) -> dict:
    request = urllib.request.Request(url, data=data, method="POST" if data is not None else "GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode()
    if not body.strip():
        return {}
    return json.loads(body)


def ch(sql: str, timeout: int = 120) -> str:
    request = urllib.request.Request(CH, data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:800]
        raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc


def ch_rows(sql: str) -> list[dict]:
    text = sql.strip()
    if "FORMAT " not in text.upper():
        text += "\nFORMAT JSONEachRow"
    body = ch(text).strip()
    if not body:
        return []
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def wait_until(predicate, timeout: int, interval: float = 2.0, what: str = "") -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception as exc:  # noqa: BLE001
            log(f"wait {what}: {exc}")
        time.sleep(interval)
    return False


def compose_up() -> None:
    # 有的环境里 iptables-legacy 的 FORWARD 默认是 DROP，而且只放行 docker0。
    # Compose 用的是另一块网桥，容器之间会全部超时。nft 已经 ACCEPT 时，
    # 把 legacy 的默认策略改成 ACCEPT 才和它一致。改不动就继续，后面的健康检查会失败。
    subprocess.run(["sudo", "iptables-legacy", "-P", "FORWARD", "ACCEPT"], check=False)
    subprocess.run(["sh", str(ROOT / "scripts" / "fetch_grafana_plugin.sh")], cwd=ROOT, check=False)
    docker("compose", "-f", str(ROOT / "docker-compose.yml"), "up", "-d", timeout=600)


def wait_healthy() -> dict:
    status = {}

    def ch_ok() -> bool:
        body = ch("SELECT 1 FORMAT TabSeparated")
        return body.strip() == "1"

    def flink_ok() -> bool:
        overview = http_json(FLINK + "/overview")
        status["flink_version"] = overview.get("flink-version")
        slots = int(overview.get("slots-total") or 0)
        return slots >= 1 and overview.get("taskmanagers", 0) >= 1

    def kafka_ok() -> bool:
        completed = docker(
            "exec",
            "pulseboard-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--list",
            check=False,
        )
        status["kafka_list"] = (completed.stdout or "") + (completed.stderr or "")
        return completed.returncode == 0

    def grafana_ok() -> bool:
        with urllib.request.urlopen("http://127.0.0.1:43123/api/health", timeout=10) as response:
            payload = json.loads(response.read().decode())
        status["grafana"] = payload
        return payload.get("database") == "ok"

    status["clickhouse"] = wait_until(ch_ok, 180, what="clickhouse")
    status["flink"] = wait_until(flink_ok, 180, what="flink")
    status["kafka"] = wait_until(kafka_ok, 180, what="kafka")
    status["grafana"] = wait_until(grafana_ok, 180, what="grafana")
    if not all(status[name] for name in ("clickhouse", "flink", "kafka", "grafana")):
        raise RuntimeError(f"services not healthy: {status}")
    status["clickhouse_version"] = ch("SELECT version() FORMAT TabSeparated").strip()
    return status


def create_topic(topic: str) -> None:
    docker(
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
        "3",
        "--replication-factor",
        "1",
        check=False,
    )


def jobs() -> list[dict]:
    payload = http_json(FLINK + "/jobs/overview")
    return payload.get("jobs") or []


def cancel_running() -> None:
    for job in jobs():
        if job.get("state") in {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING", "RECONCILING", "FAILING"}:
            docker("exec", "pulseboard-jobmanager", "/opt/flink/bin/flink", "cancel", job["jid"], check=False, timeout=60)
    wait_until(
        lambda: all(job.get("state") not in {"RUNNING", "RESTARTING", "CREATED"} for job in jobs()),
        90,
        what="cancel",
    )


def submit_job(run_id: str) -> str:
    topic = f"events-{run_id}"
    completed = docker(
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
        "/opt/data/dim_item.csv",
        "--window.ms",
        str(WINDOW_MS),
        "--watermark.ms",
        str(WATERMARK_MS),
        "--idle.sec",
        "20",
        "--topn",
        str(TOP_N),
        "--parallelism",
        "2",
        "--batch.size",
        "1000",
        "--flush.ms",
        "200",
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
    ok = wait_until(lambda: any(job.get("jid") == jid and job.get("state") == "RUNNING" for job in jobs()), 120, what="running")
    if not ok:
        raise RuntimeError(f"job {jid} did not reach RUNNING: {jobs()} exceptions={job_exceptions(jid)}")
    return jid


def job_exceptions(jid: str) -> str:
    try:
        payload = http_json(f"{FLINK}/jobs/{jid}/exceptions")
    except Exception as exc:  # noqa: BLE001
        return str(exc)
    root = payload.get("root-exception") or ""
    return root[:1500]


def job_state(jid: str) -> str:
    for job in jobs():
        if job.get("jid") == jid:
            return job.get("state") or ""
    return ""


def checkpoints(jid: str) -> dict:
    try:
        return http_json(f"{FLINK}/jobs/{jid}/checkpoints")
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def checkpoint_brief(payload: dict) -> dict:
    latest = (payload.get("latest") or {}).get("completed") or {}
    restored = (payload.get("latest") or {}).get("restored") or {}
    brief = {"counts": payload.get("counts")}
    if latest:
        brief["completed"] = {
            "id": latest.get("id"),
            "status": latest.get("status"),
            "end_to_end_duration_ms": latest.get("end_to_end_duration"),
            "state_size_bytes": latest.get("state_size"),
            "checkpointed_size_bytes": latest.get("checkpointed_size")
            or latest.get("persisted_data")
            or latest.get("processed_data"),
        }
        brief["completed_raw"] = latest
    if restored:
        brief["restored"] = {
            "id": restored.get("id"),
            "restore_timestamp": restored.get("restore_timestamp"),
            "external_path": restored.get("external_path"),
            "raw": restored,
        }
    if payload.get("error"):
        brief["error"] = payload["error"]
    return brief


def parse_replay(text: str) -> dict:
    for line in text.splitlines():
        if line.startswith("PULSEBOARD_REPLAY "):
            return json.loads(line[len("PULSEBOARD_REPLAY ") :])
    raise RuntimeError(f"replay report missing: {text[-1500:]}")


def replay_command(run_id: str, rate: float, ratio: float, span_ms: int) -> list[str]:
    return [
        PY,
        "-m",
        "replayer.replay",
        "--bootstrap",
        "127.0.0.1:19092",
        "--topic",
        f"events-{run_id}",
        "--run-id",
        run_id,
        "--rate",
        str(rate),
        "--disorder-ratio",
        str(ratio),
        "--disorder-span-ms",
        str(span_ms),
        "--seed",
        "7",
        "--partitions",
        "3",
        "--window-ms",
        str(WINDOW_MS),
        "--watermark-ms",
        str(WATERMARK_MS),
        "--kick",
    ]


def run_replay(run_id: str, rate: float, ratio: float, span_ms: int) -> dict:
    completed = run(replay_command(run_id, rate, ratio, span_ms), timeout=900)
    return parse_replay((completed.stdout or "") + (completed.stderr or ""))


def start_replay(run_id: str, rate: float, ratio: float, span_ms: int) -> tuple[subprocess.Popen, list[str]]:
    lines: list[str] = []
    proc = subprocess.Popen(
        replay_command(run_id, rate, ratio, span_ms),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    def _read() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)
            log(line.rstrip())

    threading.Thread(target=_read, daemon=True).start()
    return proc, lines


def settle(run_id: str, produced: int, timeout: int = 300) -> dict:
    history = []
    stable = 0
    last = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = ch_rows(
            f"""
            SELECT
              (SELECT count() FROM pulseboard.ods_events FINAL WHERE run_id = '{run_id}') AS ods,
              (SELECT count() FROM pulseboard.ads_pv_uv FINAL WHERE run_id = '{run_id}') AS windows,
              (SELECT count() FROM pulseboard.dwd_late_events FINAL WHERE run_id = '{run_id}') AS late
            """
        )
        current = rows[0] if rows else {"ods": 0, "windows": 0, "late": 0}
        history.append({"t": time.time(), **current})
        log(f"settle {run_id} {current}")
        signature = (current.get("ods"), current.get("windows"), current.get("late"))
        if signature == last and int(current.get("ods") or 0) >= produced and int(current.get("windows") or 0) > 0:
            stable += 1
            if stable >= 3:
                return {"stable": True, "final": current, "polls": len(history)}
        else:
            stable = 0
        last = signature
        time.sleep(5)
    return {"stable": False, "final": last and {"ods": last[0], "windows": last[1], "late": last[2]}, "polls": len(history), "reason": "timeout"}


def latency(run_id: str) -> dict:
    ods = ch_rows(
        f"""
        SELECT
          count() AS n,
          min(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS min_ms,
          quantile(0.5)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS p50_ms,
          quantile(0.99)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS p99_ms,
          max(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(produce_time)) AS max_ms
        FROM pulseboard.ods_events FINAL
        WHERE run_id = '{run_id}'
        """
    )
    ads = ch_rows(
        f"""
        SELECT
          count() AS windows,
          min(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS min_ms,
          quantile(0.5)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS p50_ms,
          quantile(0.99)(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS p99_ms,
          max(toUnixTimestamp64Milli(inserted_at) - toUnixTimestamp64Milli(max_produce_time)) AS max_ms
        FROM pulseboard.ads_pv_uv FINAL
        WHERE run_id = '{run_id}'
        """
    )
    return {"ods_insert_minus_produce": ods[0] if ods else None, "ads_insert_minus_max_produce": ads[0] if ads else None}


def rt_tables(run_id: str) -> dict:
    pv = ch_rows(
        f"""
        SELECT toUnixTimestamp(window_start) * 1000 AS window_start_ms,
               pv, uv, cart_cnt, buy_cnt, events, dim_miss
        FROM pulseboard.ads_pv_uv FINAL
        WHERE run_id = '{run_id}'
        ORDER BY window_start
        """
    )
    funnel = ch_rows(
        f"""
        SELECT toUnixTimestamp(window_start) * 1000 AS window_start_ms,
               pv_users, cart_users, buy_users, pv_to_cart_users, cart_to_buy_users, pv_to_buy_users
        FROM pulseboard.ads_funnel FINAL
        WHERE run_id = '{run_id}'
        ORDER BY window_start
        """
    )
    top = ch_rows(
        f"""
        SELECT toUnixTimestamp(window_start) * 1000 AS window_start_ms,
               rank, item_id, category_code, pv
        FROM pulseboard.ads_top_items FINAL
        WHERE run_id = '{run_id}'
        ORDER BY window_start, rank
        """
    )
    late = ch_rows(
        f"""
        SELECT count() AS late_events, countIf(behavior = 'pv') AS late_pv
        FROM pulseboard.dwd_late_events FINAL
        WHERE run_id = '{run_id}'
        """
    )
    dup = ch_rows(
        f"SELECT count() AS duplicate_events FROM pulseboard.dwd_duplicate_events FINAL WHERE run_id = '{run_id}'"
    )
    physical = ch_rows(f"SELECT count() AS physical FROM pulseboard.ods_events WHERE run_id = '{run_id}'")
    final = ch_rows(f"SELECT count() AS final_rows FROM pulseboard.ods_events FINAL WHERE run_id = '{run_id}'")
    return {
        "pv_uv": pv,
        "funnel": funnel,
        "top_items": top,
        "late": late[0] if late else None,
        "duplicates": dup[0] if dup else None,
        "ods_physical": physical[0] if physical else None,
        "ods_final": final[0] if final else None,
    }


def load_sql_file() -> dict[str, str]:
    text = (ROOT / "sql" / "batch_metrics.sql").read_text(encoding="utf-8")
    parts = re.split(r"(?m)^-- name: (\w+)\s*$", text)
    named = {}
    for i in range(1, len(parts), 2):
        named[parts[i]] = parts[i + 1].strip()
    return named


def load_batch_tables() -> None:
    ch("TRUNCATE TABLE pulseboard.batch_events")
    ch("TRUNCATE TABLE pulseboard.dim_item")
    events = (ROOT / "data" / "sample" / "events.csv").read_bytes()
    dim = (ROOT / "data" / "dim" / "dim_item.csv").read_bytes()
    ch("INSERT INTO pulseboard.batch_events FORMAT CSVWithNames\n" + events.decode())
    ch("INSERT INTO pulseboard.dim_item FORMAT CSVWithNames\n" + dim.decode())


def batch_sql() -> dict:
    queries = load_sql_file()
    rendered = {name: sql.format(window_ms=WINDOW_MS, top_n=TOP_N) for name, sql in queries.items()}
    return {name: ch_rows(sql) for name, sql in rendered.items()}


def python_batch() -> dict:
    sys.path.insert(0, str(ROOT))
    from batch.metrics import compute

    events = []
    with (ROOT / "data" / "sample" / "events.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            events.append(
                {
                    "user_id": int(row["user_id"]),
                    "item_id": int(row["item_id"]),
                    "behavior": row["behavior"],
                    "event_time_ms": int(row["event_time_ms"]),
                }
            )
    dim = {}
    with (ROOT / "data" / "dim" / "dim_item.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            dim[int(row["item_id"])] = row["category_code"] or "UNKNOWN"
    started = time.perf_counter()
    result = compute(events, WINDOW_MS, TOP_N, dim)
    result["elapsed_s"] = round(time.perf_counter() - started, 3)
    return result


def num(value) -> float:
    return float(value)


def index_by(rows: list[dict], key: str) -> dict:
    return {int(row[key]) : row for row in rows}


def compare_metric(rt_rows: list[dict], batch_rows: list[dict], fields: list[str]) -> dict:
    rt = index_by(rt_rows, "window_start_ms")
    batch = index_by(batch_rows, "window_start_ms")
    keys = sorted(set(rt) | set(batch))
    per_field = {}
    for field in fields:
        diffs = []
        missing = 0
        for key in keys:
            if key not in rt or key not in batch:
                missing += 1
                continue
            diffs.append(abs(num(rt[key][field]) - num(batch[key][field])))
        per_field[field] = {
            "windows_compared": len(diffs),
            "windows_missing_one_side": missing,
            "windows_with_diff": sum(1 for item in diffs if item != 0),
            "sum_abs_diff": sum(diffs),
            "max_abs_diff": max(diffs) if diffs else None,
            "sum_rt": sum(num(row[field]) for row in rt_rows),
            "sum_batch": sum(num(row[field]) for row in batch_rows),
        }
    return per_field


def compare_top(rt_rows: list[dict], batch_rows: list[dict]) -> dict:
    def key_of(row: dict) -> tuple:
        return (int(row["window_start_ms"]), int(row["rank"]))

    rt = {key_of(row): row for row in rt_rows}
    batch = {key_of(row): row for row in batch_rows}
    keys = sorted(set(rt) | set(batch))
    mismatch = 0
    missing = 0
    for key in keys:
        if key not in rt or key not in batch:
            missing += 1
            continue
        if int(rt[key]["item_id"]) != int(batch[key]["item_id"]) or int(rt[key]["pv"]) != int(batch[key]["pv"]):
            mismatch += 1
    return {
        "pairs_rt": len(rt),
        "pairs_batch": len(batch),
        "pairs_missing_one_side": missing,
        "pairs_item_or_pv_mismatch": mismatch,
    }


def write_reconcile(run_id: str, rt: dict, batch_rows: dict) -> None:
    version = int(time.time() * 1000)
    pv = compare_rows_for_insert(rt["pv_uv"], batch_rows["pv_uv"], ["pv", "uv", "cart_cnt", "buy_cnt"])
    funnel = compare_rows_for_insert(
        rt["funnel"],
        batch_rows["funnel"],
        ["pv_users", "pv_to_cart_users", "cart_to_buy_users", "pv_to_buy_users"],
    )
    values = []
    for metric, pairs in {**pv, **funnel}.items():
        for window_ms, rt_value, batch_value in pairs:
            stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(window_ms / 1000))
            values.append(
                f"('{run_id}','{metric}','{stamp}',{rt_value},{batch_value},{abs(rt_value - batch_value)},{version})"
            )
    if not values:
        return
    ch("INSERT INTO pulseboard.ads_reconcile (run_id, metric, window_start, rt_value, batch_value, abs_diff, version) VALUES "
       + ",".join(values))


def compare_rows_for_insert(rt_rows, batch_rows, fields):
    rt = index_by(rt_rows, "window_start_ms")
    batch = index_by(batch_rows, "window_start_ms")
    out = {field: [] for field in fields}
    for key in sorted(set(rt) & set(batch)):
        for field in fields:
            out[field].append((key, num(rt[key][field]), num(batch[key][field])))
    return out


def normalize_python(result: dict) -> dict:
    return {
        "pv_uv": result["pv_uv"],
        "funnel": result["funnel"],
        "top_items": result["top_items"],
        "duplicate_events": result["duplicate_events"],
        "elapsed_s": result["elapsed_s"],
    }


def try_spark() -> dict:
    report: dict = {}
    free = capture(["bash", "-lc", "awk '/MemAvailable/ {print $2}' /proc/meminfo"])
    report["mem_available_kb_before"] = free
    stopped = False
    try:
        available_kb = int(free)
    except ValueError:
        available_kb = 0
    if available_kb and available_kb < 2_500_000:
        docker("stop", "pulseboard-taskmanager", "pulseboard-jobmanager", check=False, timeout=60)
        stopped = True
        report["flink_stopped_for_spark"] = True
    try:
        import pyspark  # noqa: F401
        report["pyspark_import"] = "already-installed"
    except Exception:
        completed = subprocess.run(
            [PY, "-m", "pip", "install", "pyspark==3.5.3"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=600,
            check=False,
        )
        report["pip_returncode"] = completed.returncode
        report["pip_tail"] = ((completed.stdout or "") + (completed.stderr or ""))[-500:]
        if completed.returncode != 0:
            report["status"] = "未测到"
            report["reason"] = "pyspark 安装失败"
            return report
    completed = subprocess.run(
        [PY, str(ROOT / "batch" / "spark_offline.py")],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=600,
        check=False,
    )
    report["returncode"] = completed.returncode
    report["output_tail"] = ((completed.stdout or "") + (completed.stderr or ""))[-1500:]
    if completed.returncode != 0:
        report["status"] = "未测到"
        report["reason"] = "spark_offline.py 退出码非 0"
    else:
        report["status"] = "ok"
        report["pv_uv"] = read_spark_csv(ROOT / "results" / "spark" / "pv_uv")
    if stopped:
        docker("start", "pulseboard-jobmanager", check=False, timeout=60)
        docker("start", "pulseboard-taskmanager", check=False, timeout=60)
        report["flink_restarted"] = wait_until(
            lambda: int(http_json(FLINK + "/overview").get("taskmanagers") or 0) >= 1,
            120,
            what="flink-after-spark",
        )
    return report


def read_spark_csv(directory: Path) -> list[dict]:
    rows = []
    if not directory.exists():
        return rows
    for path in directory.glob("*.csv"):
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append({key: (int(float(value)) if key != "category_code" else value) for key, value in row.items() if value is not None})
    return rows


def machine_info() -> dict:
    return {
        "uname": capture(["uname", "-a"]),
        "nproc": capture(["nproc"]),
        "cpu": capture(["bash", "-lc", "awk -F: '/model name/ {print $2; exit}' /proc/cpuinfo"]),
        "meminfo": capture(["bash", "-lc", "grep -E 'MemTotal|MemAvailable|SwapTotal' /proc/meminfo"]),
        "os": capture(["bash", "-lc", ". /etc/os-release; printf '%s %s\\n' \"$NAME\" \"$VERSION\""]),
        "java": capture(["bash", "-lc", "java -version"]),
        "python": capture([PY, "--version"]),
        "docker": capture([*DOCKER, "info", "--format", "Server={{.ServerVersion}} Driver={{.Driver}} NCPU={{.NCPU}} MemTotal={{.MemTotal}}"]),
        "disk": capture(["bash", "-lc", "df -h / | tail -1"]),
    }


def fmt(value) -> str:
    if value is None:
        return "未测到"
    if isinstance(value, float):
        if value.is_integer() and abs(value) < 1e15:
            return str(int(value))
        return f"{value:.4g}"
    return str(value)


def render(doc: dict) -> str:
    machine = doc.get("machine") or {}
    lines = [
        "# PulseBoard 基准测试",
        "",
        "数字全部来自同一次 `scripts/benchmark.py` 运行，原始 JSON 在 `docs/measurements.json`。",
        "脚本没有填上的格子写成「未测到」，并附原因。",
        "",
        "## 机器与条件",
        "",
        "| 项 | 实测 |",
        "| --- | --- |",
        f"| uname | `{machine.get('uname')}` |",
        f"| CPU | {fmt((machine.get('cpu') or '').strip())}，nproc={fmt((machine.get('nproc') or '').strip())} |",
        f"| 内存 | `{machine.get('meminfo')}` |",
        f"| 系统 | {fmt(machine.get('os'))} |",
        f"| Java（宿主机） | `{machine.get('java')}` |",
        f"| Python | {fmt(machine.get('python'))} |",
        f"| Docker | `{machine.get('docker')}` |",
        f"| 根分区 | `{machine.get('disk')}` |",
        f"| ClickHouse | {fmt((doc.get('services') or {}).get('clickhouse_version'))} |",
        f"| Flink | {fmt((doc.get('services') or {}).get('flink_version'))} |",
        "",
        "测量条件（脚本写死的参数，不是事后调整）：",
        "",
        "- 样本 200000 条，窗口 600000 ms，水位延迟 120000 ms，Top-N=10。",
        "- Kafka `acks=all`，生产者幂等，linger 20 ms；3 个分区。",
        "- Flink 并行度 2，checkpoint 间隔 10 s，EXACTLY_ONCE 语义，hashmap 状态，文件 checkpoint。",
        "- 水位是按分区的 punctuated watermark（每条事件都发）。",
        "- ClickHouse Sink 批量 1000 行或 200 ms，checkpoint 时再刷一次。查询用 FINAL。",
        "- 乱序实验 seed=7。recovery 限速 4000 条/秒，用来在杀掉 TaskManager 之前留下 checkpoint。",
        "- Docker 存储驱动见上表。驱动若是 vfs，读写会比 overlay2 慢，吞吐要连同驱动一起看。",
        "",
        "## 各次运行",
        "",
    ]
    for run_id, item in (doc.get("runs") or {}).items():
        lines.append(f"### {run_id}")
        lines.append("")
        if item.get("error") and not item.get("replay") and not item.get("settle"):
            lines.append(f"未测完。原因：`{item['error']}`")
            lines.append("")
            continue
        if item.get("error"):
            lines.append(f"运行中途失败，下面是失败前已经拿到的数。原因：`{item['error']}`")
            lines.append("")
        replay = item.get("replay") or {}
        settle_info = item.get("settle") or {}
        lines.append("| 项 | 值 |")
        lines.append("| --- | --- |")
        lines.append(f"| 生产条数 | {fmt(replay.get('produced'))} |")
        lines.append(f"| 生产耗时（秒） | {fmt(replay.get('elapsed_s'))} |")
        lines.append(f"| 生产速率（条/秒） | {fmt(replay.get('produce_rate'))} |")
        lines.append(f"| 乱序回退条数 | {fmt(replay.get('inversions'))} |")
        lines.append(f"| 生产端最大回退（毫秒） | {fmt(replay.get('max_lateness_ms'))} |")
        lines.append(f"| 端到端耗时（秒，开打到 ODS 稳定） | {fmt(item.get('e2e_s'))} |")
        lines.append(f"| 端到端吞吐（条/秒，生产条数 / 端到端耗时） | {fmt(item.get('e2e_events_per_s'))} |")
        lines.append(f"| ODS 是否稳定 | {fmt(settle_info.get('stable'))} |")
        final = settle_info.get("final") or {}
        lines.append(f"| ODS FINAL 行数 | {fmt(final.get('ods'))} |")
        lines.append(f"| PV/UV 窗口数 | {fmt(final.get('windows'))} |")
        lines.append(f"| 迟到行数（稳定时） | {fmt(final.get('late'))} |")
        late = (item.get("rt") or {}).get("late") or {}
        lines.append(f"| 迟到事件 / 迟到 PV | {fmt(late.get('late_events'))} / {fmt(late.get('late_pv'))} |")
        dup = (item.get("rt") or {}).get("duplicates") or {}
        lines.append(f"| 重复侧输出 | {fmt(dup.get('duplicate_events'))} |")
        physical = (item.get("rt") or {}).get("ods_physical") or {}
        final_rows = (item.get("rt") or {}).get("ods_final") or {}
        lines.append(f"| ODS count() / count() FINAL | {fmt(physical.get('physical'))} / {fmt(final_rows.get('final_rows'))} |")
        ckpt = item.get("checkpoint") or {}
        completed = ckpt.get("completed") or {}
        lines.append(f"| 最近一次 checkpoint 耗时（毫秒） | {fmt(completed.get('end_to_end_duration_ms'))} |")
        lines.append(f"| 最近一次 checkpoint 状态大小（字节） | {fmt(completed.get('state_size_bytes'))} |")
        lines.append("")
        if item.get("latency"):
            lines.append("延迟（仅 baseline 作为标题数字；单位毫秒，inserted_at 减 produce_time，含最多 200 ms 的刷写）：")
            lines.append("")
            lines.append("| 口径 | n | min | p50 | p99 | max |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for label, block in item["latency"].items():
                block = block or {}
                lines.append(
                    f"| {label} | {fmt(block.get('n') or block.get('windows'))} | {fmt(block.get('min_ms'))} | {fmt(block.get('p50_ms'))} | {fmt(block.get('p99_ms'))} | {fmt(block.get('max_ms'))} |"
                )
            lines.append("")
        lines.append("和 Python 离线口径的差（按窗口取绝对差，再求和。UV 可以按窗口比，不能把各窗口 UV 加起来当成总用户数）：")
        lines.append("")
        lines.append(render_diff(item.get("vs_python")))
        lines.append("")
        lines.append("和 ClickHouse SQL 离线口径的差：")
        lines.append("")
        lines.append(render_diff(item.get("vs_clickhouse_sql")))
        lines.append("")
        if item.get("recovery"):
            lines.append("TaskManager 恢复：")
            lines.append("")
            rec = item["recovery"]
            lines.append("| 项 | 值 |")
            lines.append("| --- | --- |")
            for key in (
                "checkpoint_before",
                "kill_to_running_s",
                "states",
                "restored",
                "job_id_unchanged",
                "ods_physical_after",
                "ods_final_after",
                "note",
            ):
                if key in rec:
                    lines.append(f"| {key} | `{rec[key]}` |")
            lines.append("")
    spark = doc.get("spark") or {}
    lines.append("## Spark")
    lines.append("")
    if spark.get("status") == "ok":
        lines.append(f"pyspark 退出码 0。和 Python PV 的对比：`{spark.get('vs_python_pv')}`。")
        lines.append("")
        lines.append("输出尾部：")
        lines.append("")
        lines.append("```")
        lines.append(spark.get("output_tail") or "")
        lines.append("```")
    else:
        lines.append(f"未测到。原因：{spark.get('reason') or spark.get('status') or '没有 spark 段'}")
        if spark.get("output_tail"):
            lines.append("")
            lines.append("```")
            lines.append(spark.get("output_tail"))
            lines.append("```")
    lines.append("")
    py = doc.get("python_batch") or {}
    lines.append("## Python 离线规格")
    lines.append("")
    window_count = py.get("windows")
    if window_count is None:
        window_count = len(py.get("pv_uv") or [])
    lines.append(f"batch/metrics.py 计算耗时 {fmt(py.get('elapsed_s'))} 秒，重复事件 {fmt(py.get('duplicate_events'))}，窗口数 {fmt(window_count)}。")
    lines.append("它和 ClickHouse SQL 的 PV 差：")
    lines.append("")
    lines.append(render_diff(doc.get("python_vs_clickhouse_sql")))
    lines.append("")
    lines.append("## 测量范围")
    lines.append("")
    for item in doc.get("unfinished") or []:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def render_diff(diff: dict | None) -> str:
    if not diff:
        return "未测到"
    if diff.get("error"):
        return f"未测到。原因：{diff['error']}"
    lines = ["| 指标 | 对比窗口 | 有差异的窗口 | 绝对差之和 | 最大绝对差 | 实时合计 | 离线合计 |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for name, block in diff.items():
        if name == "topn":
            lines.append(
                f"| topn item/pv | rt {block.get('pairs_rt')} / batch {block.get('pairs_batch')} | 不一致 {fmt(block.get('pairs_item_or_pv_mismatch'))}，缺一侧 {fmt(block.get('pairs_missing_one_side'))} |  |  |  |  |"
            )
            continue
        if not isinstance(block, dict) or "sum_abs_diff" not in block:
            continue
        lines.append(
            f"| {name} | {fmt(block.get('windows_compared'))} | {fmt(block.get('windows_with_diff'))} | {fmt(block.get('sum_abs_diff'))} | {fmt(block.get('max_abs_diff'))} | {fmt(block.get('sum_rt'))} | {fmt(block.get('sum_batch'))} |"
        )
    return "\n".join(lines)


def save(doc: dict) -> None:
    OUT_JSON.write_text(json.dumps(doc, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    # 原始表太大，markdown 只保留汇总。JSON 里去掉逐窗口明细，避免把结果文件写成数据转储。
    slim = json.loads(json.dumps(doc, default=str))
    for item in (slim.get("runs") or {}).values():
        item.pop("rt", None)
    slim.pop("python_batch_rows", None)
    if isinstance(slim.get("spark"), dict):
        slim["spark"].pop("pv_uv", None)
    OUT_JSON.write_text(json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_MD.write_text(render(doc), encoding="utf-8")


def diff_bundle(rt: dict, batch_rows: dict, batch_top_key: str = "top_items") -> dict:
    try:
        pv = compare_metric(rt["pv_uv"], batch_rows["pv_uv"], ["pv", "uv", "cart_cnt", "buy_cnt", "dim_miss"])
        funnel_fields = ["pv_users", "cart_users", "buy_users", "pv_to_cart_users", "cart_to_buy_users", "pv_to_buy_users"]
        funnel = compare_metric(rt["funnel"], batch_rows["funnel"], funnel_fields)
        top = compare_top(rt["top_items"], batch_rows[batch_top_key])
        return {**pv, **funnel, "topn": top}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def main() -> int:
    doc: dict = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": machine_info(),
        "runs": {},
        "unfinished": [
            "Flink SQL 只写在 sql/flink_reference.sql，没有提交到集群，也没有进入这组数字。",
            "数据集只用了公开 CSV 的前 200000 行，不是全量两个月。",
            "Grafana 面板依赖 grafana-clickhouse-datasource 插件；若插件查询格式和当前版本不一致，以 ClickHouse 里的表为准。",
        ],
    }
    try:
        compose_up()
        doc["services"] = wait_healthy()
    except Exception as exc:  # noqa: BLE001
        doc["services_error"] = str(exc)
        doc["unfinished"].append(f"集群没有就绪：{exc}")
        save(doc)
        return 1

    jar = ROOT / "flink-jobs" / "target" / "pulseboard-pipeline.jar"
    if not jar.exists():
        doc["unfinished"].append("缺少 flink-jobs/target/pulseboard-pipeline.jar")
        save(doc)
        return 1

    try:
        load_batch_tables()
        batch_rows = batch_sql()
        py_rows = normalize_python(python_batch())
        doc["python_batch"] = {
            "elapsed_s": py_rows["elapsed_s"],
            "duplicate_events": py_rows["duplicate_events"],
            "pv_uv": py_rows["pv_uv"],
        }
        doc["python_vs_clickhouse_sql"] = diff_bundle(
            {"pv_uv": py_rows["pv_uv"], "funnel": py_rows["funnel"], "top_items": py_rows["top_items"]},
            {"pv_uv": batch_rows["pv_uv"], "funnel": batch_rows["funnel"], "top_items": batch_rows["top_items"]},
        )
        # 离线对照不需要逐行留在最终 JSON 的 python 段里，比较时用内存中的 py_rows。
        doc["python_batch"] = {
            "elapsed_s": py_rows["elapsed_s"],
            "duplicate_events": py_rows["duplicate_events"],
            "windows": len(py_rows["pv_uv"]),
        }
    except Exception as exc:  # noqa: BLE001
        batch_rows = None
        py_rows = None
        doc["unfinished"].append(f"离线口径计算失败：{exc}")
        log(f"batch failed: {exc}")

    for spec in EXPERIMENTS:
        run_id = spec["run_id"]
        item: dict = {}
        doc["runs"][run_id] = item
        try:
            cancel_running()
            create_topic(f"events-{run_id}")
            jid = submit_job(run_id)
            item["job_id"] = jid
            started = time.perf_counter()
            if spec.get("recovery"):
                proc, lines = start_replay(run_id, spec["rate"], spec["ratio"], spec["span_ms"])
                seen_checkpoint = False
                checkpoint_before = None
                deadline = time.time() + 180
                while time.time() < deadline and proc.poll() is None:
                    payload = checkpoints(jid)
                    completed = (payload.get("counts") or {}).get("completed") or 0
                    ods = ch_rows(f"SELECT count() AS c FROM pulseboard.ods_events WHERE run_id = '{run_id}'")
                    count = int((ods[0]["c"] if ods else 0) or 0)
                    log(f"recovery wait checkpoint={completed} ods={count}")
                    if completed >= 1 and count >= 10000:
                        seen_checkpoint = True
                        checkpoint_before = checkpoint_brief(payload)
                        break
                    time.sleep(3)
                recovery = {"checkpoint_before": checkpoint_before, "job_id": jid}
                if not seen_checkpoint:
                    recovery["note"] = "未测到：在回放结束前没有同时满足 checkpoint>=1 且 ODS>=10000，因此没有杀 TaskManager。"
                    item["recovery"] = recovery
                else:
                    states = []
                    t_kill = time.perf_counter()
                    docker("kill", "pulseboard-taskmanager", check=False)
                    # 心跳超时之前作业仍显示 RUNNING。先等到它离开 RUNNING，再启动，
                    # 否则计时会停在 docker 命令本身的耗时上。
                    left_running = False
                    while time.perf_counter() - t_kill < 60:
                        state = job_state(jid)
                        states.append({"after_kill_s": round(time.perf_counter() - t_kill, 3), "state": state})
                        if state and state != "RUNNING":
                            left_running = True
                            break
                        time.sleep(1)
                    docker("start", "pulseboard-taskmanager", check=False)
                    running_at = None
                    while time.perf_counter() - t_kill < 180:
                        state = job_state(jid)
                        states.append({"after_kill_s": round(time.perf_counter() - t_kill, 3), "state": state})
                        if left_running and state == "RUNNING":
                            overview = http_json(FLINK + "/overview")
                            if int(overview.get("taskmanagers") or 0) >= 1 and int(overview.get("slots-total") or 0) >= 1:
                                running_at = time.perf_counter()
                                break
                        time.sleep(1)
                    recovery["states"] = states
                    recovery["kill_to_running_s"] = None if running_at is None else round(running_at - t_kill, 3)
                    recovery["job_id_unchanged"] = job_state(jid) != ""
                    if running_at is None:
                        recovery["note"] = "未测到恢复完成：180 秒内作业没有回到 RUNNING。"
                        recovery["exceptions"] = job_exceptions(jid)
                try:
                    proc.wait(timeout=900)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    recovery["note"] = (recovery.get("note") or "") + " 回放进程超时。"
                text = "".join(lines)
                try:
                    item["replay"] = parse_replay(text)
                except Exception as exc:  # noqa: BLE001
                    item["replay_error"] = str(exc)
                    item["replay_tail"] = text[-1000:]
                item["recovery"] = recovery
            else:
                item["replay"] = run_replay(run_id, spec["rate"], spec["ratio"], spec["span_ms"])
            produced = int((item.get("replay") or {}).get("produced") or 0)
            item["settle"] = settle(run_id, produced)
            item["e2e_s"] = round(time.perf_counter() - started, 3)
            if item["e2e_s"] > 0 and produced:
                item["e2e_events_per_s"] = round(produced / item["e2e_s"], 2)
            item["checkpoint"] = checkpoint_brief(checkpoints(jid))
            if spec.get("recovery"):
                restored = checkpoint_brief(checkpoints(jid)).get("restored")
                item["recovery"]["restored"] = restored
                if not restored:
                    logs = capture([*DOCKER, "logs", "--tail", "200", "pulseboard-jobmanager"])
                    hits = [line for line in logs.splitlines() if "estor" in line.lower()]
                    item["recovery"]["jobmanager_restore_lines"] = hits[-8:]
            item["rt"] = rt_tables(run_id)
            if spec.get("recovery"):
                item["recovery"]["ods_physical_after"] = item["rt"].get("ods_physical")
                item["recovery"]["ods_final_after"] = item["rt"].get("ods_final")
            if spec.get("latency"):
                item["latency"] = latency(run_id)
            if py_rows is not None:
                item["vs_python"] = diff_bundle(item["rt"], py_rows)
            else:
                item["vs_python"] = {"error": "Python 离线结果不可用"}
            if batch_rows is not None:
                item["vs_clickhouse_sql"] = diff_bundle(item["rt"], batch_rows)
                try:
                    write_reconcile(run_id, item["rt"], batch_rows)
                except Exception as exc:  # noqa: BLE001
                    item["reconcile_error"] = str(exc)
            else:
                item["vs_clickhouse_sql"] = {"error": "ClickHouse SQL 结果不可用"}
            exc_text = job_exceptions(jid)
            if exc_text:
                item["exceptions"] = exc_text
        except Exception as exc:  # noqa: BLE001
            item["error"] = str(exc)
            log(f"{run_id} failed: {exc}")
        save(doc)

    try:
        cancel_running()
    except Exception as exc:  # noqa: BLE001
        doc["unfinished"].append(f"结束时取消作业失败：{exc}")
    try:
        doc["spark"] = try_spark()
        spark_rows = (doc.get("spark") or {}).get("pv_uv") or []
        if spark_rows and py_rows is not None:
            doc["spark"]["vs_python_pv"] = compare_metric(spark_rows, py_rows["pv_uv"], ["pv", "uv", "cart_cnt", "buy_cnt"])
    except Exception as exc:  # noqa: BLE001
        doc["spark"] = {"status": "未测到", "reason": str(exc)}
    doc["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save(doc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
