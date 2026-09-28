"""把 2019-11-01 全天按事件时间回放进当前管道。

只追加 docs/measurements-followup.json 的 day-20191101。
不改 benchmark-realistic.md。行留在 ClickHouse 里，方便看昼夜波峰；
若查询因内存失败，记录错误后再删。
"""

from __future__ import annotations

import csv
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import benchmark as bench  # noqa: E402
import benchmark_realistic as realistic  # noqa: E402
import followup_bench as follow  # noqa: E402
from batch.metrics import compute  # noqa: E402

RUN_ID = "day-20191101"
EVENTS = ROOT / "data" / "realistic-day" / "events.csv"
DIM = ROOT / "data" / "dim" / "dim_item_day.csv"
META = ROOT / "data" / "realistic-day" / "meta.json"
# 86399000 ms 事件跨度压到约 200 秒墙钟。
SPEEDUP = 86399000 / 200000


def file_batch() -> tuple[dict, dict, list[dict]]:
    meta = json.loads(META.read_text(encoding="utf-8"))
    dim = {}
    with DIM.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            dim[int(row["item_id"])] = row["category_code"] or "UNKNOWN"
    events = []
    seen = set()
    file_dup = 0
    file_dup_pv = 0
    hourly = Counter()
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
    meta["hourly_file"] = [{"hour_utc": hour, "events": hourly[hour]} for hour in sorted(hourly)]
    meta["file_duplicate_events"] = file_dup
    meta["file_duplicate_pv"] = file_dup_pv
    meta["speedup"] = SPEEDUP
    meta["target_wall_s"] = 200
    if result["duplicate_events"] != file_dup:
        raise RuntimeError(f"file duplicate count {file_dup} != metrics {result['duplicate_events']}")
    py_rows = bench.normalize_python(result)
    return meta, py_rows, result["pv_uv"]


def paced_command(run_id: str) -> list[str]:
    cmd = realistic.paced_command(run_id, SPEEDUP, 0)
    for index, token in enumerate(cmd):
        if token == "--input":
            cmd[index + 1] = str(EVENTS)
            break
    return cmd


def start_replay(run_id: str):
    import subprocess
    import threading

    lines: list[str] = []
    proc = subprocess.Popen(
        paced_command(run_id),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    def _read() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line)
            bench.log(line.rstrip())

    threading.Thread(target=_read, daemon=True).start()
    return proc, lines


def ch_memory() -> dict:
    try:
        rows = bench.ch_rows(
            """
            SELECT metric, value
            FROM system.metrics
            WHERE metric IN ('MemoryTracking', 'Query')
            """
        )
        return {row["metric"]: row["value"] for row in rows}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def hourly_streaming(run_id: str) -> dict:
    ods = bench.ch_rows(
        f"""
        SELECT toHour(event_time) AS hour_utc, count() AS events
        FROM pulseboard.ods_events FINAL
        WHERE run_id = '{run_id}'
        GROUP BY hour_utc
        ORDER BY hour_utc
        """
    )
    ads = bench.ch_rows(
        f"""
        SELECT toHour(window_start) AS hour_utc,
               sum(pv) AS pv,
               sum(events) AS events,
               sum(uv) AS uv_sum_of_windows
        FROM pulseboard.ads_pv_uv FINAL
        WHERE run_id = '{run_id}'
        GROUP BY hour_utc
        ORDER BY hour_utc
        """
    )
    return {
        "ods_by_event_hour": ods,
        "ads_by_window_hour": ads,
        "uv_note": "uv_sum_of_windows 是各 10 分钟窗口 UV 相加，不是该小时的去重用户数。",
    }


def main() -> int:
    doc = follow.load()
    item: dict = {"kind": "day", "speedup": SPEEDUP, "input": str(EVENTS)}
    doc["runs"][RUN_ID] = item
    follow.save(doc)
    try:
        bench.log("loading day file and computing offline metrics")
        meta, py_rows, _pv = file_batch()
        item["dataset"] = {
            "rows": meta.get("rows"),
            "span_ms": meta.get("span_ms"),
            "min_event_time_ms": meta.get("min_event_time_ms"),
            "max_event_time_ms": meta.get("max_event_time_ms"),
            "sha256_events": meta.get("sha256_events"),
            "behaviors": meta.get("behaviors"),
            "file_duplicate_events": meta.get("file_duplicate_events"),
            "file_duplicate_pv": meta.get("file_duplicate_pv"),
            "hourly_file": meta.get("hourly_file"),
            "python_elapsed_s": py_rows.get("elapsed_s"),
            "python_duplicate_events": py_rows.get("duplicate_events"),
            "speedup": SPEEDUP,
        }
        follow.save(doc)
        file_dup_pv = int(meta["file_duplicate_pv"])
        follow.ensure_tm()
        bench.cancel_running()
        follow.recreate(f"events-{RUN_ID}", 3)
        jid = follow.submit(RUN_ID, dim="/opt/data/dim_item_day.csv")
        item["job_id"] = jid
        follow.save(doc)
        proc, lines = start_replay(RUN_ID)
        item["replay"] = realistic.finish_replay(proc, lines, timeout=900)
        follow.save(doc)
        produced = int((item.get("replay") or {}).get("produced") or 0)
        item["settle"] = bench.settle(RUN_ID, produced, timeout=420)
        item["exceptions"] = (bench.job_exceptions(jid) or "")[:1500]
        item["checkpoint"] = follow.checkpoint_now(jid)
        rt = bench.rt_tables(RUN_ID)
        dup = bench.ch_rows(
            f"""
            SELECT count() AS duplicate_events, countIf(behavior = 'pv') AS duplicate_pv
            FROM pulseboard.dwd_duplicate_events FINAL
            WHERE run_id = '{RUN_ID}'
            """
        )
        rt["duplicate_pv_row"] = dup[0] if dup else None
        batch_pv = sum(bench.num(row["pv"]) for row in py_rows["pv_uv"])
        item["summary"] = {
            "windows": len(rt.get("pv_uv") or []),
            "late": rt.get("late"),
            "duplicates": rt.get("duplicates"),
            "duplicate_pv": rt.get("duplicate_pv_row"),
            "ods_physical": rt.get("ods_physical"),
            "ods_final": rt.get("ods_final"),
        }
        item["accuracy"] = {
            "vs_python": bench.diff_bundle(rt, py_rows),
            "pv_identity": realistic.pv_identity(rt, batch_pv, file_dup_pv),
        }
        item["latency_quantile_exact"] = follow.quantile_exact(RUN_ID)
        item["hourly_streaming"] = hourly_streaming(RUN_ID)
        item["clickhouse_memory"] = ch_memory()
        item["kept_in_clickhouse"] = True
        follow.save(doc)
        bench.log(f"day identity {item['accuracy']['pv_identity']}")
    except Exception as exc:  # noqa: BLE001
        item["error"] = str(exc)
        item["clickhouse_memory"] = ch_memory()
        follow.save(doc)
        bench.log(f"day failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
