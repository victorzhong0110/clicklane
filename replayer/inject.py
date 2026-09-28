"""按真实事件时间安排发送时刻，并注入抖动、迟到、重复和畸形记录。

release_s 是相对回放起点的秒数：

    (event_time_ms - origin_ms) / speedup / 1000 + extra_s
    extra_s = delay_ms / speedup / 1000

抖动和迟到只推迟发送，不改 event_time_ms。
如果先把事件时间加上延迟再按事件时间排序，乱序会被重新排掉。
同一事件秒里的记录 release_s 相同，会连在一起发出，只在秒与秒之间睡眠。
"""

from __future__ import annotations

MALFORMED_KINDS = ("bad_json", "bad_user", "bad_behavior", "bad_time", "empty")


def plan_sends(
    events: list[dict],
    speedup: float,
    rng,
    jitter_ratio: float = 0.05,
    jitter_ms: float = 30_000,
    late_ratio: float = 0.005,
    late_min_ms: float = 180_000,
    late_max_ms: float = 600_000,
    duplicate_ratio: float = 0.002,
    malformed: int = 50,
) -> list[dict]:
    if speedup <= 0:
        raise ValueError("speedup must be > 0")
    if not events:
        return []
    if late_min_ms > late_max_ms:
        raise ValueError("late_min_ms must be <= late_max_ms")
    origin = min(int(event["event_time_ms"]) for event in events)
    end = max(int(event["event_time_ms"]) for event in events)
    planned: list[dict] = []
    seq = 0
    for event in events:
        base = (int(event["event_time_ms"]) - origin) / speedup / 1000.0
        extra_ms = 0.0
        kind = "data"
        roll = rng.random()
        if roll < late_ratio:
            extra_ms = rng.uniform(late_min_ms, late_max_ms)
            kind = "late"
        elif roll < late_ratio + jitter_ratio:
            extra_ms = rng.uniform(0.0, jitter_ms)
            kind = "jitter"
        planned.append(
            {
                "release_s": base + extra_ms / speedup / 1000.0,
                "seq": seq,
                "kind": kind,
                "event": event,
                "malform": None,
            }
        )
        seq += 1
        if rng.random() < duplicate_ratio:
            duplicate = dict(event)
            duplicate["event_id"] = f"{event['event_id']}d"
            planned.append(
                {
                    "release_s": planned[-1]["release_s"],
                    "seq": seq,
                    "kind": "duplicate",
                    "event": duplicate,
                    "malform": None,
                }
            )
            seq += 1
    span_s = (end - origin) / speedup / 1000.0
    for index in range(max(0, int(malformed))):
        # 均匀铺在时间轴上，不扎堆在文件头。
        release = 0.0 if span_s <= 0 else span_s * (index + 0.5) / malformed
        planned.append(
            {
                "release_s": release,
                "seq": seq,
                "kind": "malformed",
                "event": None,
                "malform": MALFORMED_KINDS[index % len(MALFORMED_KINDS)],
            }
        )
        seq += 1
    planned.sort(key=lambda item: (item["release_s"], item["seq"]))
    return planned


def injection_summary(planned: list[dict]) -> dict:
    counts: dict[str, int] = {}
    malforms: dict[str, int] = {}
    for item in planned:
        counts[item["kind"]] = counts.get(item["kind"], 0) + 1
        if item["kind"] == "malformed":
            name = item["malform"] or "unknown"
            malforms[name] = malforms.get(name, 0) + 1
    return {"counts": counts, "malformed_kinds": malforms, "planned": len(planned)}
