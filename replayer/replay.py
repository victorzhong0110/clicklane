"""按事件时间把样本打进 Kafka。

--rate 0 表示尽快发送，用来测吞吐。
--disorder-ratio / --disorder-span-ms 控制乱序比例和事件时间回退幅度。
--speedup > 0 时改走真实事件时间加速回放：按 (事件时间 - 起点) / speedup 睡眠，
并注入抖动、超出水位的迟到、重复和畸形记录。speedup 为 0 时保持原来的限速/乱序路径。

发送结束后向每个分区写一条 watermark_kick，把水位推过最后一个窗口。
Kafka 源是无界的，没有这几条“踢一脚”的事件，最后一段时间的窗口不会关闭。
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path

from replayer.disorder import apply_disorder, disorder_stats
from replayer.inject import injection_summary, plan_sends

ROOT = Path(__file__).resolve().parents[1]


def load_events(path: Path, limit: int) -> list[dict]:
    events = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            events.append(
                {
                    "event_id": row["event_id"],
                    "user_id": int(row["user_id"]),
                    "item_id": int(row["item_id"]),
                    "behavior": row["behavior"],
                    "event_time_ms": int(row["event_time_ms"]),
                }
            )
            if limit and len(events) >= limit:
                break
    events.sort(key=lambda item: (item["event_time_ms"], int(item["event_id"])))
    return events


def build_payload(event: dict, run_id: str, produce_time_ms: int, kick: bool) -> dict:
    return {
        "event_id": event["event_id"],
        "run_id": run_id,
        "user_id": event["user_id"],
        "item_id": event["item_id"],
        "behavior": event["behavior"],
        "event_time_ms": event["event_time_ms"],
        "produce_time_ms": produce_time_ms,
        "watermark_kick": kick,
    }


def kick_timestamp(max_event_time_ms: int, window_ms: int, watermark_ms: int) -> int:
    # 水位 = maxTs - watermark - 1，要盖过最后一个窗口的右端点。
    return max_event_time_ms + window_ms + watermark_ms + 60_000


class _Clock:
    def __init__(self, rate: float):
        self.rate = rate
        self.start = time.perf_counter()
        self.sent = 0

    def wait(self) -> None:
        self.sent += 1
        if self.rate <= 0:
            return
        if self.sent % 100 != 0:
            return
        target = self.sent / self.rate
        delay = target - (time.perf_counter() - self.start)
        if delay > 0:
            time.sleep(delay)


def malformed_value(kind: str, run_id: str, index: int) -> bytes:
    token = f"{run_id}-{index}"
    if kind == "bad_json":
        return f"{{not-json-{token}".encode()
    if kind == "empty":
        return b""
    if kind == "bad_user":
        payload = {
            "event_id": f"bad-user-{token}",
            "run_id": run_id,
            "user_id": 0,
            "item_id": 1,
            "behavior": "pv",
            "event_time_ms": 1572566400000,
            "produce_time_ms": int(time.time() * 1000),
            "watermark_kick": False,
        }
    elif kind == "bad_behavior":
        payload = {
            "event_id": f"bad-behavior-{token}",
            "run_id": run_id,
            "user_id": 1,
            "item_id": 1,
            "behavior": "nope",
            "event_time_ms": 1572566400000,
            "produce_time_ms": int(time.time() * 1000),
            "watermark_kick": False,
        }
    elif kind == "bad_time":
        payload = {
            "event_id": f"bad-time-{token}",
            "run_id": run_id,
            "user_id": 1,
            "item_id": 1,
            "behavior": "pv",
            "event_time_ms": 1,
            "produce_time_ms": int(time.time() * 1000),
            "watermark_kick": False,
        }
    else:
        raise ValueError(kind)
    return json.dumps(payload, separators=(",", ":")).encode()


def _send(producer, topic: str, key: bytes | None, value: bytes, partition: int | None = None) -> None:
    kwargs = {"value": value}
    if key is not None:
        kwargs["key"] = key
    if partition is not None:
        kwargs["partition"] = partition
    producer.send(topic, **kwargs)


def _emit_kicks(args, producer, max_ts: int) -> int:
    if not args.kick:
        return 0
    kick_ts = kick_timestamp(max_ts, args.window_ms, args.watermark_ms)
    kicks = 0
    for partition in range(args.partitions):
        payload = build_payload(
            {
                "event_id": f"kick-{args.run_id}-{partition}",
                "user_id": 0,
                "item_id": 0,
                "behavior": "kick",
                "event_time_ms": kick_ts,
            },
            args.run_id,
            int(time.time() * 1000),
            kick=True,
        )
        _send(
            producer,
            args.topic,
            None,
            json.dumps(payload, separators=(",", ":")).encode(),
            partition=partition,
        )
        kicks += 1
    return kicks


def replay_paced(args, producer, events: list[dict]) -> dict:
    rng = random.Random(args.seed)
    planned = plan_sends(
        events,
        speedup=args.speedup,
        rng=rng,
        jitter_ratio=args.jitter_ratio,
        jitter_ms=args.jitter_ms,
        late_ratio=args.late_ratio,
        late_min_ms=args.late_min_ms,
        late_max_ms=args.late_max_ms,
        duplicate_ratio=args.duplicate_ratio,
        malformed=args.malformed,
    )
    summary = injection_summary(planned)
    started = time.perf_counter()
    behind_s = 0.0
    malformed_index = 0
    sent = 0
    for item in planned:
        delay = item["release_s"] - (time.perf_counter() - started)
        if delay > 0:
            time.sleep(delay)
        else:
            behind_s = max(behind_s, -delay)
        if item["kind"] == "malformed":
            value = malformed_value(item["malform"], args.run_id, malformed_index)
            malformed_index += 1
            _send(producer, args.topic, b"bad", value)
        else:
            event = item["event"]
            payload = build_payload(event, args.run_id, int(time.time() * 1000), kick=False)
            _send(
                producer,
                args.topic,
                str(event["user_id"]).encode(),
                json.dumps(payload, separators=(",", ":")).encode(),
            )
        sent += 1
        if sent % 20000 == 0:
            print(f"produced {sent}", file=sys.stderr, flush=True)
    max_ts = max(event["event_time_ms"] for event in events)
    kicks = _emit_kicks(args, producer, max_ts)
    producer.flush()
    elapsed = time.perf_counter() - started
    data_rows = summary["counts"].get("data", 0) + summary["counts"].get("jitter", 0) + summary["counts"].get("late", 0)
    report = {
        "run_id": args.run_id,
        "topic": args.topic,
        "mode": "paced",
        "produced": data_rows + summary["counts"].get("duplicate", 0),
        "planned": summary["planned"],
        "kicks": kicks,
        "elapsed_s": round(elapsed, 3),
        "produce_rate": round(sent / elapsed, 2) if elapsed > 0 else None,
        "speedup": args.speedup,
        "seed": args.seed,
        "injection": summary,
        "max_behind_s": round(behind_s, 3),
        "input_rows": len(events),
        "min_event_time_ms": min(event["event_time_ms"] for event in events),
        "max_event_time_ms": max_ts,
    }
    print("PULSEBOARD_REPLAY " + json.dumps(report, ensure_ascii=False), flush=True)
    return report


def replay(args, producer) -> dict:
    events = load_events(args.input, args.limit)
    if not events:
        raise SystemExit("no events to replay")
    if args.speedup > 0:
        return replay_paced(args, producer, events)
    rng = random.Random(args.seed)
    ordered = list(events)
    disordered = list(
        apply_disorder(
            ordered,
            ratio=args.disorder_ratio,
            span_ms=args.disorder_span_ms,
            rng=rng,
            time_of=lambda item: item["event_time_ms"],
        )
    )
    stats = disorder_stats(disordered, lambda item: item["event_time_ms"])
    clock = _Clock(args.rate)
    started = time.perf_counter()
    for event in disordered:
        produce_ms = int(time.time() * 1000)
        payload = build_payload(event, args.run_id, produce_ms, kick=False)
        producer.send(
            args.topic,
            key=str(event["user_id"]).encode(),
            value=json.dumps(payload, separators=(",", ":")).encode(),
        )
        clock.wait()
        if clock.sent % 20000 == 0:
            print(f"produced {clock.sent}", file=sys.stderr, flush=True)

    max_ts = max(event["event_time_ms"] for event in ordered)
    kicks = _emit_kicks(args, producer, max_ts)
    producer.flush()
    elapsed = time.perf_counter() - started
    report = {
        "run_id": args.run_id,
        "topic": args.topic,
        "produced": len(disordered),
        "kicks": kicks,
        "elapsed_s": round(elapsed, 3),
        "produce_rate": round(len(disordered) / elapsed, 2) if elapsed > 0 else None,
        "disorder_ratio": args.disorder_ratio,
        "disorder_span_ms": args.disorder_span_ms,
        "rate_limit": args.rate,
        "inversions": stats["inversions"],
        "max_lateness_ms": stats["max_lateness_ms"],
        "seed": args.seed,
        "mode": "rate",
        "speedup": 0,
    }
    print("PULSEBOARD_REPLAY " + json.dumps(report, ensure_ascii=False), flush=True)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="把点击流样本回放到 Kafka")
    parser.add_argument(
        "--input",
        type=Path,
        default=ROOT / "data" / "sample" / "events.csv",
    )
    parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rate", type=float, default=0, help="条/秒，0 表示不限速")
    parser.add_argument("--disorder-ratio", type=float, default=0.0)
    parser.add_argument("--disorder-span-ms", type=int, default=0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--window-ms", type=int, default=600_000)
    parser.add_argument("--watermark-ms", type=int, default=120_000)
    parser.add_argument("--kick", action="store_true", default=True)
    parser.add_argument("--no-kick", action="store_false", dest="kick")
    parser.add_argument("--speedup", type=float, default=0, help="事件时间 / 墙钟。0 表示走原来的 --rate 路径")
    parser.add_argument("--jitter-ratio", type=float, default=0.05)
    parser.add_argument("--jitter-ms", type=float, default=30_000)
    parser.add_argument("--late-ratio", type=float, default=0.005)
    parser.add_argument("--late-min-ms", type=float, default=180_000)
    parser.add_argument("--late-max-ms", type=float, default=600_000)
    parser.add_argument("--duplicate-ratio", type=float, default=0.002)
    parser.add_argument("--malformed", type=int, default=50)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--delivery-timeout-ms", type=int, default=120_000)
    parser.add_argument("--request-timeout-ms", type=int, default=30_000)
    parser.add_argument("--max-block-ms", type=int, default=60_000)
    args = parser.parse_args(argv)

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        linger_ms=20,
        batch_size=64 * 1024,
        retries=args.retries,
        enable_idempotence=True,
        request_timeout_ms=args.request_timeout_ms,
        delivery_timeout_ms=args.delivery_timeout_ms,
        max_block_ms=args.max_block_ms,
    )
    try:
        replay(args, producer)
    finally:
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
