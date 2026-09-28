"""不限速、多进程加压。

每条记录在发送时改写 produce_time_ms，延迟仍是插入时间减生产时间。
模板在进程启动时编一次，避免每条都走 json.dumps。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from replayer.replay import build_payload, load_events  # noqa: E402

TOKEN = b'"produce_time_ms":0000000000000'


def _templates(events: list[dict], run_id: str) -> list[tuple[bytes, bytes]]:
    built = []
    for event in events:
        payload = build_payload(event, run_id, 0, kick=False)
        raw = json.dumps(payload, separators=(",", ":")).encode()
        raw = raw.replace(b'"produce_time_ms":0', TOKEN, 1)
        if TOKEN not in raw:
            raise SystemExit("produce_time placeholder missing")
        built.append((str(event["user_id"]).encode(), raw))
    return built


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="向 Kafka 不限速加压")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--loops", type=int, default=0, help="0 表示按秒数循环；正数表示只发这么多轮")
    args = parser.parse_args(argv)

    events = load_events(args.input, 0)
    mine = [event for index, event in enumerate(events) if index % args.shards == args.shard]
    templates = _templates(mine, args.run_id)
    if not templates:
        raise SystemExit("shard has no events")

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        linger_ms=5,
        batch_size=128 * 1024,
        retries=3,
        enable_idempotence=True,
        request_timeout_ms=15000,
        delivery_timeout_ms=30000,
        max_block_ms=5000,
        buffer_memory=64 * 1024 * 1024,
    )
    started = time.perf_counter()
    deadline = started + args.seconds
    sent = 0
    blocked = 0
    loops = 0
    try:
        while True:
            if args.loops and loops >= args.loops:
                break
            if not args.loops and time.perf_counter() >= deadline:
                break
            for key, template in templates:
                if not args.loops and time.perf_counter() >= deadline:
                    break
                now_ms = str(int(time.time() * 1000)).encode()
                if len(now_ms) != 13:
                    raise SystemExit(f"produce_time is not 13 digits: {now_ms!r}")
                value = template.replace(TOKEN, b'"produce_time_ms":' + now_ms, 1)
                try:
                    producer.send(args.topic, key=key, value=value)
                except Exception:
                    blocked += 1
                    time.sleep(0.05)
                sent += 1
            loops += 1
        producer.flush(timeout=60)
    finally:
        producer.close()
    elapsed = time.perf_counter() - started
    report = {
        "shard": args.shard,
        "shards": args.shards,
        "sent": sent,
        "loops": loops,
        "blocked": blocked,
        "elapsed_s": round(elapsed, 3),
        "produce_rate": round(sent / elapsed, 2) if elapsed else None,
        "shard_rows": len(templates),
    }
    print("PULSEBOARD_BLAST " + json.dumps(report), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
