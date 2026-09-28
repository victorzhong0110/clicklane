"""下载（或读取本地）公开点击流，整理成回放样本和商品维表。

默认数据源是 Michael Kechinov 发布、REES46 / Open CDP 采集的
“eCommerce behavior data from multi category store”。
许可证：可免费用于研究、书籍和教学，需要注明出处。
我们只取文件开头的若干行，不把样本提交进 git。

也接受已经落在 data/raw/ 的天池 UserBehavior CSV。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

from replayer.schema import category_or_unknown, map_behavior, parse_event_time_ms

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
SAMPLE_DIR = ROOT / "data" / "sample"
DIM_DIR = ROOT / "data" / "dim"

DEFAULT_URL = (
    "https://huggingface.co/datasets/kevykibbz/"
    "ecommerce-behavior-data-from-multi-category-store_oct-nov_2019/"
    "resolve/main/ecommerce-behavior-data-from-multi-category-store_oct-nov_2019.csv"
)

SOURCE = {
    "name": "eCommerce behavior data from multi category store",
    "author": "Michael Kechinov",
    "collector": "REES46 / Open CDP",
    "page": "https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store",
    "mirror": DEFAULT_URL,
    "license": (
        "Dataset card: free to use for research, books, and educational materials "
        "with attribution to REES46 and the Kaggle dataset page. "
        "This repository stores only a download script, not the rows."
    ),
}


def download_head(url: str, dest: Path, max_lines: int) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "pulseboard-prepare"})
    with urllib.request.urlopen(req, timeout=120) as resp, dest.open("w", encoding="utf-8") as out:
        for i, raw in enumerate(resp):
            out.write(raw.decode("utf-8", "replace"))
            if i + 1 >= max_lines:
                break


def _row_from_rees46(row: dict, event_id: str) -> dict | None:
    behavior = map_behavior(row.get("event_type", ""))
    try:
        user_id = int(row["user_id"])
        item_id = int(row["product_id"])
        category_id = int(row["category_id"]) if row.get("category_id") else -1
        event_time_ms = parse_event_time_ms(row["event_time"])
    except (KeyError, ValueError):
        return None
    return {
        "event_id": event_id,
        "user_id": user_id,
        "item_id": item_id,
        "behavior": behavior,
        "category_id": category_id,
        "category_code": category_or_unknown(row.get("category_code", "")),
        "event_time_ms": event_time_ms,
    }


def _row_from_taobao(parts: list[str], event_id: str) -> dict | None:
    # user_id, item_id, category_id, behavior, timestamp_seconds
    if len(parts) < 5:
        return None
    try:
        user_id = int(parts[0])
        item_id = int(parts[1])
        category_id = int(parts[2])
        behavior = map_behavior(parts[3])
        event_time_ms = parse_event_time_ms(parts[4])
    except ValueError:
        return None
    return {
        "event_id": event_id,
        "user_id": user_id,
        "item_id": item_id,
        "behavior": behavior,
        "category_id": category_id,
        "category_code": f"cat-{category_id}",
        "event_time_ms": event_time_ms,
    }


def load_rows(path: Path, limit: int) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8", newline="") as handle:
        head = handle.readline()
        if not head:
            return rows
        lowered = head.lower()
        if "event_type" in lowered or "event_time" in lowered:
            handle.seek(0)
            reader = csv.DictReader(handle)
            for i, raw in enumerate(reader, start=1):
                if len(rows) >= limit:
                    break
                parsed = _row_from_rees46(raw, str(i))
                if parsed:
                    rows.append(parsed)
            return rows
        # 无表头：先尝试把首行当淘宝数据。
        first = _row_from_taobao(head.strip().split(","), "1")
        if first:
            rows.append(first)
        for i, line in enumerate(handle, start=2):
            if len(rows) >= limit:
                break
            parsed = _row_from_taobao(line.strip().split(","), str(i))
            if parsed:
                rows.append(parsed)
    return rows


def build_dim(rows: list[dict]) -> list[dict]:
    """每个商品选出现次数最多的类目；并列时取字典序较小的 category_code。"""
    counts: dict[int, Counter] = defaultdict(Counter)
    category_ids: dict[tuple[int, str], Counter] = defaultdict(Counter)
    for row in rows:
        code = category_or_unknown(row["category_code"])
        counts[row["item_id"]][code] += 1
        category_ids[(row["item_id"], code)][row["category_id"]] += 1
    dim = []
    for item_id, counter in counts.items():
        best_count = max(counter.values())
        candidates = sorted(code for code, n in counter.items() if n == best_count)
        code = candidates[0]
        cat_id = category_ids[(item_id, code)].most_common(1)[0][0]
        dim.append(
            {
                "item_id": item_id,
                "category_id": cat_id,
                "category_code": code,
            }
        )
    dim.sort(key=lambda item: item["item_id"])
    return dim


def write_outputs(
    rows: list[dict],
    dim: list[dict],
    source_name: str,
    events_path: Path | None = None,
    dim_path: Path | None = None,
    meta_path: Path | None = None,
) -> dict:
    events_path = events_path or (SAMPLE_DIR / "events.csv")
    dim_path = dim_path or (DIM_DIR / "dim_item.csv")
    meta_path = meta_path or (SAMPLE_DIR / "meta.json")
    events_path.parent.mkdir(parents=True, exist_ok=True)
    dim_path.parent.mkdir(parents=True, exist_ok=True)
    # 按事件时间稳定排序，回放器再在此基础上制造乱序。
    rows.sort(key=lambda row: (row["event_time_ms"], int(row["event_id"])))
    with events_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["event_id", "user_id", "item_id", "behavior", "event_time_ms"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "event_id": row["event_id"],
                    "user_id": row["user_id"],
                    "item_id": row["item_id"],
                    "behavior": row["behavior"],
                    "event_time_ms": row["event_time_ms"],
                }
            )
    with dim_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["item_id", "category_id", "category_code"]
        )
        writer.writeheader()
        writer.writerows(dim)

    digest = hashlib.sha256(events_path.read_bytes()).hexdigest()
    behaviors = Counter(row["behavior"] for row in rows)
    min_ts = rows[0]["event_time_ms"] if rows else 0
    max_ts = rows[-1]["event_time_ms"] if rows else 0
    meta = {
        "source": SOURCE,
        "input": source_name,
        "rows": len(rows),
        "unique_items": len(dim),
        "unique_users": len({row["user_id"] for row in rows}),
        "behaviors": dict(behaviors),
        "min_event_time_ms": min_ts,
        "max_event_time_ms": max_ts,
        "span_ms": max_ts - min_ts if rows else 0,
        # 样本大约覆盖数小时。10 分钟窗口能得到三十个左右的点，看板和乱序实验都够用。
        "window_ms": 600_000,
        "watermark_ms": 120_000,
        "sha256_events": digest,
        "events_csv": str(events_path.resolve().relative_to(ROOT)),
        "dim_csv": str(dim_path.resolve().relative_to(ROOT)),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="准备 PulseBoard 样本和维表")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="本地 CSV。缺省时使用 data/raw/sample_head.csv，没有就下载。",
    )
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument(
        "--events-out",
        type=Path,
        default=None,
        help="写到别的路径，避免覆盖 data/sample/events.csv。",
    )
    parser.add_argument("--dim-out", type=Path, default=None)
    parser.add_argument("--meta-out", type=Path, default=None)
    args = parser.parse_args(argv)

    raw_path = args.input or (RAW_DIR / "sample_head.csv")
    if not raw_path.exists():
        if args.skip_download:
            print(f"missing {raw_path}", file=sys.stderr)
            return 1
        print(f"downloading first {args.rows + 1} lines -> {raw_path}")
        download_head(args.url, raw_path, args.rows + 1)
    rows = load_rows(raw_path, args.rows)
    if not rows:
        print("no rows parsed", file=sys.stderr)
        return 1
    dim = build_dim(rows)
    meta = write_outputs(
        rows,
        dim,
        str(raw_path),
        events_path=args.events_out,
        dim_path=args.dim_out,
        meta_path=args.meta_out,
    )
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
