"""把公开点击流样本规范成管道内部的一行事件。

支持两种输入：

1. REES46 / Michael Kechinov 多品类商店行为数据
   （event_time, event_type, product_id, category_id, category_code, ...）
2. 天池淘宝 UserBehavior
   （user_id, item_id, category_id, behavior, timestamp，timestamp 为秒）

业务行为统一成 pv / cart / buy / fav / remove，方便同一套 Flink 作业复用。
"""

from __future__ import annotations

from datetime import datetime, timezone

# 上游行为 -> 管道行为。未出现在表里的值原样保留，交给质量规则判非法。
BEHAVIOR_MAP = {
    "view": "pv",
    "pv": "pv",
    "click": "pv",
    "cart": "cart",
    "add_to_cart": "cart",
    "purchase": "buy",
    "buy": "buy",
    "order": "buy",
    "fav": "fav",
    "favorite": "fav",
    "remove_from_cart": "remove",
    "remove": "remove",
}


def map_behavior(raw: str) -> str:
    key = (raw or "").strip().lower()
    return BEHAVIOR_MAP.get(key, key)


def parse_event_time_ms(raw: str) -> int:
    """解析 ISO-8601（...Z）或纯数字秒/毫秒时间戳。"""
    text = (raw or "").strip()
    if not text:
        raise ValueError("empty event time")
    if text.isdigit():
        value = int(text)
        # 10 位及以下按秒，否则按毫秒。天池是秒，REES46 走 ISO 分支。
        if value < 10_000_000_000:
            return value * 1000
        return value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def category_or_unknown(code: str) -> str:
    text = (code or "").strip()
    return text if text else "UNKNOWN"
