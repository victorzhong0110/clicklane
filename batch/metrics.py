"""离线口径。实时作业、ClickHouse SQL、Spark 都要和这里对齐。

窗口左端点按 epoch 对齐：start = event_time_ms - event_time_ms % window_ms。
PV 只数 behavior=pv，UV 是窗口内有过 pv 的用户数。
漏斗看的是用户，不是次数，并且后一步的最早时间不能早于前一步。
同一秒内的两个行为因为时间戳精度就是 1 秒，用 >= 而不是 >。
Top-N 按 pv 降序、item_id 升序，保证和 Java 侧的排序完全一致。
重复行不从 PV 里删掉，只在质量指标里另算。
"""

from __future__ import annotations


def window_start(event_time_ms: int, window_ms: int) -> int:
    if window_ms <= 0:
        raise ValueError("window_ms must be positive")
    return event_time_ms - (event_time_ms % window_ms)


def _empty_user():
    return {
        "pv": 0,
        "cart": 0,
        "buy": 0,
        "events": 0,
        "dim_miss": 0,
        "max_produce": 0,
        "min_pv": None,
        "min_cart": None,
        "min_buy": None,
    }


def compute(events, window_ms: int, top_n: int, dim: dict[int, str]) -> dict:
    users: dict[tuple[int, int], dict] = {}
    item_pv: dict[tuple[int, int], dict] = {}
    category_pv: dict[tuple[int, str], int] = {}
    duplicate_keys: set[tuple] = set()
    duplicates = 0

    for event in events:
        behavior = event["behavior"]
        if behavior not in {"pv", "cart", "buy", "fav", "remove"}:
            continue
        start = window_start(int(event["event_time_ms"]), window_ms)
        user_key = (start, int(event["user_id"]))
        slot = users.get(user_key)
        if slot is None:
            slot = _empty_user()
            users[user_key] = slot
        slot["events"] += 1
        produce = int(event.get("produce_time_ms") or 0)
        if produce > slot["max_produce"]:
            slot["max_produce"] = produce
        ts = int(event["event_time_ms"])
        code = dim.get(int(event["item_id"]), "UNKNOWN")
        if code == "UNKNOWN":
            slot["dim_miss"] += 1
        if behavior == "pv":
            slot["pv"] += 1
            slot["min_pv"] = ts if slot["min_pv"] is None else min(slot["min_pv"], ts)
            item_key = (start, int(event["item_id"]))
            item = item_pv.get(item_key)
            if item is None:
                item = {"pv": 0, "category_code": code}
                item_pv[item_key] = item
            item["pv"] += 1
            category_pv[(start, code)] = category_pv.get((start, code), 0) + 1
        elif behavior == "cart":
            slot["cart"] += 1
            slot["min_cart"] = ts if slot["min_cart"] is None else min(slot["min_cart"], ts)
        elif behavior == "buy":
            slot["buy"] += 1
            slot["min_buy"] = ts if slot["min_buy"] is None else min(slot["min_buy"], ts)

        natural = (
            int(event["user_id"]),
            int(event["item_id"]),
            behavior,
            ts,
        )
        if natural in duplicate_keys:
            duplicates += 1
        else:
            duplicate_keys.add(natural)

    pv_uv = []
    funnel = []
    by_window_users: dict[int, list[dict]] = {}
    for (start, _user), slot in users.items():
        by_window_users.setdefault(start, []).append(slot)

    for start in sorted(by_window_users):
        slots = by_window_users[start]
        pv = sum(slot["pv"] for slot in slots)
        cart = sum(slot["cart"] for slot in slots)
        buy = sum(slot["buy"] for slot in slots)
        events_n = sum(slot["events"] for slot in slots)
        dim_miss = sum(slot["dim_miss"] for slot in slots)
        uv = sum(1 for slot in slots if slot["pv"] > 0)
        max_produce = max(slot["max_produce"] for slot in slots)
        pv_uv.append(
            {
                "window_start_ms": start,
                "window_end_ms": start + window_ms,
                "pv": pv,
                "uv": uv,
                "cart_cnt": cart,
                "buy_cnt": buy,
                "events": events_n,
                "dim_miss": dim_miss,
                "max_produce_time_ms": max_produce,
            }
        )
        funnel.append(
            {
                "window_start_ms": start,
                "window_end_ms": start + window_ms,
                "pv_users": sum(1 for slot in slots if slot["min_pv"] is not None),
                "cart_users": sum(1 for slot in slots if slot["min_cart"] is not None),
                "buy_users": sum(1 for slot in slots if slot["min_buy"] is not None),
                "pv_to_cart_users": sum(
                    1
                    for slot in slots
                    if slot["min_pv"] is not None
                    and slot["min_cart"] is not None
                    and slot["min_cart"] >= slot["min_pv"]
                ),
                "cart_to_buy_users": sum(
                    1
                    for slot in slots
                    if slot["min_cart"] is not None
                    and slot["min_buy"] is not None
                    and slot["min_buy"] >= slot["min_cart"]
                ),
                "pv_to_buy_users": sum(
                    1
                    for slot in slots
                    if slot["min_pv"] is not None
                    and slot["min_buy"] is not None
                    and slot["min_buy"] >= slot["min_pv"]
                ),
            }
        )

    by_window_items: dict[int, list] = {}
    for (start, item_id), item in item_pv.items():
        by_window_items.setdefault(start, []).append((item_id, item))
    top_items = []
    for start in sorted(by_window_items):
        ranked = sorted(
            by_window_items[start],
            key=lambda pair: (-pair[1]["pv"], pair[0]),
        )[:top_n]
        for rank, (item_id, item) in enumerate(ranked, start=1):
            top_items.append(
                {
                    "window_start_ms": start,
                    "window_end_ms": start + window_ms,
                    "rank": rank,
                    "item_id": item_id,
                    "category_code": item["category_code"],
                    "pv": item["pv"],
                }
            )

    categories = []
    for (start, code), pv in sorted(category_pv.items()):
        categories.append(
            {
                "window_start_ms": start,
                "window_end_ms": start + window_ms,
                "category_code": code,
                "pv": pv,
            }
        )

    return {
        "pv_uv": pv_uv,
        "funnel": funnel,
        "top_items": top_items,
        "category_pv": categories,
        "duplicate_events": duplicates,
    }
