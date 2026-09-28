from batch.metrics import compute, window_start


def test_window_alignment_matches_epoch():
    assert window_start(1572566400000, 600_000) == 1572566400000
    assert window_start(1572566400000 + 599_999, 600_000) == 1572566400000
    assert window_start(1572566400000 + 600_000, 600_000) == 1572566400000 + 600_000


def test_funnel_and_topn_tie_break():
    events = [
        {"user_id": 1, "item_id": 10, "behavior": "pv", "event_time_ms": 1_000},
        {"user_id": 1, "item_id": 10, "behavior": "pv", "event_time_ms": 1_000},
        {"user_id": 1, "item_id": 11, "behavior": "cart", "event_time_ms": 2_000},
        {"user_id": 2, "item_id": 12, "behavior": "buy", "event_time_ms": 3_000},
        {"user_id": 3, "item_id": 10, "behavior": "pv", "event_time_ms": 4_000},
        {"user_id": 3, "item_id": 99, "behavior": "pv", "event_time_ms": 4_000},
    ]
    dim = {10: "phone", 11: "washer", 12: "shirt"}
    result = compute(events, window_ms=60_000, top_n=2, dim=dim)
    pv = result["pv_uv"][0]
    assert pv["pv"] == 4
    assert pv["uv"] == 2
    assert pv["cart_cnt"] == 1
    assert pv["buy_cnt"] == 1
    assert pv["dim_miss"] == 1
    funnel = result["funnel"][0]
    assert funnel["pv_users"] == 2
    assert funnel["cart_users"] == 1
    assert funnel["buy_users"] == 1
    assert funnel["pv_to_cart_users"] == 1
    assert funnel["pv_to_buy_users"] == 0
    assert result["duplicate_events"] == 1
    assert result["top_items"][0]["item_id"] == 10
    assert result["top_items"][0]["pv"] == 3
    assert result["top_items"][1]["item_id"] == 99
    assert result["top_items"][1]["category_code"] == "UNKNOWN"
