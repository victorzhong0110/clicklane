from replayer.schema import map_behavior, parse_event_time_ms


def test_rees46_behaviors():
    assert map_behavior("view") == "pv"
    assert map_behavior("cart") == "cart"
    assert map_behavior("purchase") == "buy"
    assert map_behavior("remove_from_cart") == "remove"


def test_taobao_behaviors_pass_through():
    assert map_behavior("pv") == "pv"
    assert map_behavior("fav") == "fav"
    assert map_behavior("buy") == "buy"


def test_iso_and_epoch_seconds():
    assert parse_event_time_ms("2019-11-01T00:00:00.000Z") == 1572566400000
    assert parse_event_time_ms("1511539200") == 1511539200000
    assert parse_event_time_ms("1572566400000") == 1572566400000
