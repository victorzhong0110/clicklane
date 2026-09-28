import random

from replayer.inject import injection_summary, plan_sends


def _events():
    return [
        {"event_id": "1", "user_id": 1, "item_id": 9, "behavior": "pv", "event_time_ms": 1_000},
        {"event_id": "2", "user_id": 1, "item_id": 9, "behavior": "pv", "event_time_ms": 2_000},
        {"event_id": "3", "user_id": 2, "item_id": 8, "behavior": "cart", "event_time_ms": 3_000},
        {"event_id": "4", "user_id": 2, "item_id": 8, "behavior": "pv", "event_time_ms": 4_000},
    ]


def test_release_follows_event_time_when_nothing_is_injected():
    planned = plan_sends(
        _events(),
        speedup=1,
        rng=random.Random(1),
        jitter_ratio=0,
        late_ratio=0,
        duplicate_ratio=0,
        malformed=0,
    )
    assert [item["release_s"] for item in planned] == [0.0, 1.0, 2.0, 3.0]
    assert [item["event"]["event_id"] for item in planned] == ["1", "2", "3", "4"]


def test_late_delays_send_without_changing_event_time():
    class AlwaysLate:
        def random(self):
            return 0.0

        def uniform(self, low, high):
            return high

    planned = plan_sends(
        _events()[:1],
        speedup=1000,
        rng=AlwaysLate(),
        jitter_ratio=0.05,
        late_ratio=1,
        late_min_ms=180_000,
        late_max_ms=600_000,
        duplicate_ratio=0,
        malformed=0,
    )
    assert planned[0]["kind"] == "late"
    assert planned[0]["event"]["event_time_ms"] == 1_000
    # 600000 ms 事件时间 / speedup 1000 = 0.6 秒墙钟。
    assert abs(planned[0]["release_s"] - 0.6) < 1e-9


def test_duplicate_keeps_the_same_release_and_a_new_id():
    class DuplicateOnly:
        def __init__(self):
            self.calls = 0

        def random(self):
            self.calls += 1
            # 第一次决定迟到/抖动：不迟到。第二次决定是否复制：复制。
            return 0.99 if self.calls % 2 == 1 else 0.0

        def uniform(self, low, high):
            return low

    planned = plan_sends(
        _events()[:1],
        speedup=1,
        rng=DuplicateOnly(),
        jitter_ratio=0,
        late_ratio=0,
        duplicate_ratio=1,
        malformed=0,
    )
    assert [item["kind"] for item in planned] == ["data", "duplicate"]
    assert planned[0]["release_s"] == planned[1]["release_s"]
    assert planned[1]["event"]["event_id"] == "1d"
    assert planned[1]["seq"] > planned[0]["seq"]


def test_malformed_records_are_spread_and_sorted():
    planned = plan_sends(
        _events(),
        speedup=1000,
        rng=random.Random(11),
        jitter_ratio=0,
        late_ratio=0,
        duplicate_ratio=0,
        malformed=5,
    )
    summary = injection_summary(planned)
    assert summary["counts"]["malformed"] == 5
    assert set(summary["malformed_kinds"]) == {"bad_json", "bad_user", "bad_behavior", "bad_time", "empty"}
    releases = [item["release_s"] for item in planned]
    assert releases == sorted(releases)
