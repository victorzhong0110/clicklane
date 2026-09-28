import random

from replayer.disorder import apply_disorder, disorder_stats


class Bits:
    """random() < ratio 时扣住。0.0 表示扣住，0.99 表示放行。"""

    def __init__(self, hold_flags):
        self._values = iter(0.0 if flag else 0.99 for flag in hold_flags)

    def random(self):
        return next(self._values)


def _times(events):
    return [event["t"] for event in events]


def test_ratio_zero_keeps_order():
    events = [{"t": t} for t in (0, 10, 20, 30)]
    out = list(
        apply_disorder(events, 0.0, 1000, random.Random(1), lambda item: item["t"])
    )
    assert _times(out) == [0, 10, 20, 30]
    stats = disorder_stats(out, lambda item: item["t"])
    assert stats["inversions"] == 0


def test_ratio_one_flushes_in_order_at_eof():
    events = [{"t": t} for t in (0, 10, 20)]
    out = list(
        apply_disorder(
            events, 1.0, 50, Bits([True, True, True]), lambda item: item["t"]
        )
    )
    assert _times(out) == [0, 10, 20]


def test_held_events_are_released_after_span():
    events = [{"t": t} for t in (0, 10, 20, 30, 40)]
    # 不扣、扣、不扣、扣、不扣。span=25。
    out = list(
        apply_disorder(
            events,
            0.5,
            25,
            Bits([False, True, False, True, False]),
            lambda item: item["t"],
        )
    )
    assert _times(out) == [0, 20, 40, 10, 30]
    stats = disorder_stats(out, lambda item: item["t"])
    assert stats["inversions"] == 2
    assert stats["max_lateness_ms"] >= 25


def test_span_zero_does_not_hold():
    events = [{"t": t} for t in (0, 5, 9)]
    out = list(
        apply_disorder(events, 0.9, 0, Bits([True, True, True]), lambda item: item["t"])
    )
    assert _times(out) == [0, 5, 9]
