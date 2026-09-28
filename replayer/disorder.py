"""事件时间乱序。

回放器先把样本按事件时间排好，再按比例“扣住”一些事件。
被扣住的事件要等到已经发出去的最大事件时间超过它 `span_ms` 才放行，
于是下游看到的事件时间会回退大约 span_ms。

比例必须小于 1：总要有一部分事件直接发出去，水位才会往前走。
比例为 1 时谁也不放行，最后只能按原顺序吐出缓冲区，乱序消失。
文件尾部剩下的事件同样按事件时间吐出——后面没有更晚的事件能把它们衬成迟到。
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator
from typing import TypeVar

T = TypeVar("T")


def apply_disorder(
    events: Iterable[T],
    ratio: float,
    span_ms: int,
    rng,
    time_of,
    buffer_cap: int = 100_000,
) -> Iterator[T]:
    """yield 乱序后的事件。time_of(event) -> int 毫秒。rng.random() -> [0, 1)。"""
    if ratio < 0 or ratio > 1:
        raise ValueError("disorder ratio must be in [0, 1]")
    if span_ms < 0:
        raise ValueError("disorder span must be >= 0")

    pending: list[tuple[int, int, T]] = []
    max_emitted: int | None = None
    seq = 0

    def release_ready() -> Iterator[T]:
        while pending and max_emitted is not None and pending[0][0] <= max_emitted:
            _, _, held = heapq.heappop(pending)
            yield held

    for event in events:
        if max_emitted is not None:
            yield from release_ready()
        hold = (
            ratio > 0
            and span_ms > 0
            and rng.random() < ratio
            and len(pending) < buffer_cap
        )
        if hold:
            ts = int(time_of(event))
            heapq.heappush(pending, (ts + span_ms, seq, event))
            seq += 1
            continue
        ts = int(time_of(event))
        if max_emitted is None or ts > max_emitted:
            max_emitted = ts
        yield event
        yield from release_ready()

    # 尾部按释放时间（也就是事件时间 + span）吐出，事件时间单调。
    while pending:
        _, _, held = heapq.heappop(pending)
        yield held


def disorder_stats(events: Iterable[T], time_of) -> dict:
    """统计发出序列相对“已见最大事件时间”的回退。"""
    running_max = None
    inversions = 0
    max_lateness_ms = 0
    count = 0
    for event in events:
        count += 1
        ts = int(time_of(event))
        if running_max is not None and ts < running_max:
            inversions += 1
            max_lateness_ms = max(max_lateness_ms, running_max - ts)
        if running_max is None or ts > running_max:
            running_max = ts
    return {
        "events": count,
        "inversions": inversions,
        "max_lateness_ms": max_lateness_ms,
    }
