package io.pulseboard.pipeline.dq;

import io.pulseboard.pipeline.model.Event;
import java.util.Set;

/**
 * 无状态质量规则。时间范围同时覆盖天池 UserBehavior（2017-11）和本项目使用的
 * REES46 样本（2019-11），避免换数据集时要改代码。
 */
public final class QualityRules {
    public static final long MIN_EVENT_TIME_MS = 1483228800000L; // 2017-01-01 UTC
    public static final long MAX_EVENT_TIME_MS = 1609459200000L; // 2021-01-01 UTC

    private static final Set<String> BEHAVIORS = Set.of("pv", "cart", "buy", "fav", "remove");

    private QualityRules() {}

    /** @return null 表示通过，否则是拒绝原因 */
    public static String invalidReason(Event event) {
        if (event == null || event.parseError) {
            return event == null || event.rejectReason.isEmpty() ? "parse_error" : event.rejectReason;
        }
        if (event.eventId == null || event.eventId.isEmpty()) {
            return "missing_event_id";
        }
        if (event.runId == null || event.runId.isEmpty()) {
            return "missing_run_id";
        }
        if (event.userId <= 0) {
            return "bad_user";
        }
        if (event.itemId <= 0) {
            return "bad_item";
        }
        if (event.behavior == null || !BEHAVIORS.contains(event.behavior)) {
            return "bad_behavior";
        }
        if (event.eventTimeMs < MIN_EVENT_TIME_MS || event.eventTimeMs > MAX_EVENT_TIME_MS) {
            return "bad_time";
        }
        return null;
    }
}
