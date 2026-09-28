package io.pulseboard.pipeline.dq;

import io.pulseboard.pipeline.model.Event;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 非法事件进侧输出，合法事件继续往下走。
 *
 * <p>生产作业的水位在 Kafka Source 上按分区生成，生成器会忽略时间范围之外的时间戳。
 * 单测没有分区，水位在本算子之后、由拓扑再生成一次。
 */
public class ValidateFunction extends ProcessFunction<Event, Event> {
    public static final OutputTag<Event> INVALID = new OutputTag<Event>("invalid-events") {};

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) {
        // kicker 必须和真实事件留在同一条流里，拆开再 union 会打乱顺序，
        // 水位可能先被 kicker 推到末尾，导致前面的事件全部变成迟到。
        if (event.isKick()) {
            out.collect(event);
            return;
        }
        String reason = QualityRules.invalidReason(event);
        if (reason != null) {
            event.rejectReason = reason;
            ctx.output(INVALID, event);
            return;
        }
        out.collect(event);
    }
}
