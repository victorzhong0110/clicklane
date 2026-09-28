package io.pulseboard.pipeline.watermark;

import io.pulseboard.pipeline.dq.QualityRules;
import io.pulseboard.pipeline.model.Event;
import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * 周期水位：onEvent 只记住最大事件时间，onPeriodicEmit 才发出水位。
 *
 * <p>公式仍是 watermark = maxEventTime - delay - 1。
 * 和 {@link PunctuatedBoundedOutOfOrder} 的差别只在“多久发一次”。
 * 高吞吐时每条事件都发水位会变成额外开销；周期水位用墙钟间隔换一点精度。
 * 间隔由 StreamExecutionEnvironment 的 autoWatermarkInterval 决定，默认 200 ms。
 * 加速回放时，这 200 ms 墙钟可能跨过不少事件时间，迟到会比 punctuated 更多。
 */
public class PeriodicBoundedOutOfOrder implements WatermarkStrategy<Event>, Serializable {
    private final long delayMs;

    public PeriodicBoundedOutOfOrder(long delayMs) {
        this.delayMs = delayMs;
    }

    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new Generator(delayMs);
    }

    @Override
    public org.apache.flink.api.common.eventtime.TimestampAssigner<Event> createTimestampAssigner(
            org.apache.flink.api.common.eventtime.TimestampAssignerSupplier.Context context) {
        return (event, recordTimestamp) -> event.eventTimeMs;
    }

    public WatermarkStrategy<Event> withIdle(Duration idle) {
        if (idle == null || idle.isZero() || idle.isNegative()) {
            return this;
        }
        return this.withIdleness(idle);
    }

    private static final class Generator implements WatermarkGenerator<Event> {
        private final long delayMs;
        private long maxTs = Long.MIN_VALUE;

        private Generator(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp < QualityRules.MIN_EVENT_TIME_MS || eventTimestamp > QualityRules.MAX_EVENT_TIME_MS) {
                return;
            }
            if (eventTimestamp > maxTs) {
                maxTs = eventTimestamp;
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (maxTs == Long.MIN_VALUE) {
                return;
            }
            output.emitWatermark(new Watermark(maxTs - delayMs - 1));
        }
    }
}
