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
 * 每条事件都推进水位：watermark = maxEventTime - delay - 1。
 *
 * <p>减 1 是为了和 Flink 自带的 BoundedOutOfOrdernessWatermarks 对齐，
 * 窗口在 watermark >= windowEnd 时触发。
 * 回放会在几秒内跨过几小时的事件时间，如果还用默认 200ms 的周期水位，
 * 这 200ms 墙钟里可能已经滑过很多事件时间，乱序实验就解释不清了。
 * 生产上的高吞吐作业通常改回周期水位，用吞吐换一点水位精度。
 */
public class PunctuatedBoundedOutOfOrder implements WatermarkStrategy<Event>, Serializable {
    private final long delayMs;

    public PunctuatedBoundedOutOfOrder(long delayMs) {
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

    /** 调用方决定要不要叠加空闲分区检测。 */
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
            // 时间范围之外的脏数据不能把水位打飞。kicker 的时间戳在范围内，用来收尾。
            if (eventTimestamp < QualityRules.MIN_EVENT_TIME_MS || eventTimestamp > QualityRules.MAX_EVENT_TIME_MS) {
                return;
            }
            if (eventTimestamp > maxTs) {
                maxTs = eventTimestamp;
            }
            output.emitWatermark(new Watermark(maxTs - delayMs - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 水位在 onEvent 里已经发过。周期回调留空，避免和 punctuated 重复。
        }
    }
}
