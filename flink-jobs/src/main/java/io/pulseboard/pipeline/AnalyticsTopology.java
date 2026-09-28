package io.pulseboard.pipeline;

import io.pulseboard.pipeline.agg.Aggregates;
import io.pulseboard.pipeline.agg.Assemblers;
import io.pulseboard.pipeline.agg.Partials.ItemPartial;
import io.pulseboard.pipeline.agg.Partials.UserPartial;
import io.pulseboard.pipeline.dim.ItemCategoryDimJoin;
import io.pulseboard.pipeline.dq.DedupFunction;
import io.pulseboard.pipeline.dq.ValidateFunction;
import io.pulseboard.pipeline.model.Event;
import io.pulseboard.pipeline.model.Rows.CategoryPvRow;
import io.pulseboard.pipeline.model.Rows.FunnelRow;
import io.pulseboard.pipeline.model.Rows.PvUvRow;
import io.pulseboard.pipeline.model.Rows.TopItemRow;
import io.pulseboard.pipeline.watermark.PunctuatedBoundedOutOfOrder;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 从“已经解析好的事件”接到各条指标流。
 *
 * <p>顺序是有意排的：先挡掉非法时间戳，再生成水位，再去重，再补类目，最后开窗。
 * 迟到侧输出只挂在“按用户开窗”这一条上，因为所有行为都会进这个窗口，
 * 一条迟到事件不会在 PV、漏斗、Top-N 三个窗口里被重复记三次。
 */
public final class AnalyticsTopology {
    public static final OutputTag<Event> LATE = new OutputTag<Event>("late-events") {};

    private AnalyticsTopology() {}

    public static Built build(DataStream<Event> parsed, JobConfig cfg) {
        DataStream<Event> scoped = parsed
                .filter(event -> {
                    // 坏 JSON 读不出 run_id。作业只消费本次 topic，把这类记录归到当前 run，
                    // 否则质量表里的 run_id 是空的，按 run 对不上注入的畸形记录。
                    if (event.parseError && (event.runId == null || event.runId.isEmpty())) {
                        event.runId = cfg.runId;
                    }
                    return event.isKick() || event.parseError || cfg.runId.equals(event.runId);
                })
                .name("scope-run");

        SingleOutputStreamOperator<Event> valid =
                scoped.process(new ValidateFunction()).name("validate");
        DataStream<Event> invalid = valid.getSideOutput(ValidateFunction.INVALID);

        // 生产路径的水位已经在 Kafka Source 上按分区生成，这里不能再生成一次，
        // 否则又退回“汇合后取最大时间戳”。内存源的单测没有分区，仍在校验之后生成。
        // 两种路径都不把 kicker 拆到另一条流再 union：union 不保证和原来的先后一致。
        DataStream<Event> timed;
        if (cfg.watermarksAssigned) {
            timed = valid;
        } else {
            WatermarkStrategy<Event> watermarks = new PunctuatedBoundedOutOfOrder(cfg.watermarkMs)
                    .withIdle(Duration.ofSeconds(cfg.idleSec));
            timed = valid.assignTimestampsAndWatermarks(watermarks).name("event-time");
        }

        SingleOutputStreamOperator<Event> deduped = timed.keyBy(Event::naturalKey)
                .process(new DedupFunction(cfg.forwardDuplicates))
                .name("dedup");
        DataStream<Event> duplicates = deduped.getSideOutput(DedupFunction.DUPLICATES);

        DataStream<Event> joined = deduped
                .map(cfg.inlineDim != null ? new ItemCategoryDimJoin(cfg.inlineDim) : new ItemCategoryDimJoin(cfg.dimPath))
                .name("item-category-join");

        DataStream<Event> events = joined.filter(event -> !event.isKick()).name("drop-kicks");

        Built built = new Built();
        built.events = events;
        built.invalid = invalid;
        built.duplicates = duplicates;
        if (!"full".equals(cfg.mode)) {
            return built;
        }

        SingleOutputStreamOperator<UserPartial> users = events.keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(cfg.windowMs)))
                .sideOutputLateData(LATE)
                .aggregate(new Aggregates.UserWindowAgg(), new Aggregates.UserWindowEmit())
                .name("user-window");
        built.late = users.getSideOutput(LATE);
        built.pvUv = users.keyBy(partial -> partial.windowStartMs)
                .process(new Assemblers.PvUvAssembler(cfg.runId))
                .name("pv-uv");
        built.funnel = users.keyBy(partial -> partial.windowStartMs)
                .process(new Assemblers.FunnelAssembler(cfg.runId))
                .name("funnel");

        DataStream<ItemPartial> items = events.filter(event -> "pv".equals(event.behavior))
                .keyBy(event -> event.itemId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(cfg.windowMs)))
                .aggregate(new Aggregates.ItemWindowAgg(), new Aggregates.ItemWindowEmit())
                .name("item-window");
        built.topItems = items.keyBy(partial -> partial.windowStartMs)
                .process(new Assemblers.TopNAssembler(cfg.runId, cfg.topN))
                .name("top-n");

        built.categoryPv = events.filter(event -> "pv".equals(event.behavior))
                .keyBy(event -> event.categoryCode == null || event.categoryCode.isEmpty()
                        ? "UNKNOWN"
                        : event.categoryCode)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(cfg.windowMs)))
                .aggregate(new Aggregates.CategoryWindowAgg(), new Aggregates.CategoryWindowEmit(cfg.runId))
                .name("category-pv");
        return built;
    }

    /** 各条输出。quality 模式下窗口相关字段保持 null。 */
    public static final class Built {
        public DataStream<Event> events;
        public DataStream<Event> invalid;
        public DataStream<Event> duplicates;
        public DataStream<Event> late;
        public DataStream<PvUvRow> pvUv;
        public DataStream<FunnelRow> funnel;
        public DataStream<TopItemRow> topItems;
        public DataStream<CategoryPvRow> categoryPv;
    }
}
