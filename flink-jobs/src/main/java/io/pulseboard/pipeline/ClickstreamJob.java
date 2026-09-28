package io.pulseboard.pipeline;

import io.pulseboard.pipeline.json.EventDeserializer;
import io.pulseboard.pipeline.model.Event;
import io.pulseboard.pipeline.model.Rows.CategoryPvRow;
import io.pulseboard.pipeline.model.Rows.FunnelRow;
import io.pulseboard.pipeline.model.Rows.LateRow;
import io.pulseboard.pipeline.model.Rows.OdsRow;
import io.pulseboard.pipeline.model.Rows.PvUvRow;
import io.pulseboard.pipeline.model.Rows.RejectRow;
import io.pulseboard.pipeline.model.Rows.TopItemRow;
import io.pulseboard.pipeline.sink.ClickHouseSink;
import io.pulseboard.pipeline.watermark.PeriodicBoundedOutOfOrder;
import io.pulseboard.pipeline.watermark.PunctuatedBoundedOutOfOrder;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 点击流作业入口。
 *
 * <p>full：质量校验 + 维表 + PV/UV + Top-N + 漏斗 + 类目 PV + 迟到侧输出，全部幂等写入 ClickHouse。
 * quality：只跑校验和去重，用来单独讲数据质量作业。演示和基准测试用 full，
 * 这样杀掉 TaskManager 时只有一个作业需要从 checkpoint 恢复。
 */
public final class ClickstreamJob {
    private ClickstreamJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.parse(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(cfg.parallelism);
        env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(cfg.checkpointDir);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
                                .RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        // 提交在 checkpoint 完成之后。积压时一次要写好几秒，60 秒会让下一次 barrier 超时。
        env.getCheckpointConfig().setCheckpointTimeout(180_000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(30, Time.seconds(5)));

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(cfg.kafkaBootstrap)
                .setTopics(cfg.topic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();
        // 水位挂在 Source 上：Kafka Source 会按分区各自推进，再取最小值。
        // 如果等事件汇合到下游再生成，同一个 subtask 上较快的分区会把较慢分区的事件打成迟到。
        // periodic 时必须把自动水位间隔设上，否则 onPeriodicEmit 不会按预期被调用。
        if ("periodic".equalsIgnoreCase(cfg.watermarkMode)) {
            env.getConfig().setAutoWatermarkInterval(cfg.watermarkIntervalMs);
        }
        WatermarkStrategy<Event> watermarks = watermarkStrategy(cfg);
        cfg.watermarksAssigned = true;
        DataStream<Event> parsed = env.fromSource(source, watermarks, "kafka-" + cfg.topic);

        AnalyticsTopology.Built built = AnalyticsTopology.build(parsed, cfg);
        attach(cfg, built);
        System.out.println("PulseBoard submit run=" + cfg.runId
                + " topic=" + cfg.topic
                + " windowMs=" + cfg.windowMs
                + " watermarkMs=" + cfg.watermarkMs
                + " watermarkMode=" + cfg.watermarkMode
                + " mode=" + cfg.mode);
        env.execute("pulseboard-" + cfg.runId);
    }

    static WatermarkStrategy<Event> watermarkStrategy(JobConfig cfg) {
        Duration idle = Duration.ofSeconds(cfg.idleSec);
        if ("periodic".equalsIgnoreCase(cfg.watermarkMode)) {
            return new PeriodicBoundedOutOfOrder(cfg.watermarkMs).withIdle(idle);
        }
        return new PunctuatedBoundedOutOfOrder(cfg.watermarkMs).withIdle(idle);
    }

    static void attach(JobConfig cfg, AnalyticsTopology.Built built) {
        built.invalid
                .map(ClickstreamJob::invalidRow)
                .addSink(sink(cfg, "dwd_invalid_events", RejectRow::toJson, false))
                .name("sink-invalid")
                .disableChaining();
        built.duplicates
                .map(ClickstreamJob::duplicateRow)
                .addSink(sink(cfg, "dwd_duplicate_events", RejectRow::toJson, false))
                .name("sink-duplicate")
                .disableChaining();
        if (!"full".equals(cfg.mode)) {
            return;
        }
        // Sink 单独成链，checkpoint barrier 在上游状态和写出缓冲之间对齐。
        built.events.map(ClickstreamJob::odsRow).addSink(sink(cfg, "ods_events", OdsRow::toJson, false)).name("sink-ods").disableChaining();
        built.pvUv.addSink(sink(cfg, "ads_pv_uv", PvUvRow::toJson, true)).name("sink-pv-uv").disableChaining();
        built.funnel.addSink(sink(cfg, "ads_funnel", FunnelRow::toJson, true)).name("sink-funnel").disableChaining();
        built.topItems.addSink(sink(cfg, "ads_top_items", TopItemRow::toJson, true)).name("sink-topn").disableChaining();
        built.categoryPv.addSink(sink(cfg, "ads_category_pv", CategoryPvRow::toJson, true)).name("sink-category").disableChaining();
        built.late
                .map(event -> lateRow(event, cfg.windowMs))
                .addSink(sink(cfg, "dwd_late_events", LateRow::toJson, false))
                .name("sink-late")
                .disableChaining();
    }

    private static <T> ClickHouseSink<T> sink(
            JobConfig cfg, String table, ClickHouseSink.Encoder<T> encoder, boolean transactional) {
        return new ClickHouseSink<>(
                cfg.clickhouseEndpoint,
                cfg.clickhouseDatabase,
                table,
                encoder,
                cfg.batchSize,
                cfg.flushMs,
                transactional);
    }

    static OdsRow odsRow(Event event) {
        OdsRow row = new OdsRow();
        row.runId = event.runId;
        row.eventId = event.eventId;
        row.userId = event.userId;
        row.itemId = event.itemId;
        row.behavior = event.behavior;
        row.categoryId = event.categoryId;
        row.categoryCode = event.categoryCode;
        row.eventTimeMs = event.eventTimeMs;
        row.produceTimeMs = event.produceTimeMs;
        row.dimMiss = event.dimMiss ? 1 : 0;
        return row;
    }

    static LateRow lateRow(Event event, long windowMs) {
        LateRow row = new LateRow();
        row.runId = event.runId;
        row.eventId = event.eventId;
        row.userId = event.userId;
        row.itemId = event.itemId;
        row.behavior = event.behavior;
        row.eventTimeMs = event.eventTimeMs;
        long mod = Math.floorMod(event.eventTimeMs, windowMs);
        row.windowStartMs = event.eventTimeMs - mod;
        row.reason = "window_closed";
        return row;
    }

    static RejectRow invalidRow(Event event) {
        RejectRow row = reject(event);
        row.reason = event.rejectReason == null || event.rejectReason.isEmpty() ? "invalid" : event.rejectReason;
        return row;
    }

    static RejectRow duplicateRow(Event event) {
        RejectRow row = reject(event);
        row.reason = "duplicate";
        return row;
    }

    private static RejectRow reject(Event event) {
        RejectRow row = new RejectRow();
        row.runId = event.runId == null ? "" : event.runId;
        row.eventId = event.eventId == null ? "" : event.eventId;
        row.userId = event.userId;
        row.itemId = event.itemId;
        row.behavior = event.behavior == null ? "" : event.behavior;
        row.eventTimeMs = event.eventTimeMs;
        return row;
    }
}
