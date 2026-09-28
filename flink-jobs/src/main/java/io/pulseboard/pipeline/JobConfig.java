package io.pulseboard.pipeline;

import io.pulseboard.pipeline.model.DimItem;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** 作业参数。全部可序列化，因为过滤 run_id 的 lambda 会把它带进作业图。 */
public class JobConfig implements Serializable {
    public String kafkaBootstrap = "kafka:9092";
    public String topic = "clickstream.events";
    public String groupId = "pulseboard";
    public String runId = "baseline";
    public String clickhouseEndpoint = "http://clickhouse:8123";
    public String clickhouseDatabase = "pulseboard";
    public String dimPath = "/opt/data/dim_item.csv";
    public long windowMs = 600_000L;
    public long watermarkMs = 120_000L;
    public int idleSec = 20;
    public int topN = 10;
    public int parallelism = 2;
    public int batchSize = 1000;
    public long flushMs = 200L;
    public boolean forwardDuplicates = true;
    public String mode = "full";
    public String checkpointDir = "file:///opt/flink/checkpoints";
    public long checkpointIntervalMs = 10_000L;
    /** punctuated：每条事件发水位。periodic：按 watermarkIntervalMs 的墙钟周期发。 */
    public String watermarkMode = "punctuated";
    public long watermarkIntervalMs = 200L;
    /**
     * true 表示 Kafka Source 已经按分区生成水位。
     * 单测的内存源没有分区，保持 false，由 AnalyticsTopology 在校验之后再生成。
     */
    public boolean watermarksAssigned;
    /** 单测直接塞进作业图。生产路径在 TaskManager 的 open() 里读 dimPath，这里保持 null。 */
    public HashMap<Long, DimItem> inlineDim;

    public static JobConfig parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                continue;
            }
            String key = arg.substring(2);
            String value = "true";
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                value = args[++i];
            }
            values.put(key, value);
        }
        JobConfig cfg = new JobConfig();
        cfg.kafkaBootstrap = values.getOrDefault("kafka.bootstrap", cfg.kafkaBootstrap);
        cfg.topic = values.getOrDefault("kafka.topic", cfg.topic);
        cfg.groupId = values.getOrDefault("kafka.group", cfg.groupId);
        cfg.runId = values.getOrDefault("run.id", cfg.runId);
        cfg.clickhouseEndpoint = values.getOrDefault("clickhouse.endpoint", cfg.clickhouseEndpoint);
        cfg.clickhouseDatabase = values.getOrDefault("clickhouse.database", cfg.clickhouseDatabase);
        cfg.dimPath = values.getOrDefault("dim.path", cfg.dimPath);
        cfg.windowMs = Long.parseLong(values.getOrDefault("window.ms", Long.toString(cfg.windowMs)));
        cfg.watermarkMs = Long.parseLong(values.getOrDefault("watermark.ms", Long.toString(cfg.watermarkMs)));
        cfg.idleSec = Integer.parseInt(values.getOrDefault("idle.sec", Integer.toString(cfg.idleSec)));
        cfg.topN = Integer.parseInt(values.getOrDefault("topn", Integer.toString(cfg.topN)));
        cfg.parallelism = Integer.parseInt(values.getOrDefault("parallelism", Integer.toString(cfg.parallelism)));
        cfg.batchSize = Integer.parseInt(values.getOrDefault("batch.size", Integer.toString(cfg.batchSize)));
        cfg.flushMs = Long.parseLong(values.getOrDefault("flush.ms", Long.toString(cfg.flushMs)));
        cfg.forwardDuplicates = Boolean.parseBoolean(values.getOrDefault("forward.duplicates", "true"));
        cfg.mode = values.getOrDefault("mode", cfg.mode);
        cfg.checkpointDir = values.getOrDefault("checkpoint.dir", cfg.checkpointDir);
        cfg.checkpointIntervalMs =
                Long.parseLong(values.getOrDefault("checkpoint.ms", Long.toString(cfg.checkpointIntervalMs)));
        cfg.watermarkMode = values.getOrDefault("watermark.mode", cfg.watermarkMode);
        cfg.watermarkIntervalMs =
                Long.parseLong(values.getOrDefault("watermark.interval.ms", Long.toString(cfg.watermarkIntervalMs)));
        return cfg;
    }
}
