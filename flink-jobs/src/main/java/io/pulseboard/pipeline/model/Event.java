package io.pulseboard.pipeline.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 一条点击流事件。
 *
 * <p>Kafka 里只有行为字段。categoryId / categoryCode / dimMiss 由维表 Join 在作业里补上，
 * 所以 JSON 里没有这三列。事实表不携带类目。
 * 字段保持 public，是为了让 Flink 把这个类识别成 POJO。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event {
    @JsonProperty("event_id")
    public String eventId = "";

    @JsonProperty("run_id")
    public String runId = "";

    @JsonProperty("user_id")
    public long userId;

    @JsonProperty("item_id")
    public long itemId;

    @JsonProperty("behavior")
    public String behavior = "";

    @JsonProperty("event_time_ms")
    public long eventTimeMs;

    @JsonProperty("produce_time_ms")
    public long produceTimeMs;

    /** 只用来把水位往前推，不进入任何指标。 */
    @JsonProperty("watermark_kick")
    public boolean watermarkKick;

    @JsonIgnore
    public boolean parseError;

    @JsonIgnore
    public String rejectReason = "";

    @JsonIgnore
    public long categoryId = -1L;

    @JsonIgnore
    public String categoryCode = "";

    @JsonIgnore
    public boolean dimMiss;

    public Event() {}

    public boolean isKick() {
        return watermarkKick;
    }

    /**
     * 同一用户、同一商品、同一行为、同一事件时间视为重复。
     * 这份公开数据的时间戳只有 1 秒精度，所以同一秒里的两次点击会撞上这个键。
     */
    public String naturalKey() {
        return userId + "|" + itemId + "|" + behavior + "|" + eventTimeMs;
    }
}
