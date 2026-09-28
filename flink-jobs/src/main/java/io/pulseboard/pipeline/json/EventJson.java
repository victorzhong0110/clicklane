package io.pulseboard.pipeline.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pulseboard.pipeline.model.Event;
import java.nio.charset.StandardCharsets;

/** Kafka 消息是一行扁平 JSON。解析失败不抛异常，交给质量算子丢到侧输出。 */
public final class EventJson {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EventJson() {}

    public static Event parse(byte[] message) {
        if (message == null || message.length == 0) {
            return bad("empty", message);
        }
        try {
            Event event = MAPPER.readValue(message, Event.class);
            if (event.eventId == null) {
                event.eventId = "";
            }
            if (event.runId == null) {
                event.runId = "";
            }
            if (event.behavior == null) {
                event.behavior = "";
            }
            return event;
        } catch (Exception ex) {
            return bad("unparseable", message);
        }
    }

    /**
     * 解析失败的记录没有可靠的 event_id。只用原因字符串做 id 时，
     * 不同的坏报文会在 ReplacingMergeTree 里被收成同一行。
     * 这里把报文字节和原因一起哈希，让内容不同的坏报文各自占一行。
     * 完全相同的空报文仍然会撞成同一个 id。
     */
    private static Event bad(String reason, byte[] message) {
        Event event = new Event();
        event.parseError = true;
        event.rejectReason = reason;
        event.eventId = "bad-" + Integer.toHexString(fingerprint(reason, message));
        event.behavior = "";
        return event;
    }

    private static int fingerprint(String reason, byte[] message) {
        int hash = reason.hashCode();
        if (message == null) {
            return hash;
        }
        for (byte value : message) {
            hash = 31 * hash + value;
        }
        return hash;
    }

    /** 测试和调试用，作业本身不把 Event 再写回 Kafka。 */
    public static byte[] bytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
