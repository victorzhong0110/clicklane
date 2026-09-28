package io.pulseboard.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.pulseboard.pipeline.json.EventJson;
import io.pulseboard.pipeline.model.Event;
import org.junit.jupiter.api.Test;

class EventJsonTest {
    @Test
    void parsesTheReplayerPayload() {
        String json = "{"
                + "\"event_id\":\"12\","
                + "\"run_id\":\"baseline\","
                + "\"user_id\":7,"
                + "\"item_id\":9,"
                + "\"behavior\":\"pv\","
                + "\"event_time_ms\":1572566400000,"
                + "\"produce_time_ms\":1700000000000,"
                + "\"watermark_kick\":false"
                + "}";
        Event event = EventJson.parse(EventJson.bytes(json));
        assertEquals("12", event.eventId);
        assertEquals("baseline", event.runId);
        assertEquals(7L, event.userId);
        assertEquals("pv", event.behavior);
        assertEquals(1572566400000L, event.eventTimeMs);
        assertFalse(event.isKick());
    }

    @Test
    void brokenJsonBecomesAParseError() {
        Event event = EventJson.parse(EventJson.bytes("{not-json"));
        assertTrue(event.parseError);
    }

    @Test
    void differentBrokenPayloadsGetDifferentIds() {
        Event first = EventJson.parse(EventJson.bytes("{not-json"));
        Event second = EventJson.parse(EventJson.bytes("{also-not"));
        assertTrue(first.parseError);
        assertTrue(second.parseError);
        assertFalse(first.eventId.isEmpty());
        assertFalse(first.eventId.equals(second.eventId));
    }
}
