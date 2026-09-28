package io.pulseboard.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.pulseboard.pipeline.dq.QualityRules;
import io.pulseboard.pipeline.model.Event;
import org.junit.jupiter.api.Test;

class QualityRulesTest {
    @Test
    void acceptsANormalView() {
        Event event = base();
        assertNull(QualityRules.invalidReason(event));
    }

    @Test
    void rejectsBadUserBehaviorAndTime() {
        Event event = base();
        event.userId = 0;
        assertEquals("bad_user", QualityRules.invalidReason(event));
        event = base();
        event.behavior = "hover";
        assertEquals("bad_behavior", QualityRules.invalidReason(event));
        event = base();
        event.eventTimeMs = 1L;
        assertEquals("bad_time", QualityRules.invalidReason(event));
    }

    private static Event base() {
        Event event = new Event();
        event.eventId = "1";
        event.runId = "t";
        event.userId = 5;
        event.itemId = 9;
        event.behavior = "pv";
        event.eventTimeMs = 1572566400000L;
        return event;
    }
}
