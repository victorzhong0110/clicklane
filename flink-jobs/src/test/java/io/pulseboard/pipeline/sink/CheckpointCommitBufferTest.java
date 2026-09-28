package io.pulseboard.pipeline.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import io.pulseboard.pipeline.sink.ClickHouseSink;

class CheckpointCommitBufferTest {
    @Test
    void snapshotMovesRowsOutOfTheOpenBuffer() {
        CheckpointCommitBuffer<String> buffer = new CheckpointCommitBuffer<>();
        buffer.add("a");
        buffer.add("b");
        List<String> frozen = buffer.snapshot(3);
        assertEquals(List.of("a", "b"), frozen);
        assertEquals(0, buffer.openCount());
        assertTrue(buffer.hasPending(3));
    }

    @Test
    void completeThroughReleasesOnlyFinishedCheckpointsInOrder() {
        CheckpointCommitBuffer<String> buffer = new CheckpointCommitBuffer<>();
        buffer.add("a");
        buffer.snapshot(1);
        buffer.add("b");
        buffer.snapshot(2);
        buffer.add("c");
        buffer.snapshot(4);

        List<CheckpointCommitBuffer.PendingBatch<String>> first = buffer.completeThrough(2);
        assertEquals(2, first.size());
        assertEquals(1L, first.get(0).checkpointId);
        assertEquals(List.of("a"), first.get(0).rows);
        assertEquals(2L, first.get(1).checkpointId);
        assertEquals(List.of("b"), first.get(1).rows);
        assertFalse(buffer.hasPending(1));
        assertTrue(buffer.hasPending(4));

        List<CheckpointCommitBuffer.PendingBatch<String>> rest = buffer.completeThrough(4);
        assertEquals(1, rest.size());
        assertEquals(List.of("c"), rest.get(0).rows);
    }

    @Test
    void abortPutsTheBatchBackInFrontOfNewerRows() {
        CheckpointCommitBuffer<String> buffer = new CheckpointCommitBuffer<>();
        buffer.add("a");
        buffer.add("b");
        buffer.snapshot(7);
        buffer.add("c");
        buffer.abort(7);
        assertFalse(buffer.hasPending(7));
        List<String> retried = buffer.snapshot(8);
        assertEquals(List.of("a", "b", "c"), retried);
    }

    @Test
    void stagedRowKeepsTheCheckpointIdAsTheVersion() {
        String staged = ClickHouseSink.encodeStaged(11, "{\"version\":11}");
        assertEquals("{\"version\":11}", ClickHouseSink.jsonOf(staged));
        assertTrue(staged.startsWith("11"));
    }

    @Test
    void abortOfUnknownCheckpointLeavesTheOpenBufferAlone() {
        CheckpointCommitBuffer<String> buffer = new CheckpointCommitBuffer<>();
        buffer.add("a");
        buffer.abort(1);
        assertEquals(List.of("a"), buffer.snapshot(2));
    }
}
