package io.pulseboard.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.pulseboard.pipeline.agg.Partials.ItemPartial;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 和 batch/metrics.py 同一个次序：pv 降序，item_id 升序。 */
class TopNOrderTest {
    @Test
    void tieBreaksByItemId() {
        List<ItemPartial> items = new ArrayList<>();
        items.add(item(20, 5));
        items.add(item(10, 5));
        items.add(item(30, 9));
        items.sort(Comparator.comparingLong((ItemPartial item) -> item.pv).reversed()
                .thenComparingLong(item -> item.itemId));
        assertEquals(30L, items.get(0).itemId);
        assertEquals(10L, items.get(1).itemId);
        assertEquals(20L, items.get(2).itemId);
    }

    private static ItemPartial item(long id, long pv) {
        ItemPartial partial = new ItemPartial();
        partial.itemId = id;
        partial.pv = pv;
        return partial;
    }
}
