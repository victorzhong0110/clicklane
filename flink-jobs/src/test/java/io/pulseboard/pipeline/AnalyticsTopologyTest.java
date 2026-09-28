package io.pulseboard.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.pulseboard.pipeline.model.DimItem;
import io.pulseboard.pipeline.model.Event;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

/**
 * 用内存源把水位、迟到侧输出、去重标记、维表 miss 和 Top-N 一次走通。
 * 窗口 60 秒，水位允许 5 秒乱序。t=70000 的事件会把水位推过第一扇窗口，
 * 排在它后面、时间戳却落在第一扇窗口里的 pv 必须进迟到侧输出，不能进 PV。
 */
class AnalyticsTopologyTest {
    /** 2019-11-01 00:00:00 UTC，能被 60 秒整除，也落在质量规则的时间范围内。 */
    private static final long BASE = 1_572_566_400_000L;

    @Test
    void windowsLateDataAndDimensionMiss() throws Exception {
        MemorySink.reset();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        JobConfig cfg = new JobConfig();
        cfg.runId = "test";
        cfg.windowMs = 60_000L;
        cfg.watermarkMs = 5_000L;
        cfg.idleSec = 0;
        cfg.topN = 10;
        cfg.forwardDuplicates = true;
        cfg.mode = "full";
        cfg.inlineDim = new HashMap<>();
        cfg.inlineDim.put(10L, new DimItem(10, 1, "phone"));
        cfg.inlineDim.put(11L, new DimItem(11, 2, "washer"));
        cfg.inlineDim.put(12L, new DimItem(12, 3, "shirt"));

        List<Event> input = List.of(
                event("1", 1, 10, "pv", BASE + 1_000, 100, false),
                event("2", 1, 10, "pv", BASE + 1_000, 101, false),
                event("3", 2, 10, "pv", BASE + 2_000, 102, false),
                event("4", 1, 11, "cart", BASE + 10_000, 103, false),
                event("5", 3, 11, "buy", BASE + 20_000, 104, false),
                event("6", 4, 99, "pv", BASE + 4_000, 105, false),
                event("bad", 0, 10, "pv", BASE + 1_500, 106, false),
                event("8", 2, 12, "pv", BASE + 70_000, 107, false),
                event("9", 1, 10, "pv", BASE + 3_000, 108, false),
                event("10", 2, 12, "pv", BASE + 130_000, 109, false),
                kick());

        var parsed = env.fromCollection(input, TypeInformation.of(Event.class));
        AnalyticsTopology.Built built = AnalyticsTopology.build(parsed, cfg);
        built.events.addSink(new MemorySink<>("events"));
        built.pvUv.addSink(new MemorySink<>("pv"));
        built.funnel.addSink(new MemorySink<>("funnel"));
        built.topItems.addSink(new MemorySink<>("top"));
        built.categoryPv.addSink(new MemorySink<>("category"));
        built.late.addSink(new MemorySink<>("late"));
        built.invalid.addSink(new MemorySink<>("invalid"));
        built.duplicates.addSink(new MemorySink<>("dup"));
        env.execute("pulseboard-topology-test");

        Map<String, String> first = pv(BASE);
        assertEquals("4", first.get("pv"));
        assertEquals("3", first.get("uv"));
        assertEquals("1", first.get("cartCnt"));
        assertEquals("1", first.get("buyCnt"));
        assertEquals("6", first.get("events"));
        assertEquals("1", first.get("dimMiss"));
        assertEquals("105", first.get("maxProduceTimeMs"));

        Map<String, String> second = pv(BASE + 60_000L);
        assertEquals("1", second.get("pv"));
        assertEquals("1", second.get("uv"));
        assertEquals("1", pv(BASE + 120_000L).get("pv"));

        Map<String, String> funnel = funnel(BASE);
        assertEquals("3", funnel.get("pvUsers"));
        assertEquals("1", funnel.get("cartUsers"));
        assertEquals("1", funnel.get("buyUsers"));
        assertEquals("1", funnel.get("pvToCartUsers"));
        assertEquals("0", funnel.get("cartToBuyUsers"));
        assertEquals("0", funnel.get("pvToBuyUsers"));

        Map<String, String> rank1 = top(BASE, "1");
        Map<String, String> rank2 = top(BASE, "2");
        assertEquals("10", rank1.get("itemId"));
        assertEquals("3", rank1.get("pv"));
        assertEquals("phone", rank1.get("categoryCode"));
        assertEquals("99", rank2.get("itemId"));
        assertEquals("UNKNOWN", rank2.get("categoryCode"));

        List<Map<String, String>> late = MemorySink.read("late");
        assertEquals(1, late.size(), "late=" + late + " events=" + MemorySink.read("events"));
        assertEquals("9", late.get(0).get("eventId"));

        List<Map<String, String>> invalid = MemorySink.read("invalid");
        assertEquals(1, invalid.size());
        assertEquals("bad_user", invalid.get(0).get("rejectReason"));

        List<Map<String, String>> dups = MemorySink.read("dup");
        assertEquals(1, dups.size());
        assertEquals("2", dups.get(0).get("eventId"));

        long categoryPhone = MemorySink.read("category").stream()
                .filter(row -> String.valueOf(BASE).equals(row.get("windowStartMs")))
                .filter(row -> "phone".equals(row.get("categoryCode")))
                .mapToLong(row -> Long.parseLong(row.get("pv")))
                .sum();
        assertEquals(3L, categoryPhone);
        assertTrue(late.stream().noneMatch(row -> "true".equals(row.get("watermarkKick"))));
    }

    private static Map<String, String> pv(long start) throws Exception {
        return MemorySink.read("pv").stream()
                .filter(row -> String.valueOf(start).equals(row.get("windowStartMs")))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "missing pv window " + start + " pv=" + safe("pv") + " events=" + safe("events")));
    }

    private static Map<String, String> funnel(long start) throws Exception {
        return MemorySink.read("funnel").stream()
                .filter(row -> String.valueOf(start).equals(row.get("windowStartMs")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing funnel " + safe("funnel")));
    }

    private static Map<String, String> top(long start, String rank) throws Exception {
        return MemorySink.read("top").stream()
                .filter(row -> String.valueOf(start).equals(row.get("windowStartMs")))
                .filter(row -> rank.equals(row.get("rank")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing top " + safe("top")));
    }

    private static String safe(String name) {
        try {
            return MemorySink.read(name).toString();
        } catch (Exception ex) {
            return ex.toString();
        }
    }

    private static Event event(
            String id, long user, long item, String behavior, long eventTime, long produceTime, boolean kick) {
        Event event = new Event();
        event.eventId = id;
        event.runId = "test";
        event.userId = user;
        event.itemId = item;
        event.behavior = behavior;
        event.eventTimeMs = eventTime;
        event.produceTimeMs = produceTime;
        event.watermarkKick = kick;
        return event;
    }

    private static Event kick() {
        return event("kick", 0, 0, "kick", BASE + 200_000, 200, true);
    }
}
