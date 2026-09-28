package io.pulseboard.pipeline.agg;

import io.pulseboard.pipeline.agg.Partials.ItemPartial;
import io.pulseboard.pipeline.agg.Partials.UserPartial;
import io.pulseboard.pipeline.model.Rows.FunnelRow;
import io.pulseboard.pipeline.model.Rows.PvUvRow;
import io.pulseboard.pipeline.model.Rows.TopItemRow;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 第二阶段按窗口汇总。
 *
 * <p>不用再套一个事件时间窗口：第一阶段输出的时间戳是 windowEnd-1，
 * 而触发它的水位已经 >= windowEnd，再开窗口会被当成迟到数据丢掉。
 * 这里注册 windowEnd 的事件时间定时器。水位是各并行度的最小值，
 * 定时器触发时，每个 subtask 的第一阶段结果都已经到齐。
 *
 * <p>allowedLateness 必须是 0。窗口如果二次触发，这里的累加会把同一用户再加一遍。
 * 迟到数据改走侧输出，不回刷主指标。
 */
public final class Assemblers {
    private Assemblers() {}

    public static final class PvUvAssembler extends KeyedProcessFunction<Long, UserPartial, PvUvRow> {
        private final String runId;
        private transient ValueState<long[]> acc;
        private transient ValueState<Boolean> emitted;

        public PvUvAssembler(String runId) {
            this.runId = runId;
        }

        @Override
        public void open(Configuration parameters) {
            // pv, uv, cart, buy, events, dimMiss, maxProduce, windowEnd
            acc = getRuntimeContext().getState(new ValueStateDescriptor<>("pvuv", long[].class));
            // 每个窗口只发一次。恢复后如果再发，累加器已经被清掉，新行只剩补进来的部分结果，
            // 更大的 checkpoint 编号会在 ReplacingMergeTree 里盖掉已经提交的完整行。
            emitted = getRuntimeContext().getState(new ValueStateDescriptor<>("pvuv-emitted", Boolean.class));
        }

        @Override
        public void processElement(UserPartial partial, Context ctx, Collector<PvUvRow> out) throws Exception {
            if (Boolean.TRUE.equals(emitted.value())) {
                return;
            }
            long[] slot = acc.value();
            if (slot == null) {
                slot = new long[8];
                slot[7] = partial.windowEndMs;
            }
            slot[0] += partial.pv;
            if (partial.pv > 0) {
                slot[1] += 1;
            }
            slot[2] += partial.cart;
            slot[3] += partial.buy;
            slot[4] += partial.events;
            slot[5] += partial.dimMiss;
            slot[6] = Math.max(slot[6], partial.maxProduceTimeMs);
            acc.update(slot);
            ctx.timerService().registerEventTimeTimer(partial.windowEndMs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PvUvRow> out) throws Exception {
            if (Boolean.TRUE.equals(emitted.value())) {
                return;
            }
            long[] slot = acc.value();
            if (slot == null) {
                return;
            }
            PvUvRow row = new PvUvRow();
            row.runId = runId;
            row.windowStartMs = ctx.getCurrentKey();
            row.windowEndMs = slot[7];
            row.pv = slot[0];
            row.uv = slot[1];
            row.cartCnt = slot[2];
            row.buyCnt = slot[3];
            row.events = slot[4];
            row.dimMiss = slot[5];
            row.maxProduceTimeMs = slot[6];
            out.collect(row);
            emitted.update(true);
            acc.clear();
        }
    }

    public static final class FunnelAssembler extends KeyedProcessFunction<Long, UserPartial, FunnelRow> {
        private final String runId;
        private transient ValueState<long[]> acc;
        private transient ValueState<Boolean> emitted;

        public FunnelAssembler(String runId) {
            this.runId = runId;
        }

        @Override
        public void open(Configuration parameters) {
            // pvUsers, cartUsers, buyUsers, pvToCart, cartToBuy, pvToBuy, windowEnd
            acc = getRuntimeContext().getState(new ValueStateDescriptor<>("funnel", long[].class));
            emitted = getRuntimeContext().getState(new ValueStateDescriptor<>("funnel-emitted", Boolean.class));
        }

        @Override
        public void processElement(UserPartial partial, Context ctx, Collector<FunnelRow> out) throws Exception {
            if (Boolean.TRUE.equals(emitted.value())) {
                return;
            }
            long[] slot = acc.value();
            if (slot == null) {
                slot = new long[7];
                slot[6] = partial.windowEndMs;
            }
            boolean pv = partial.minPvTs != Long.MAX_VALUE;
            boolean cart = partial.minCartTs != Long.MAX_VALUE;
            boolean buy = partial.minBuyTs != Long.MAX_VALUE;
            if (pv) {
                slot[0] += 1;
            }
            if (cart) {
                slot[1] += 1;
            }
            if (buy) {
                slot[2] += 1;
            }
            if (pv && cart && partial.minCartTs >= partial.minPvTs) {
                slot[3] += 1;
            }
            if (cart && buy && partial.minBuyTs >= partial.minCartTs) {
                slot[4] += 1;
            }
            if (pv && buy && partial.minBuyTs >= partial.minPvTs) {
                slot[5] += 1;
            }
            acc.update(slot);
            ctx.timerService().registerEventTimeTimer(partial.windowEndMs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<FunnelRow> out) throws Exception {
            if (Boolean.TRUE.equals(emitted.value())) {
                return;
            }
            long[] slot = acc.value();
            if (slot == null) {
                return;
            }
            FunnelRow row = new FunnelRow();
            row.runId = runId;
            row.windowStartMs = ctx.getCurrentKey();
            row.windowEndMs = slot[6];
            row.pvUsers = slot[0];
            row.cartUsers = slot[1];
            row.buyUsers = slot[2];
            row.pvToCartUsers = slot[3];
            row.cartToBuyUsers = slot[4];
            row.pvToBuyUsers = slot[5];
            out.collect(row);
            emitted.update(true);
            acc.clear();
        }
    }

    public static final class TopNAssembler extends KeyedProcessFunction<Long, ItemPartial, TopItemRow> {
        private final String runId;
        private final int topN;
        private transient ListState<ItemPartial> items;
        private transient ValueState<Long> windowEnd;
        private transient ValueState<Boolean> emitted;

        public TopNAssembler(String runId, int topN) {
            this.runId = runId;
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) {
            items = getRuntimeContext().getListState(new ListStateDescriptor<>("items", ItemPartial.class));
            windowEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("window-end", Long.class));
            emitted = getRuntimeContext().getState(new ValueStateDescriptor<>("topn-emitted", Boolean.class));
        }

        @Override
        public void processElement(ItemPartial partial, Context ctx, Collector<TopItemRow> out) throws Exception {
            if (Boolean.TRUE.equals(emitted.value())) {
                return;
            }
            items.add(partial);
            if (windowEnd.value() == null) {
                windowEnd.update(partial.windowEndMs);
            }
            ctx.timerService().registerEventTimeTimer(partial.windowEndMs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopItemRow> out) throws Exception {
            if (Boolean.TRUE.equals(emitted.value())) {
                return;
            }
            List<ItemPartial> all = new ArrayList<>();
            for (ItemPartial partial : items.get()) {
                all.add(partial);
            }
            all.sort(Comparator.comparingLong((ItemPartial item) -> item.pv).reversed()
                    .thenComparingLong(item -> item.itemId));
            int limit = Math.min(topN, all.size());
            Long end = windowEnd.value();
            for (int i = 0; i < limit; i++) {
                ItemPartial partial = all.get(i);
                TopItemRow row = new TopItemRow();
                row.runId = runId;
                row.windowStartMs = ctx.getCurrentKey();
                row.windowEndMs = end == null ? partial.windowEndMs : end;
                row.rank = i + 1;
                row.itemId = partial.itemId;
                row.categoryCode = partial.categoryCode;
                row.pv = partial.pv;
                out.collect(row);
            }
            items.clear();
            windowEnd.clear();
            emitted.update(true);
        }
    }
}
