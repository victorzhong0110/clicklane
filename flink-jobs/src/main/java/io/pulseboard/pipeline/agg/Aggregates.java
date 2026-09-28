package io.pulseboard.pipeline.agg;

import io.pulseboard.pipeline.agg.Partials.CategoryPartial;
import io.pulseboard.pipeline.agg.Partials.ItemPartial;
import io.pulseboard.pipeline.agg.Partials.UserPartial;
import io.pulseboard.pipeline.model.Event;
import io.pulseboard.pipeline.model.Rows.CategoryPvRow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 第一阶段：按用户 / 商品 / 类目在事件时间窗口里增量聚合。累加器在 getResult 时复制，避免被下一条事件改掉。 */
public final class Aggregates {
    private Aggregates() {}

    public static final class UserWindowAgg implements AggregateFunction<Event, UserPartial, UserPartial> {
        @Override
        public UserPartial createAccumulator() {
            return new UserPartial();
        }

        @Override
        public UserPartial add(Event event, UserPartial acc) {
            acc.userId = event.userId;
            acc.events += 1;
            if (event.dimMiss) {
                acc.dimMiss += 1;
            }
            if (event.produceTimeMs > acc.maxProduceTimeMs) {
                acc.maxProduceTimeMs = event.produceTimeMs;
            }
            switch (event.behavior) {
                case "pv":
                    acc.pv += 1;
                    acc.minPvTs = Math.min(acc.minPvTs, event.eventTimeMs);
                    break;
                case "cart":
                    acc.cart += 1;
                    acc.minCartTs = Math.min(acc.minCartTs, event.eventTimeMs);
                    break;
                case "buy":
                    acc.buy += 1;
                    acc.minBuyTs = Math.min(acc.minBuyTs, event.eventTimeMs);
                    break;
                default:
                    break;
            }
            return acc;
        }

        @Override
        public UserPartial getResult(UserPartial acc) {
            return acc.copy();
        }

        @Override
        public UserPartial merge(UserPartial left, UserPartial right) {
            UserPartial merged = left.copy();
            merged.pv += right.pv;
            merged.cart += right.cart;
            merged.buy += right.buy;
            merged.events += right.events;
            merged.dimMiss += right.dimMiss;
            merged.maxProduceTimeMs = Math.max(left.maxProduceTimeMs, right.maxProduceTimeMs);
            merged.minPvTs = Math.min(left.minPvTs, right.minPvTs);
            merged.minCartTs = Math.min(left.minCartTs, right.minCartTs);
            merged.minBuyTs = Math.min(left.minBuyTs, right.minBuyTs);
            if (merged.userId == 0) {
                merged.userId = right.userId;
            }
            return merged;
        }
    }

    public static final class UserWindowEmit extends ProcessWindowFunction<UserPartial, UserPartial, Long, TimeWindow> {
        @Override
        public void process(Long userId, Context ctx, Iterable<UserPartial> elements, Collector<UserPartial> out) {
            UserPartial acc = elements.iterator().next();
            acc.userId = userId;
            acc.windowStartMs = ctx.window().getStart();
            acc.windowEndMs = ctx.window().getEnd();
            out.collect(acc);
        }
    }

    public static final class ItemWindowAgg implements AggregateFunction<Event, ItemPartial, ItemPartial> {
        @Override
        public ItemPartial createAccumulator() {
            return new ItemPartial();
        }

        @Override
        public ItemPartial add(Event event, ItemPartial acc) {
            acc.itemId = event.itemId;
            acc.categoryCode = event.categoryCode == null ? "" : event.categoryCode;
            acc.pv += 1;
            return acc;
        }

        @Override
        public ItemPartial getResult(ItemPartial acc) {
            return acc.copy();
        }

        @Override
        public ItemPartial merge(ItemPartial left, ItemPartial right) {
            ItemPartial merged = left.copy();
            merged.pv += right.pv;
            if (merged.categoryCode.isEmpty()) {
                merged.categoryCode = right.categoryCode;
            }
            if (merged.itemId == 0) {
                merged.itemId = right.itemId;
            }
            return merged;
        }
    }

    public static final class ItemWindowEmit extends ProcessWindowFunction<ItemPartial, ItemPartial, Long, TimeWindow> {
        @Override
        public void process(Long itemId, Context ctx, Iterable<ItemPartial> elements, Collector<ItemPartial> out) {
            ItemPartial acc = elements.iterator().next();
            acc.itemId = itemId;
            acc.windowStartMs = ctx.window().getStart();
            acc.windowEndMs = ctx.window().getEnd();
            out.collect(acc);
        }
    }

    public static final class CategoryWindowAgg implements AggregateFunction<Event, CategoryPartial, CategoryPartial> {
        @Override
        public CategoryPartial createAccumulator() {
            return new CategoryPartial();
        }

        @Override
        public CategoryPartial add(Event event, CategoryPartial acc) {
            acc.pv += 1;
            return acc;
        }

        @Override
        public CategoryPartial getResult(CategoryPartial acc) {
            return acc.copy();
        }

        @Override
        public CategoryPartial merge(CategoryPartial left, CategoryPartial right) {
            CategoryPartial merged = new CategoryPartial();
            merged.pv = left.pv + right.pv;
            return merged;
        }
    }

    public static final class CategoryWindowEmit
            extends ProcessWindowFunction<CategoryPartial, CategoryPvRow, String, TimeWindow> {
        private final String runId;

        public CategoryWindowEmit(String runId) {
            this.runId = runId;
        }

        @Override
        public void process(String category, Context ctx, Iterable<CategoryPartial> elements, Collector<CategoryPvRow> out) {
            CategoryPvRow row = new CategoryPvRow();
            row.runId = runId;
            row.categoryCode = category == null ? "UNKNOWN" : category;
            row.windowStartMs = ctx.window().getStart();
            row.windowEndMs = ctx.window().getEnd();
            row.pv = elements.iterator().next().pv;
            out.collect(row);
        }
    }
}
