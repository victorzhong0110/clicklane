package io.pulseboard.pipeline.agg;

/** 窗口第一阶段吐出的中间结果。public 字段，Flink 按 POJO 序列化。 */
public final class Partials {
    private Partials() {}

    public static class UserPartial {
        public long userId;
        public long windowStartMs;
        public long windowEndMs;
        public long pv;
        public long cart;
        public long buy;
        public long events;
        public long dimMiss;
        public long maxProduceTimeMs;
        public long minPvTs = Long.MAX_VALUE;
        public long minCartTs = Long.MAX_VALUE;
        public long minBuyTs = Long.MAX_VALUE;

        public UserPartial copy() {
            UserPartial copy = new UserPartial();
            copy.userId = userId;
            copy.windowStartMs = windowStartMs;
            copy.windowEndMs = windowEndMs;
            copy.pv = pv;
            copy.cart = cart;
            copy.buy = buy;
            copy.events = events;
            copy.dimMiss = dimMiss;
            copy.maxProduceTimeMs = maxProduceTimeMs;
            copy.minPvTs = minPvTs;
            copy.minCartTs = minCartTs;
            copy.minBuyTs = minBuyTs;
            return copy;
        }
    }

    public static class ItemPartial {
        public long itemId;
        public String categoryCode = "";
        public long pv;
        public long windowStartMs;
        public long windowEndMs;

        public ItemPartial copy() {
            ItemPartial copy = new ItemPartial();
            copy.itemId = itemId;
            copy.categoryCode = categoryCode;
            copy.pv = pv;
            copy.windowStartMs = windowStartMs;
            copy.windowEndMs = windowEndMs;
            return copy;
        }
    }

    public static class CategoryPartial {
        public long pv;

        public CategoryPartial copy() {
            CategoryPartial copy = new CategoryPartial();
            copy.pv = pv;
            return copy;
        }
    }
}
