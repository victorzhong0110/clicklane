package io.pulseboard.pipeline.model;

import io.pulseboard.pipeline.json.JsonText;

/** 写入 ClickHouse 的结果行。toJson 的字段名和 init.sql 里的列名一一对应。 */
public final class Rows {
    private Rows() {}

    public static final class PvUvRow {
        public String runId = "";
        public long windowStartMs;
        public long windowEndMs;
        public long pv;
        public long uv;
        public long cartCnt;
        public long buyCnt;
        public long events;
        public long dimMiss;
        public long maxProduceTimeMs;

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"window_start\":\"" + JsonText.dateTime(windowStartMs) + "\","
                    + "\"window_end\":\"" + JsonText.dateTime(windowEndMs) + "\","
                    + "\"pv\":" + pv + ","
                    + "\"uv\":" + uv + ","
                    + "\"cart_cnt\":" + cartCnt + ","
                    + "\"buy_cnt\":" + buyCnt + ","
                    + "\"events\":" + events + ","
                    + "\"dim_miss\":" + dimMiss + ","
                    + "\"max_produce_time\":\"" + JsonText.dateTime64(maxProduceTimeMs) + "\","
                    + "\"version\":" + version
                    + "}";
        }
    }

    public static final class FunnelRow {
        public String runId = "";
        public long windowStartMs;
        public long windowEndMs;
        public long pvUsers;
        public long cartUsers;
        public long buyUsers;
        public long pvToCartUsers;
        public long cartToBuyUsers;
        public long pvToBuyUsers;

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"window_start\":\"" + JsonText.dateTime(windowStartMs) + "\","
                    + "\"window_end\":\"" + JsonText.dateTime(windowEndMs) + "\","
                    + "\"pv_users\":" + pvUsers + ","
                    + "\"cart_users\":" + cartUsers + ","
                    + "\"buy_users\":" + buyUsers + ","
                    + "\"pv_to_cart_users\":" + pvToCartUsers + ","
                    + "\"cart_to_buy_users\":" + cartToBuyUsers + ","
                    + "\"pv_to_buy_users\":" + pvToBuyUsers + ","
                    + "\"version\":" + version
                    + "}";
        }
    }

    public static final class TopItemRow {
        public String runId = "";
        public long windowStartMs;
        public long windowEndMs;
        public int rank;
        public long itemId;
        public String categoryCode = "";
        public long pv;

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"window_start\":\"" + JsonText.dateTime(windowStartMs) + "\","
                    + "\"window_end\":\"" + JsonText.dateTime(windowEndMs) + "\","
                    + "\"rank\":" + rank + ","
                    + "\"item_id\":" + itemId + ","
                    + "\"category_code\":\"" + JsonText.quote(categoryCode) + "\","
                    + "\"pv\":" + pv + ","
                    + "\"version\":" + version
                    + "}";
        }
    }

    public static final class CategoryPvRow {
        public String runId = "";
        public long windowStartMs;
        public long windowEndMs;
        public String categoryCode = "";
        public long pv;

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"window_start\":\"" + JsonText.dateTime(windowStartMs) + "\","
                    + "\"window_end\":\"" + JsonText.dateTime(windowEndMs) + "\","
                    + "\"category_code\":\"" + JsonText.quote(categoryCode) + "\","
                    + "\"pv\":" + pv + ","
                    + "\"version\":" + version
                    + "}";
        }
    }

    public static final class LateRow {
        public String runId = "";
        public String eventId = "";
        public long userId;
        public long itemId;
        public String behavior = "";
        public long eventTimeMs;
        public long windowStartMs;
        public String reason = "window_closed";

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"event_id\":\"" + JsonText.quote(eventId) + "\","
                    + "\"user_id\":" + userId + ","
                    + "\"item_id\":" + itemId + ","
                    + "\"behavior\":\"" + JsonText.quote(behavior) + "\","
                    + "\"event_time\":\"" + JsonText.dateTime64(eventTimeMs) + "\","
                    + "\"window_start\":\"" + JsonText.dateTime(windowStartMs) + "\","
                    + "\"reason\":\"" + JsonText.quote(reason) + "\","
                    + "\"version\":" + version
                    + "}";
        }
    }

    public static final class RejectRow {
        public String runId = "";
        public String eventId = "";
        public String reason = "";
        public long userId;
        public long itemId;
        public String behavior = "";
        public long eventTimeMs;

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"event_id\":\"" + JsonText.quote(eventId) + "\","
                    + "\"reason\":\"" + JsonText.quote(reason) + "\","
                    + "\"user_id\":" + userId + ","
                    + "\"item_id\":" + itemId + ","
                    + "\"behavior\":\"" + JsonText.quote(behavior) + "\","
                    + "\"event_time_ms\":" + eventTimeMs + ","
                    + "\"version\":" + version
                    + "}";
        }
    }

    public static final class OdsRow {
        public String runId = "";
        public String eventId = "";
        public long userId;
        public long itemId;
        public String behavior = "";
        public long categoryId;
        public String categoryCode = "";
        public long eventTimeMs;
        public long produceTimeMs;
        public int dimMiss;

        public String toJson(long version) {
            return "{"
                    + "\"run_id\":\"" + JsonText.quote(runId) + "\","
                    + "\"event_id\":\"" + JsonText.quote(eventId) + "\","
                    + "\"user_id\":" + userId + ","
                    + "\"item_id\":" + itemId + ","
                    + "\"behavior\":\"" + JsonText.quote(behavior) + "\","
                    + "\"category_id\":" + categoryId + ","
                    + "\"category_code\":\"" + JsonText.quote(categoryCode) + "\","
                    + "\"event_time\":\"" + JsonText.dateTime64(eventTimeMs) + "\","
                    + "\"produce_time\":\"" + JsonText.dateTime64(produceTimeMs) + "\","
                    + "\"dim_miss\":" + dimMiss + ","
                    + "\"version\":" + version
                    + "}";
        }
    }
}
