-- 口径对照，不是基准测试实际提交的作业。
-- 侧输出、幂等 ClickHouse Sink 用的是 DataStream API（见 AnalyticsTopology）。
-- 这里给出同一窗口口径的 Flink SQL，作为 DataStream 实现的对照。
--
-- 假设事件表已经声明 event_time 为 ROWTIME，水位延迟 2 分钟。

-- PV / UV
-- SELECT window_start, window_end,
--        COUNT(*) FILTER (WHERE behavior = 'pv') AS pv,
--        COUNT(DISTINCT user_id) FILTER (WHERE behavior = 'pv') AS uv
-- FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' MINUTES))
-- GROUP BY window_start, window_end;

-- Top-N：先按窗口+商品计数，再用 ROW_NUMBER 取前 10。
-- 排序必须是 pv DESC, item_id ASC，和 Java 的 TopNAssembler、Spark 的 row_number 一致。
-- SELECT * FROM (
--   SELECT window_start, item_id, pv,
--          ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY pv DESC, item_id ASC) AS rn
--   FROM item_counts
-- ) WHERE rn <= 10;
