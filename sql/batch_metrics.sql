-- ClickHouse 离线口径，和 batch/metrics.py、Flink 作业对齐。
-- 窗口左端点：intDiv(event_time_ms, window_ms) * window_ms，按 epoch 对齐。
-- 不用 toStartOfInterval，避免会话时区把边界挪走。
-- scripts/benchmark.py 按 "-- name:" 切分，并把 {window_ms}、{top_n} 替换成数字。
--
-- minIf / maxIf 在没有命中行时返回该类型的默认值 0，不是 NULL。
-- 漏斗必须先判断 countIf(...) > 0，否则每个用户都会被算进 pv_users。

-- name: pv_uv
SELECT
    intDiv(event_time_ms, {window_ms}) * {window_ms} AS window_start_ms,
    countIf(behavior = 'pv') AS pv,
    uniqExactIf(user_id, behavior = 'pv') AS uv,
    countIf(behavior = 'cart') AS cart_cnt,
    countIf(behavior = 'buy') AS buy_cnt,
    count() AS events,
    countIf(category_code = 'UNKNOWN') AS dim_miss
FROM
(
    SELECT
        e.event_time_ms AS event_time_ms,
        e.user_id AS user_id,
        e.behavior AS behavior,
        ifNull(nullIf(d.category_code, ''), 'UNKNOWN') AS category_code
    FROM pulseboard.batch_events AS e
    LEFT JOIN pulseboard.dim_item AS d ON e.item_id = d.item_id
)
GROUP BY window_start_ms
ORDER BY window_start_ms

-- name: funnel
SELECT
    window_start_ms,
    countIf(min_pv IS NOT NULL) AS pv_users,
    countIf(min_cart IS NOT NULL) AS cart_users,
    countIf(min_buy IS NOT NULL) AS buy_users,
    countIf(min_pv IS NOT NULL AND min_cart IS NOT NULL AND min_cart >= min_pv) AS pv_to_cart_users,
    countIf(min_cart IS NOT NULL AND min_buy IS NOT NULL AND min_buy >= min_cart) AS cart_to_buy_users,
    countIf(min_pv IS NOT NULL AND min_buy IS NOT NULL AND min_buy >= min_pv) AS pv_to_buy_users
FROM
(
    SELECT
        intDiv(event_time_ms, {window_ms}) * {window_ms} AS window_start_ms,
        user_id,
        if(countIf(behavior = 'pv') > 0, minIf(event_time_ms, behavior = 'pv'), NULL) AS min_pv,
        if(countIf(behavior = 'cart') > 0, minIf(event_time_ms, behavior = 'cart'), NULL) AS min_cart,
        if(countIf(behavior = 'buy') > 0, minIf(event_time_ms, behavior = 'buy'), NULL) AS min_buy
    FROM pulseboard.batch_events
    GROUP BY window_start_ms, user_id
)
GROUP BY window_start_ms
ORDER BY window_start_ms

-- name: top_items
SELECT window_start_ms, rank, item_id, category_code, pv
FROM
(
    SELECT
        window_start_ms,
        item_id,
        category_code,
        pv,
        row_number() OVER (PARTITION BY window_start_ms ORDER BY pv DESC, item_id ASC) AS rank
    FROM
    (
        SELECT
            intDiv(e.event_time_ms, {window_ms}) * {window_ms} AS window_start_ms,
            e.item_id AS item_id,
            ifNull(nullIf(d.category_code, ''), 'UNKNOWN') AS category_code,
            count() AS pv
        FROM pulseboard.batch_events AS e
        LEFT JOIN pulseboard.dim_item AS d ON e.item_id = d.item_id
        WHERE e.behavior = 'pv'
        GROUP BY window_start_ms, item_id, category_code
    )
)
WHERE rank <= {top_n}
ORDER BY window_start_ms, rank

-- name: duplicates
SELECT count() - uniqExact(user_id, item_id, behavior, event_time_ms) AS duplicate_events
FROM pulseboard.batch_events
