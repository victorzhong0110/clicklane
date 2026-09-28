CREATE DATABASE IF NOT EXISTS pulseboard;

CREATE TABLE IF NOT EXISTS pulseboard.ods_events (
    run_id String,
    event_id String,
    user_id Int64,
    item_id Int64,
    behavior LowCardinality(String),
    category_id Int64,
    category_code String,
    event_time DateTime64(3, 'UTC'),
    produce_time DateTime64(3, 'UTC'),
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    dim_miss UInt8,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, event_id);

CREATE TABLE IF NOT EXISTS pulseboard.ads_pv_uv (
    run_id String,
    window_start DateTime('UTC'),
    window_end DateTime('UTC'),
    pv UInt64,
    uv UInt64,
    cart_cnt UInt64,
    buy_cnt UInt64,
    events UInt64,
    dim_miss UInt64,
    max_produce_time DateTime64(3, 'UTC'),
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, window_start);

CREATE TABLE IF NOT EXISTS pulseboard.ads_funnel (
    run_id String,
    window_start DateTime('UTC'),
    window_end DateTime('UTC'),
    pv_users UInt64,
    cart_users UInt64,
    buy_users UInt64,
    pv_to_cart_users UInt64,
    cart_to_buy_users UInt64,
    pv_to_buy_users UInt64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, window_start);

CREATE TABLE IF NOT EXISTS pulseboard.ads_top_items (
    run_id String,
    window_start DateTime('UTC'),
    window_end DateTime('UTC'),
    rank UInt16,
    item_id Int64,
    category_code String,
    pv UInt64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, window_start, rank);

CREATE TABLE IF NOT EXISTS pulseboard.ads_category_pv (
    run_id String,
    window_start DateTime('UTC'),
    window_end DateTime('UTC'),
    category_code String,
    pv UInt64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, window_start, category_code);

CREATE TABLE IF NOT EXISTS pulseboard.dwd_late_events (
    run_id String,
    event_id String,
    user_id Int64,
    item_id Int64,
    behavior LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    window_start DateTime('UTC'),
    reason LowCardinality(String),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, event_id, reason);

CREATE TABLE IF NOT EXISTS pulseboard.dwd_invalid_events (
    run_id String,
    event_id String,
    reason LowCardinality(String),
    user_id Int64,
    item_id Int64,
    behavior String,
    event_time_ms Int64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, event_id, reason);

CREATE TABLE IF NOT EXISTS pulseboard.dwd_duplicate_events (
    run_id String,
    event_id String,
    reason LowCardinality(String),
    user_id Int64,
    item_id Int64,
    behavior String,
    event_time_ms Int64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, event_id, reason);

CREATE TABLE IF NOT EXISTS pulseboard.batch_events (
    event_id String,
    user_id Int64,
    item_id Int64,
    behavior LowCardinality(String),
    event_time_ms Int64
) ENGINE = MergeTree
ORDER BY (event_time_ms, user_id, item_id);

CREATE TABLE IF NOT EXISTS pulseboard.dim_item (
    item_id Int64,
    category_id Int64,
    category_code String
) ENGINE = MergeTree
ORDER BY item_id;

CREATE TABLE IF NOT EXISTS pulseboard.ads_reconcile (
    run_id String,
    metric LowCardinality(String),
    window_start DateTime('UTC'),
    rt_value Float64,
    batch_value Float64,
    abs_diff Float64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id, metric, window_start);
