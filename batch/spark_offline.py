"""同一口径的 Spark 离线作业。

窗口、漏斗顺序、Top-N 次序都和 batch/metrics.py、Flink 作业一致。
输出写到 results/spark/，对账脚本再和实时表比。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, default=ROOT / "data" / "sample" / "events.csv")
    parser.add_argument("--dim", type=Path, default=ROOT / "data" / "dim" / "dim_item.csv")
    parser.add_argument("--meta", type=Path, default=ROOT / "data" / "sample" / "meta.json")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "spark")
    args = parser.parse_args()

    meta = json.loads(args.meta.read_text(encoding="utf-8"))
    window_ms = int(meta["window_ms"])
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pulseboard-batch")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    events = (
        spark.read.option("header", True)
        .csv(str(args.events))
        .select(
            F.col("event_id"),
            F.col("user_id").cast("long"),
            F.col("item_id").cast("long"),
            F.col("behavior"),
            F.col("event_time_ms").cast("long"),
        )
    )
    dim = (
        spark.read.option("header", True)
        .csv(str(args.dim))
        .select(F.col("item_id").cast("long"), F.col("category_code"))
    )
    joined = events.join(dim, "item_id", "left").withColumn(
        "category_code", F.coalesce(F.col("category_code"), F.lit("UNKNOWN"))
    )
    joined = joined.withColumn(
        "window_start_ms",
        (F.col("event_time_ms") / F.lit(window_ms)).cast("long") * F.lit(window_ms),
    )

    pv_uv = joined.groupBy("window_start_ms").agg(
        F.count(F.when(F.col("behavior") == "pv", 1)).alias("pv"),
        F.countDistinct(F.when(F.col("behavior") == "pv", F.col("user_id"))).alias("uv"),
        F.count(F.when(F.col("behavior") == "cart", 1)).alias("cart_cnt"),
        F.count(F.when(F.col("behavior") == "buy", 1)).alias("buy_cnt"),
    )
    users = joined.groupBy("window_start_ms", "user_id").agg(
        F.min(F.when(F.col("behavior") == "pv", F.col("event_time_ms"))).alias("min_pv"),
        F.min(F.when(F.col("behavior") == "cart", F.col("event_time_ms"))).alias("min_cart"),
        F.min(F.when(F.col("behavior") == "buy", F.col("event_time_ms"))).alias("min_buy"),
    )
    funnel = users.groupBy("window_start_ms").agg(
        F.count(F.when(F.col("min_pv").isNotNull(), 1)).alias("pv_users"),
        F.count(F.when(F.col("min_cart").isNotNull(), 1)).alias("cart_users"),
        F.count(F.when(F.col("min_buy").isNotNull(), 1)).alias("buy_users"),
        F.count(
            F.when(F.col("min_pv").isNotNull() & F.col("min_cart").isNotNull() & (F.col("min_cart") >= F.col("min_pv")), 1)
        ).alias("pv_to_cart_users"),
        F.count(
            F.when(F.col("min_cart").isNotNull() & F.col("min_buy").isNotNull() & (F.col("min_buy") >= F.col("min_cart")), 1)
        ).alias("cart_to_buy_users"),
        F.count(
            F.when(F.col("min_pv").isNotNull() & F.col("min_buy").isNotNull() & (F.col("min_buy") >= F.col("min_pv")), 1)
        ).alias("pv_to_buy_users"),
    )
    item_counts = (
        joined.where(F.col("behavior") == "pv")
        .groupBy("window_start_ms", "item_id", "category_code")
        .agg(F.count(F.lit(1)).alias("pv"))
    )
    from pyspark.sql.window import Window

    ranked = item_counts.withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("window_start_ms").orderBy(F.col("pv").desc(), F.col("item_id").asc())
        ),
    ).where(F.col("rank") <= 10)

    args.out.mkdir(parents=True, exist_ok=True)
    pv_uv.coalesce(1).write.mode("overwrite").option("header", True).csv(str(args.out / "pv_uv"))
    funnel.coalesce(1).write.mode("overwrite").option("header", True).csv(str(args.out / "funnel"))
    ranked.coalesce(1).write.mode("overwrite").option("header", True).csv(str(args.out / "top_items"))
    spark.stop()
    print(json.dumps({"spark": "ok", "out": str(args.out)}))


if __name__ == "__main__":
    main()
