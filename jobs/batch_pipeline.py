from src.config.spark import get_spark
from src.ingestion.batch_ingestion import write_to_bronze
from src.processing.parser import parse_bronze_to_silver
from src.processing.aggregations import get_hourly_df, get_ip_df, get_endpoint_df
from constants import RAW_LOG_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH
from pyspark.sql.functions import col, round, avg, stddev

def run_pipeline(raw_path, bronze_path, silver_path, gold_path):
    spark = get_spark("batch-baseline-job")

    # ─────────────────────────────
    # 1. BRONZE
    # ─────────────────────────────

    bronze_df = write_to_bronze(spark, raw_path, bronze_path)

    # ─────────────────────────────
    # 2. SILVER
    # ─────────────────────────────

    silver_df = parse_bronze_to_silver(bronze_df)

    (silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("status")
        .save(silver_path)
    )

    # ─────────────────────────────
    # 3. GOLD - Aggregations
    # ─────────────────────────────

    hourly_df = get_hourly_df(silver_df)
    ip_df = get_ip_df(silver_df)
    endpoint_df = get_endpoint_df(silver_df)

    # ─────────────────────────────
    # 4. IP BASELINE (for streaming)
    # ─────────────────────────────

    ip_baseline = (ip_df
        .select(
            col("host"),
            col("total_requests").alias("batch_total_requests"),
            col("error_rate_pct").alias("batch_error_rate"),
            col("unique_endpoints_hit").alias("batch_endpoint_breadth"),
            round(col("total_requests") / (28 * 24), 2)
                .alias("expected_hourly_requests")
        )
    )

    (ip_baseline
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/ip_baselines")
    )

    # ─────────────────────────────
    # 5. GLOBAL STATS (for streaming anomaly detection)
    # ─────────────────────────────

    global_stats = hourly_df.agg(
        avg("total_requests").alias("mean"),
        stddev("total_requests").alias("stddev")
    ).collect()[0]

    global_stats_df = spark.createDataFrame(
        [(float(global_stats["mean"]), float(global_stats["stddev"]))],
        ["global_mean", "global_stddev"]
    )

    (global_stats_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/global_stats")
    )

    # ─────────────────────────────
    # 6. GOLD TABLES
    # ─────────────────────────────

    (hourly_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/hourly_stats")
    )

    (ip_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/ip_behaviour")
    )

    (endpoint_df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{gold_path}/endpoint_stats")
    )
    print("Batch pipeline completed successfully.")
    

if __name__ == "__main__":
    run_pipeline(
        raw_path = RAW_LOG_PATH,
        bronze_path = BRONZE_PATH,
        silver_path = SILVER_PATH,
        gold_path = GOLD_PATH
    )


