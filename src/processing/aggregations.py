from pyspark.sql.functions import date_trunc, window, col, when, round, count, sum, avg, countDistinct, min, max
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def get_hourly_df(df: DataFrame):
    hourly_df = (df
        .withColumn("hour_bucket", date_trunc("hour", col("timestamp")))
        .groupBy("hour_bucket")
        .agg(
            count("*").alias("total_requests"),
            sum("bytes").alias("total_bytes"),
            count(when(col("status") == 404, 1)).alias("errors_404"),
            count(when(col("status") == 500, 1)).alias("errors_500"),
            count(when(col("status").between(200, 299), 1)).alias("success_count")
        )
        .withColumn("error_rate",
            round((col("errors_404") + col("errors_500")) / col("total_requests"), 4)
        )
        .withColumn("success_rate",
            round(col("success_count") / col("total_requests"), 4)
        )
        .orderBy("hour_bucket")
    )

    # Define the window: partition by nothing (whole dataset), order by time, 
    # look back 2 rows (= current + 2 previous hours = 3hr window)
    hourly_window = (Window
        .orderBy("hour_bucket")
        .rowsBetween(-2, 0))  # -2 = 2 rows back, 0 = current row

    hourly_df = hourly_df.withColumn(
        "rolling_3hr_avg_requests",
        round(avg("total_requests").over(hourly_window), 2)
    )
    return hourly_df

def get_ip_df(df: DataFrame):
    ip_df = (df
        .groupBy("host")
        .agg(
            count("*").alias("total_requests"),
            sum("bytes").alias("total_bytes_transferred"),
            count(when(col("status") >= 400, 1)).alias("total_errors"),
            round(
                count(when(col("status") >= 400, 1)) / count("*") * 100, 2
            ).alias("error_rate_pct"),
            min("timestamp").alias("first_seen"),
            max("timestamp").alias("last_seen"),
            countDistinct("endpoint").alias("unique_endpoints_hit")
        )
        .orderBy("total_requests", ascending=False)
    )
    
    return ip_df


def get_endpoint_df(df: DataFrame):
    endpoint_df = (df
        .groupBy("endpoint")
        .agg(
            count("*").alias("total_hits"),
            round(avg("bytes"), 2).alias("avg_bytes"),
            max("bytes").alias("max_bytes"),
            count(when(col("status") == 404, 1)).alias("not_found_count"),
            count(when(col("status") == 200, 1)).alias("success_count"),
            countDistinct("host").alias("unique_visitors")
        )
        .orderBy("total_hits", ascending=False)
    )
    return endpoint_df



def get_live_ip_activity(df: DataFrame) -> DataFrame:
    return (df
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), "10 minutes", "5 minutes"),
            col("host")
        )
        .agg(
            count("*").alias("live_requests"),
            count(when(col("status") >= 400, True)).alias("live_errors"),
            count(when(col("status") == 404, True)).alias("live_404s")
        )
        .select(
            col("host"),
            col("window.start").alias("window_start"),
            col("live_requests"),
            col("live_errors"),
            col("live_404s"),
            round(col("live_errors") / col("live_requests") * 100, 2).alias("live_error_rate")
        )
    )

