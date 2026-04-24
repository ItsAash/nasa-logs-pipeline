from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round, broadcast


def detect_anomalies(
    df: DataFrame,
    ip_profile: DataFrame,
    GLOBAL_MEAN: float,
    GLOBAL_STDDEV: float
) -> DataFrame:
    """
    Detect anomalies using batch-informed baselines + live stream activity.
    """

    enriched = (df
        .join(
            broadcast(ip_profile),
            on="host",
            how="left"
        )
        .withColumn(
            "rate_multiplier",
            round(
                col("live_requests") /
                (col("expected_hourly_requests") / 6 + lit(1)),
                2
            )
        )
        .withColumn(
            "global_z_score",
            round(
                (col("live_requests") - lit(GLOBAL_MEAN / 6)) /
                lit(GLOBAL_STDDEV / 6),
                2
            )
        )
    )

    anomalies = (enriched
        .withColumn(
            "anomaly_type",
            when(
                col("batch_total_requests").isNotNull() &
                (col("rate_multiplier") > 5),
                "PERSONAL_SPIKE"
            )
            .when(
                col("batch_total_requests").isNull() &
                (col("global_z_score") > 3),
                "UNKNOWN_IP_SURGE"
            )
            .when(
                col("live_error_rate") > 30,
                "LIVE_ERROR_SURGE"
            )
            .when(
                (col("live_404s") > 20) &
                (col("batch_error_rate") < 5),
                "NEW_SCANNING_BEHAVIOUR"
            )
        )
        .filter(col("anomaly_type").isNotNull())
        .withColumn(
            "severity",
            when(col("rate_multiplier") > 10, "CRITICAL")
            .when(col("rate_multiplier") > 5, "HIGH")
            .when(col("live_error_rate") > 50, "CRITICAL")
            .otherwise("MEDIUM")
        )
    )

    return anomalies