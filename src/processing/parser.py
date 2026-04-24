from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, col, when, try_to_timestamp, lit
from constants import LOG_PATTERN, TIMESTAMP_FORMAT

def parse_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Applies Regex to raw Kafka value and casts to typed Silver schema.
    """
    # 1. Regex Extraction
    parsed_df = df.select(
        regexp_extract("log", LOG_PATTERN, 1).alias("host"),
        regexp_extract("log", LOG_PATTERN, 2).alias("timestamp_raw"),
        regexp_extract("log", LOG_PATTERN, 3).alias("method"),
        regexp_extract("log", LOG_PATTERN, 4).alias("endpoint"),
        regexp_extract("log", LOG_PATTERN, 5).alias("protocol"),
        regexp_extract("log", LOG_PATTERN, 6).alias("status_code_raw"),
        regexp_extract("log", LOG_PATTERN, 7).alias("bytes_raw"),
    )

    # 2. Type Casting and Cleaning
    silver_df = parsed_df.select(
        col("host"),

        try_to_timestamp(
        when(col("timestamp_raw") == "", lit(None))
        .otherwise(col("timestamp_raw")),
            lit(TIMESTAMP_FORMAT)
        ).alias("timestamp"),

        col("method"),
        col("endpoint"),
        col("protocol"),

        when(col("status_code_raw") == "", lit(None))
        .otherwise(col("status_code_raw"))
        .cast("int")
        .alias("status"),

        when((col("bytes_raw") == "-") | (col("bytes_raw") == ""), 0)
        .otherwise(col("bytes_raw"))
        .cast("int")
        .alias("bytes"),
    )

    # 3. Filter malformed rows (where host extraction failed)
    return silver_df.filter(col("host") != "")

    