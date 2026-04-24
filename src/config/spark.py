from pyspark.sql import SparkSession


def get_spark(app_name: str = "log-anomaly-detection") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Kafka support (important for local / non-Databricks)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )