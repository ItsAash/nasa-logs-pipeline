from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class KafkaReader:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def read_bronze_stream(self, spark: SparkSession):
        """Reads raw data from Kafka."""
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        return df.select(col("value").cast("string").alias("log"))