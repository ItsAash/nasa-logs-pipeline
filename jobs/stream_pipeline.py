from src.ingestion.kafka_reader import KafkaReader
from src.config.spark import get_spark
from src.processing.parser import parse_bronze_to_silver
from src.processing.aggregations import get_live_ip_activity
from src.processing.anomaly import detect_anomalies
from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, GOLD_PATH

def run_pipeline():
    spark = get_spark()
    reader = KafkaReader(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    # 1. BRONZE: Ingestion
    raw_stream = reader.read_bronze_stream(spark)

    # 2. SILVER: Parsing & Initial Aggregation
    parsed_stream = parse_bronze_to_silver(raw_stream)
    aggregated_stream = get_live_ip_activity(parsed_stream)

    # 3. Load batch baseline
    ip_profiles = spark.read.format("delta").load(
        f"{GOLD_PATH}/ip_baselines"
    )

    global_stats_row = spark.read.format("delta").load(
        f"{GOLD_PATH}/global_stats"
    ).collect()[0]

    GLOBAL_MEAN = global_stats_row["global_mean"]
    GLOBAL_STDDEV = global_stats_row["global_stddev"]

    # 4. GOLD: Anomaly Detection
    anomaly_stream = detect_anomalies(
        aggregated_stream,
        ip_profiles,
        GLOBAL_MEAN,
        GLOBAL_STDDEV
    )

    # 5. SINK: Write to Gold Delta Table
    query = (anomaly_stream
        .writeStream
        .format("delta")
        .queryName("anomaly_detection_stream")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/workspace/default/logs_volume/checkpoints/anomaly_stream")
        .trigger(availableNow=True)
        .start("/Volumes/workspace/default/logs_volume/output/anomalies")
    )

    query.awaitTermination()



if __name__ == "__main__":
    run_pipeline()

    
