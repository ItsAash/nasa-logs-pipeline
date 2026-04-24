from pyspark.sql.functions import col

def write_to_bronze(spark, raw_path, bronze_path):
    raw_df = spark.read.text(raw_path)
    
    raw_df.write.format("delta").mode("overwrite").save(bronze_path)
    return raw_df.select(col("value").cast("string").alias("log"))