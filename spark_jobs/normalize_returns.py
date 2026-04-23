from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, trim

spark = SparkSession.builder \
    .appName("NormalizeReturns") \
    .enableHiveSupport() \
    .getOrCreate()

print("Reading raw returns...")
df = spark.read.json("hdfs://namenode:9000/data/raw/returns/")

df_clean = df \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .withColumn("status", lower(trim(col("status")))) \
    .withColumn("reason", lower(trim(col("reason")))) \
    .drop("timestamp") \
    .dropDuplicates(["return_id"]) \
    .filter(col("return_id").isNotNull()) \
    .filter(col("seller_id").isNotNull())

print("Writing refined returns...")
df_clean.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/refined/returns/")

print("Done: normalize_returns")
spark.stop()