from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, trim

spark = SparkSession.builder \
    .appName("NormalizeOrders") \
    .enableHiveSupport() \
    .getOrCreate()

print("Reading raw orders...")
df = spark.read.json("hdfs://namenode:9000/data/raw/orders/")

df_clean = df \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .withColumn("status", lower(trim(col("status")))) \
    .withColumn("category", lower(trim(col("category")))) \
    .withColumn("amount", col("amount").cast("double")) \
    .drop("timestamp") \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("seller_id").isNotNull()) \
    .filter(col("amount") > 0)

print("Writing refined orders...")
df_clean.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/refined/orders/")

spark.sql("MSCK REPAIR TABLE refined.orders")
print("Done: normalize_orders")
spark.stop()