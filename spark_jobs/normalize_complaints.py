from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, trim

spark = SparkSession.builder.appName("NormalizeComplaints").getOrCreate()

df = spark.read.json("hdfs://namenode:9000/data/raw/complaints/")
df_clean = df \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .withColumn("severity", lower(trim(col("severity")))) \
    .withColumn("category", lower(trim(col("category")))) \
    .drop("timestamp") \
    .dropDuplicates(["complaint_id"]) \
    .filter(col("complaint_id").isNotNull())

df_clean.write.mode("overwrite").parquet("hdfs://namenode:9000/data/refined/complaints/")
print("Done: normalize_complaints")
spark.stop()
