from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim

spark = SparkSession.builder.appName("NormalizeSellers").getOrCreate()

df = spark.read.json("hdfs://namenode:9000/data/raw/sellers/")
df_clean = df \
    .withColumn("registration_date", to_date(col("registration_date"))) \
    .withColumn("seller_name", trim(col("seller_name"))) \
    .dropDuplicates(["seller_id"]) \
    .filter(col("seller_id").isNotNull())

df_clean.write.mode("overwrite").parquet("hdfs://namenode:9000/data/refined/sellers/")
print("Done: normalize_sellers")
spark.stop()
