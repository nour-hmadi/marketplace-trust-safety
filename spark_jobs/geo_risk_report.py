from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("GeoRisk").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:9000/data/curated/seller_risk/")
data = [row.asDict() for row in df.collect()]

with open("/tmp/data.json", "w") as f:
    json.dump(data, f)

print("Data exported!")
spark.stop()
