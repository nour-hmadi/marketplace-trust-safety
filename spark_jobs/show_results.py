from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ShowResults").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("\n========= SELLER RISK TABLE =========")
spark.read.parquet("hdfs://namenode:9000/data/curated/seller_risk/").show(truncate=False)
print("\n========= FRAUD PATTERNS TABLE =========")
spark.read.parquet("hdfs://namenode:9000/data/curated/fraud_patterns/").show(truncate=False)
spark.stop()
