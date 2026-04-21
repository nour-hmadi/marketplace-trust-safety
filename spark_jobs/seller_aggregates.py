from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, when

spark = SparkSession.builder \
    .appName("SellerAggregates") \
    .enableHiveSupport() \
    .getOrCreate()

orders    = spark.read.parquet("hdfs://namenode:9000/data/refined/orders/")
returns   = spark.read.parquet("hdfs://namenode:9000/data/refined/returns/")
complaints = spark.read.parquet("hdfs://namenode:9000/data/refined/complaints/")
sellers   = spark.read.parquet("hdfs://namenode:9000/data/refined/sellers/")

# Order metrics per seller
order_metrics = orders.groupBy("seller_id").agg(
    count("order_id").alias("total_orders"),
    avg("amount").alias("avg_order_amount")
)

# Return metrics per seller
return_metrics = returns.groupBy("seller_id").agg(
    count("return_id").alias("total_returns")
)

# Complaint metrics per seller
complaint_metrics = complaints.groupBy("seller_id").agg(
    count("complaint_id").alias("total_complaints"),
    count(when(col("severity") == "high", 1)).alias("high_severity_complaints")
)

# Join everything together
seller_summary = sellers \
    .join(order_metrics, "seller_id", "left") \
    .join(return_metrics, "seller_id", "left") \
    .join(complaint_metrics, "seller_id", "left") \
    .fillna(0)

# Calculate return rate
seller_summary = seller_summary.withColumn(
    "return_rate",
    when(col("total_orders") > 0,
         col("total_returns") / col("total_orders")
    ).otherwise(0.0)
)

print("Writing seller aggregates...")
seller_summary.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/refined/seller_aggregates/")

print("Done: seller_aggregates")
spark.stop()