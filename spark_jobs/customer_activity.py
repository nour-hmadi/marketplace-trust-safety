"""Curated customer activity per seller, with masked customer IDs.

The real customer_id never leaves the refined layer: it is replaced by a salted SHA-256 hash.
The same customer always gets the same masked ID, but the masked ID can't be turned back.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, concat, count, lit, max as spark_max,
                                   round as spark_round, sha2, sum as spark_sum, when)

spark = SparkSession.builder.appName("CustomerActivity").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

import os

SALT = os.environ.get("MASKING_SALT")   # secret, passed in by the Flask gateway from the local .env file
if not SALT:
    raise RuntimeError("MASKING_SALT is not set: refusing to mask customer IDs with a public salt")

orders = spark.read.parquet("hdfs://namenode:9000/data/refined/orders/")
returns = spark.read.parquet("hdfs://namenode:9000/data/refined/returns/")

# data quality: returns that match no order of the same seller
orphans = returns.join(orders.select("order_id", "seller_id"), ["order_id", "seller_id"], "left_anti").count()

# one row per returned order, flagging counterfeit returns
returned = returns.groupBy("order_id", "seller_id").agg(
    count("*").alias("return_events"),
    spark_max(when(col("reason") == "counterfeit", 1).otherwise(0)).alias("counterfeit"),
)

activity = (orders.join(returned, ["order_id", "seller_id"], "left")
            .withColumn("customer_masked", sha2(concat(lit(SALT), col("customer_id")), 256))
            .groupBy("seller_id", "customer_masked")
            .agg(count("order_id").alias("orders"),
                 spark_round(spark_sum("amount"), 2).alias("total_spent"),
                 count("return_events").alias("returns"),
                 spark_sum(coalesce(col("counterfeit"), lit(0))).alias("counterfeit_returns"))
            .withColumn("return_rate", spark_round(col("returns") / col("orders"), 3)))

activity.write.mode("overwrite").parquet("hdfs://namenode:9000/data/curated/customer_activity/")
print(f"Returns that match no order of the same seller: {orphans}")
print(f"Done: customer_activity ({activity.count()} customer-seller pairs)")
spark.stop()
