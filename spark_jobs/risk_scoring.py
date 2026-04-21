from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp

spark = SparkSession.builder \
    .appName("RiskScoring") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/data/refined/seller_aggregates/")

# Risk score: weighted formula
df = df.withColumn(
    "risk_score",
    (col("return_rate") * 40) +
    (col("high_severity_complaints") * 15) +
    (when(col("verification_status") == "unverified", 20).otherwise(0)) +
    (when(col("total_complaints") > 3, 15).otherwise(0)) +
    (when(col("total_orders") > 100, -10).otherwise(0))
)

# Risk label based on score
df = df.withColumn(
    "risk_label",
    when(col("risk_score") >= 60, "HIGH")
    .when(col("risk_score") >= 30, "MEDIUM")
    .otherwise("LOW")
)

# Fraud patterns: flag specific suspicious combinations
fraud_df = df.filter(
    (col("return_rate") > 0.5) |
    (col("high_severity_complaints") >= 2) |
    ((col("verification_status") == "unverified") & (col("total_complaints") > 2))
).select(
    col("seller_id"),
    when(col("return_rate") > 0.5, "HIGH_RETURN_RATE")
    .when(col("high_severity_complaints") >= 2, "REPEATED_HIGH_COMPLAINTS")
    .otherwise("UNVERIFIED_WITH_COMPLAINTS").alias("pattern_type"),
    col("risk_score").alias("indicator_value"),
    lit(30.0).alias("threshold"),
    current_timestamp().alias("flagged_at"),
    when(col("return_rate") > 0.5, "Return rate exceeds 50% threshold")
    .when(col("high_severity_complaints") >= 2, "Multiple high severity complaints detected")
    .otherwise("Unverified seller with multiple complaints").alias("explanation")
)

# Write seller risk to curated layer
df.select(
    "seller_id", "seller_name", "region", "verification_status",
    "total_orders", "total_returns", "return_rate",
    "total_complaints", "high_severity_complaints",
    "avg_order_amount", "risk_score", "risk_label"
).write.mode("overwrite") \
 .parquet("hdfs://namenode:9000/data/curated/seller_risk/")

# Write fraud patterns to curated layer
fraud_df.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/fraud_patterns/")

print("Done: risk_scoring")
spark.stop()