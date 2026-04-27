from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, datediff

spark = SparkSession.builder \
    .appName("RiskScoring") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:9000/data/refined/seller_aggregates/")

# ── COMPONENT 1: Return Rate (naturally 0-1, weight 25%) ────
# return_rate = returns / total_orders → already computed
df = df.withColumn("return_component",
    col("return_rate") * 25
)

# ── COMPONENT 2: Complaint Volume Rate (naturally 0-1, weight 35%) ──
# complaint_rate = total_complaints / total_orders
# "how many orders generate a complaint?"
df = df.withColumn("complaint_rate",
    when(col("total_orders") > 0,
        col("total_complaints") / col("total_orders")
    ).otherwise(0.0)
)
df = df.withColumn("complaint_volume_component",
    when(col("complaint_rate") > 1.0, 1.0)
    .otherwise(col("complaint_rate")) * 35
)

# ── COMPONENT 3: High Severity Rate (naturally 0-1, weight 25%) ──
# high_severity_rate = high_complaints / total_complaints
# "of all complaints, what % are HIGH severity?"
df = df.withColumn("high_severity_rate",
    when(col("total_complaints") > 0,
        col("high_severity_complaints") / col("total_complaints")
    ).otherwise(0.0)
)
df = df.withColumn("complaint_severity_component",
    col("high_severity_rate") * 25
)

# ── COMPONENT 4: Trust Penalty (weight 10%) ─────────────────
# unverified sellers get full penalty
df = df.withColumn("trust_component",
    when(col("verification_status") == "unverified", 10.0)
    .otherwise(0.0)
)

# ── COMPONENT 5: New Seller Penalty (weight 5%) ─────────────
# sellers registered less than 6 months ago
df = df.withColumn("days_active",
    datediff(current_timestamp().cast("date"),
             col("registration_date").cast("date"))
)
df = df.withColumn("new_seller_component",
    when(col("days_active") < 180, 5.0)
    .when(col("days_active") < 365, 2.5)
    .otherwise(0.0)
)

# ── FINAL RISK INDEX (0-100) ─────────────────────────────────
df = df.withColumn("risk_index",
    col("return_component") +
    col("complaint_volume_component") +
    col("complaint_severity_component") +
    col("trust_component") +
    col("new_seller_component")
)

# ── RAW RISK SCORE (kept for reference) ─────────────────────
df = df.withColumn("risk_score",
    (col("return_rate") * 40) +
    (col("high_severity_complaints") * 15) +
    when(col("verification_status") == "unverified", 20).otherwise(0) +
    when(col("total_complaints") > 3, 15).otherwise(0) +
    when(col("total_orders") > 100, -10).otherwise(0)
)

# ── RISK LABEL based on new index ───────────────────────────
df = df.withColumn("risk_label",
    when(col("risk_index") >= 55, "HIGH")
    .when(col("risk_index") >= 30, "MEDIUM")
    .otherwise("LOW")
)

# ── FRAUD PATTERNS ───────────────────────────────────────────
fraud_df = df.filter(
    (col("return_rate") > 0.3) |
    (col("high_severity_complaints") >= 2) |
    ((col("verification_status") == "unverified") &
     (col("total_complaints") > 2))
).select(
    col("seller_id"),
    when(col("return_rate") > 0.5, "HIGH_RETURN_RATE")
    .when(col("high_severity_complaints") >= 2,
          "REPEATED_HIGH_COMPLAINTS")
    .otherwise("UNVERIFIED_WITH_COMPLAINTS").alias("pattern_type"),
    col("risk_score").alias("indicator_value"),
    lit(30.0).alias("threshold"),
    current_timestamp().alias("flagged_at"),
    when(col("return_rate") > 0.5,
         "Return rate exceeds 50% threshold")
    .when(col("high_severity_complaints") >= 2,
          "Multiple high severity complaints detected")
    .otherwise("Unverified seller with multiple complaints")
    .alias("explanation")
)

# ── WRITE CURATED LAYER ──────────────────────────────────────
df.select(
    "seller_id", "seller_name", "region", "verification_status",
    "total_orders", "total_returns", "return_rate",
    "total_complaints", "high_severity_complaints",
    "avg_order_amount", "risk_score", "risk_index", "risk_label"
).write.mode("overwrite") \
 .parquet("hdfs://namenode:9000/data/curated/seller_risk/")

fraud_df.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/fraud_patterns/")

print("Done: risk_scoring")
spark.stop()
