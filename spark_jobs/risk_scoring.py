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
# One row per (seller, pattern). indicator_value = the metric that triggered the rule,
# threshold = that rule's limit. Rates per order keep the rules fair for small and big sellers.
from datetime import datetime

scored_at = datetime.now().replace(microsecond=0)   # one timestamp for the whole run

df = df.withColumn("high_severity_per_order",
    when(col("total_orders") > 0, col("high_severity_complaints") / col("total_orders")).otherwise(0.0)
)


def pattern(condition, name, indicator, threshold, explanation):
    return df.filter(condition).select(
        col("seller_id"),
        lit(name).alias("pattern_type"),
        col(indicator).cast("double").alias("indicator_value"),
        lit(float(threshold)).alias("threshold"),
        lit(scored_at).alias("flagged_at"),
        lit(explanation).alias("explanation"),
    )


fraud_df = pattern(col("return_rate") > 0.30, "HIGH_RETURN_RATE", "return_rate", 0.30,
                   "More than 30% of orders are returned") \
    .unionByName(pattern(col("high_severity_per_order") > 0.10, "FREQUENT_HIGH_SEVERITY_COMPLAINTS",
                         "high_severity_per_order", 0.10,
                         "More than 1 in 10 orders leads to a high-severity complaint")) \
    .unionByName(pattern((col("verification_status") == "unverified") & (col("complaint_rate") > 0.10),
                         "UNVERIFIED_WITH_COMPLAINTS", "complaint_rate", 0.10,
                         "Unverified seller, and more than 1 in 10 orders gets a complaint"))

# ── WRITE CURATED LAYER ──────────────────────────────────────
seller_risk = df.select(
    "seller_id", "seller_name", "region", "verification_status",
    "total_orders", "total_returns", "return_rate",
    "total_complaints", "high_severity_complaints",
    "avg_order_amount", "risk_score", "risk_index", "risk_label"
).withColumn("scored_at", lit(scored_at))

seller_risk.write.mode("overwrite").parquet("hdfs://namenode:9000/data/curated/seller_risk/")
fraud_df.write.mode("overwrite").parquet("hdfs://namenode:9000/data/curated/fraud_patterns/")

# ── RISK HISTORY (append-only): one row per seller per run, for trend charts ──
seller_risk.select("scored_at", "seller_id", "seller_name", "risk_index", "risk_label",
                   "total_orders", "return_rate") \
    .write.mode("append").parquet("hdfs://namenode:9000/data/curated/risk_history/")

print(f"Done: risk_scoring ({fraud_df.count()} fraud patterns flagged)")
spark.stop()
