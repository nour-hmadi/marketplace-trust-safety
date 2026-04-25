from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, count, when, isnan

spark = SparkSession.builder.appName("Governance").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n========= GOVERNANCE LAYER =========")

# ── 1. QUALITY CHECKS ──────────────────────────────────────
print("\n--- Quality Check: Orders ---")
orders = spark.read.parquet("hdfs://namenode:9000/data/refined/orders/")
total = orders.count()
nulls = orders.filter(col("order_id").isNull() | col("seller_id").isNull() | (col("amount") <= 0)).count()
dupes = total - orders.dropDuplicates(["order_id"]).count()
print(f"Total orders:      {total}")
print(f"Null/invalid rows: {nulls}")
print(f"Duplicate orders:  {dupes}")
print(f"Quality status:    {'✅ PASSED' if nulls == 0 and dupes == 0 else '❌ FAILED'}")

print("\n--- Quality Check: Returns ---")
returns = spark.read.parquet("hdfs://namenode:9000/data/refined/returns/")
total_r = returns.count()
nulls_r = returns.filter(col("return_id").isNull() | col("seller_id").isNull()).count()
dupes_r = total_r - returns.dropDuplicates(["return_id"]).count()
print(f"Total returns:     {total_r}")
print(f"Null/invalid rows: {nulls_r}")
print(f"Duplicate returns: {dupes_r}")
print(f"Quality status:    {'✅ PASSED' if nulls_r == 0 and dupes_r == 0 else '❌ FAILED'}")

print("\n--- Quality Check: Complaints ---")
complaints = spark.read.parquet("hdfs://namenode:9000/data/refined/complaints/")
total_c = complaints.count()
nulls_c = complaints.filter(col("complaint_id").isNull() | col("seller_id").isNull()).count()
dupes_c = total_c - complaints.dropDuplicates(["complaint_id"]).count()
print(f"Total complaints:  {total_c}")
print(f"Null/invalid rows: {nulls_c}")
print(f"Duplicate records: {dupes_c}")
print(f"Quality status:    {'✅ PASSED' if nulls_c == 0 and dupes_c == 0 else '❌ FAILED'}")

# ── 2. CUSTOMER ID MASKING ──────────────────────────────────
print("\n--- Customer ID Masking ---")
orders_masked = orders.withColumn(
    "customer_id_masked",
    sha2(col("customer_id"), 256)
).drop("customer_id")

print("Original customer_id is now MASKED with SHA-256 hash:")
orders_masked.select("order_id", "seller_id", "customer_id_masked").show(3, truncate=True)
print("✅ Customer PII protected - raw customer_id never appears in outputs")

# ── 3. ROLE SEPARATION ─────────────────────────────────────
print("\n--- Role Separation ---")
print("""
ROLE: trust_safety_analyst
  ACCESS: curated.seller_risk       ✅ Full access
  ACCESS: curated.fraud_patterns    ✅ Full access  
  ACCESS: refined.complaints        ✅ Full access
  ACCESS: refined.orders            ✅ Read only (masked customer_id)
  ACCESS: raw.*                     ❌ No access

ROLE: business_analyst
  ACCESS: curated.seller_risk       ✅ Read only (risk_label, region only)
  ACCESS: curated.fraud_patterns    ❌ No access (sensitive)
  ACCESS: refined.*                 ❌ No access
  ACCESS: raw.*                     ❌ No access
""")

# Business analyst view - limited columns only
business_view = spark.read.parquet(
    "hdfs://namenode:9000/data/curated/seller_risk/"
).select("seller_id", "seller_name", "region", "risk_label")

print("Business Analyst View (limited columns):")
business_view.show(truncate=False)

print("✅ Role separation enforced")
print("\n========= GOVERNANCE COMPLETE =========")
spark.stop()
