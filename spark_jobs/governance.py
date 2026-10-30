"""Data governance checks: how much each layer cleans, and whether customer IDs stay masked.

Writes one row per check to /data/curated/data_quality (append-only, like risk_history),
so the Governance page can show the latest results.
"""
from datetime import datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, length

spark = SparkSession.builder.appName("Governance").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://namenode:9000/data"
DATASETS = {"orders": "order_id", "returns": "return_id", "complaints": "complaint_id"}
checked_at = datetime.now().replace(microsecond=0)
rows = []


def add(dataset, metric, value, status, explanation):
    rows.append(Row(checked_at=checked_at, dataset=dataset, metric=metric,
                    value=int(value), status=status, explanation=explanation))


def check(dataset, metric, value, explanation):
    add(dataset, metric, value, "pass" if value == 0 else "fail", explanation)


# ── 1. Cleaning funnel per dataset: raw → refined ─────────────
for dataset, key in DATASETS.items():
    raw = spark.read.json(f"{HDFS}/raw/{dataset}/")
    refined = spark.read.parquet(f"{HDFS}/refined/{dataset}/")
    raw_rows = raw.count()
    raw_with_key = raw.filter(col(key).isNotNull()).count()
    raw_unique = raw.filter(col(key).isNotNull()).select(key).distinct().count()
    refined_rows = refined.count()
    duplicates = raw_with_key - raw_unique

    add(dataset, "raw_rows", raw_rows, "info", "Rows received in HDFS raw (all Airflow files, kept as audit trail)")
    add(dataset, "duplicates_removed", duplicates, "info", "Copies of the same event removed by Spark")
    add(dataset, "invalid_removed", max(raw_rows - duplicates - refined_rows, 0), "info",
        "Rows dropped for a missing ID or invalid value (for example an amount outside 0-10,000)")
    add(dataset, "refined_rows", refined_rows, "info", "Clean rows in HDFS refined")
    check(dataset, "empty_keys_in_refined",
          refined.filter(col(key).isNull() | col("seller_id").isNull()).count(),
          "Refined rows without an ID or a seller")
    check(dataset, "duplicate_keys_in_refined",
          refined_rows - refined.select(key).distinct().count(),
          "IDs that still appear more than once in refined")

# ── 2. Referential integrity: every return should belong to an order of the same seller ──
orders = spark.read.parquet(f"{HDFS}/refined/orders/")
returns = spark.read.parquet(f"{HDFS}/refined/returns/")
orphans = returns.join(orders.select("order_id", "seller_id"), ["order_id", "seller_id"], "left_anti").count()
add("returns", "orphan_returns", orphans, "warning" if orphans else "pass",
    "Returns that match no order of the same seller (can't be traced to a customer)")

# ── 3. Privacy: no real customer ID may reach the curated layer ──
activity = spark.read.parquet(f"{HDFS}/curated/customer_activity/")
check("customer_activity", "unmasked_customer_ids",
      activity.filter((length("customer_masked") != 64) | col("customer_masked").rlike("^C[0-9]+$")).count(),
      "Customer IDs in curated that are not a 64-character SHA-256 hash")

spark.createDataFrame(rows).write.mode("append").parquet(f"{HDFS}/curated/data_quality/")
failed = sum(1 for r in rows if r.status == "fail")
print(f"Done: governance ({len(rows)} checks, {failed} failed)")
spark.stop()
