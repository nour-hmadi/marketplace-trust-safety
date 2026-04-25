from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RegisterHiveTables") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n========= REGISTERING HIVE TABLES =========")

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS raw")
spark.sql("CREATE DATABASE IF NOT EXISTS refined")
spark.sql("CREATE DATABASE IF NOT EXISTS curated")
print("✅ Databases created: raw, refined, curated")

# Register curated seller_risk table
spark.read.parquet("hdfs://namenode:9000/data/curated/seller_risk/") \
    .write.mode("overwrite").saveAsTable("curated.seller_risk")
print("✅ Table registered: curated.seller_risk")

# Register curated fraud_patterns table
spark.read.parquet("hdfs://namenode:9000/data/curated/fraud_patterns/") \
    .write.mode("overwrite").saveAsTable("curated.fraud_patterns")
print("✅ Table registered: curated.fraud_patterns")

# Register refined tables
spark.read.parquet("hdfs://namenode:9000/data/refined/orders/") \
    .write.mode("overwrite").saveAsTable("refined.orders")
print("✅ Table registered: refined.orders")

spark.read.parquet("hdfs://namenode:9000/data/refined/sellers/") \
    .write.mode("overwrite").saveAsTable("refined.sellers")
print("✅ Table registered: refined.sellers")

# Show all registered tables
print("\n--- Registered Tables ---")
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES IN curated").show()
spark.sql("SHOW TABLES IN refined").show()

# Query the tables by name
print("\n--- Query by Table Name (like Hive) ---")
spark.sql("SELECT seller_id, seller_name, risk_label, risk_score FROM curated.seller_risk ORDER BY risk_score DESC").show(truncate=False)

print("\n✅ All tables registered in Hive Metastore!")
spark.stop()
