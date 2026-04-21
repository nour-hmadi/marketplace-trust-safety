-- ============================================
-- RAW LAYER: stores data exactly as ingested
-- ============================================

CREATE DATABASE IF NOT EXISTS raw;

-- Raw Orders
CREATE EXTERNAL TABLE IF NOT EXISTS raw.orders (
    order_id    STRING,
    seller_id   STRING,
    customer_id STRING,
    amount      DOUBLE,
    timestamp   STRING,
    status      STRING,
    category    STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/raw/orders/';

-- Raw Returns
CREATE EXTERNAL TABLE IF NOT EXISTS raw.returns (
    return_id   STRING,
    order_id    STRING,
    seller_id   STRING,
    timestamp   STRING,
    reason      STRING,
    status      STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/raw/returns/';

-- Raw Complaints
CREATE EXTERNAL TABLE IF NOT EXISTS raw.complaints (
    complaint_id STRING,
    seller_id    STRING,
    timestamp    STRING,
    severity     STRING,
    category     STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/raw/complaints/';

-- Raw Sellers
CREATE EXTERNAL TABLE IF NOT EXISTS raw.sellers (
    seller_id           STRING,
    seller_name         STRING,
    registration_date   STRING,
    region              STRING,
    category            STRING,
    verification_status STRING,
    support_tier        STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data/raw/sellers/';