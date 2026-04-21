-- ============================================
-- CURATED LAYER: analyst-ready outputs
-- ============================================

CREATE DATABASE IF NOT EXISTS curated;

-- Seller Risk Summary
CREATE TABLE IF NOT EXISTS curated.seller_risk (
    seller_id               STRING,
    seller_name             STRING,
    region                  STRING,
    verification_status     STRING,
    total_orders            BIGINT,
    total_returns           BIGINT,
    return_rate             DOUBLE,
    total_complaints        BIGINT,
    high_severity_complaints BIGINT,
    avg_order_amount        DOUBLE,
    risk_score              DOUBLE,
    risk_label              STRING
)
STORED AS PARQUET
LOCATION '/data/curated/seller_risk/';

-- Fraud Patterns
CREATE TABLE IF NOT EXISTS curated.fraud_patterns (
    seller_id           STRING,
    pattern_type        STRING,
    indicator_value     DOUBLE,
    threshold           DOUBLE,
    flagged_at          TIMESTAMP,
    explanation         STRING
)
STORED AS PARQUET
LOCATION '/data/curated/fraud_patterns/';