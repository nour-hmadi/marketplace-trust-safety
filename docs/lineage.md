# Data Lineage Documentation
## Marketplace Trust & Safety Analytics Platform

---

## Source to Raw

| Source | Format | Ingestion Method | Raw HDFS Path |
|--------|--------|-----------------|---------------|
| Kafka orders topic | JSON stream | Airflow + kafka-console-consumer | /data/raw/orders/ |
| Kafka returns topic | JSON stream | Airflow + kafka-console-consumer | /data/raw/returns/ |
| complaints.json | JSON batch | Airflow PythonOperator | /data/raw/complaints/ |
| sellers.json | JSON batch | Airflow PythonOperator | /data/raw/sellers/ |

---

## Raw to Refined

| Raw Path | Spark Job | Transformations Applied | Refined Path |
|----------|-----------|------------------------|--------------|
| /data/raw/orders/ | normalize_orders.py | timestamp cast, status lowercase, dedup, null filter | /data/refined/orders/ |
| /data/raw/returns/ | normalize_returns.py | timestamp cast, reason lowercase, dedup, null filter | /data/refined/returns/ |
| /data/raw/complaints/ | normalize_orders.py | timestamp cast, severity lowercase | /data/refined/complaints/ |
| /data/raw/sellers/ | normalize_orders.py | date cast, trim whitespace | /data/refined/sellers/ |

---

## Refined to Curated

| Refined Inputs | Spark Job | Logic Applied | Curated Output |
|----------------|-----------|---------------|----------------|
| orders, returns, complaints, sellers | seller_aggregates.py | group by seller, compute ratios | /data/refined/seller_aggregates/ |
| seller_aggregates | risk_scoring.py | weighted risk formula, fraud flags | /data/curated/seller_risk/ |
| seller_aggregates | risk_scoring.py | suspicious pattern detection | /data/curated/fraud_patterns/ |

---

## Risk Score Formula
risk_score =
(return_rate × 40)

(high_severity_complaints × 15)
(unverified seller → +20)
(total_complaints > 3 → +15)
(total_orders > 100 → -10)


| Score Range | Risk Label |
|-------------|------------|
| 60 and above | HIGH |
| 30 to 59 | MEDIUM |
| Below 30 | LOW |

---

## Governance Controls

| Control | Implementation |
|---------|---------------|
| Customer ID masking | customer_id never appears in curated layer |
| Role separation | trust_safety role can access curated, business role cannot |
| Duplicate detection | dropDuplicates() in all Spark normalization jobs |
| Null filtering | filter() on key fields in all normalization jobs |
| Audit trail | Raw layer is never modified, full history preserved |