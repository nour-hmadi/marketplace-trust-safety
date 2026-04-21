# Architecture Documentation
## Marketplace Trust & Safety Analytics Platform

---

## Overview
This platform ingests, transforms, and governs marketplace data
to help trust-and-safety analysts identify risky sellers and
suspicious transaction patterns.

---

## Components

### 1. Ingestion Layer
- **Kafka** handles streaming ingestion of orders and returns
- **Airflow batch DAGs** load seller profiles and complaints from JSON files
- All raw data lands in **HDFS raw layer** unchanged

### 2. Storage Layer (HDFS - 3 Layers)
| Layer | Path | Purpose |
|-------|------|---------|
| Raw | /data/raw/ | Exact copy of source data, never modified |
| Refined | /data/refined/ | Cleaned, normalized, typed data |
| Curated | /data/curated/ | Analyst-ready risk outputs |

### 3. Processing Layer
- **PySpark** jobs handle all transformations between layers
- Jobs are modular: normalize → aggregate → score → publish

### 4. Metadata Layer
- **Hive Metastore** registers all curated tables
- Analysts can query tables by name without knowing HDFS paths

### 5. Orchestration Layer
- **Airflow** coordinates all ingestion, transformation,
  validation, and publication steps
- DAGs run on schedules and depend on each other

### 6. Consumption Layer
- **Jupyter Notebooks** for analyst exploration
- **SQL queries** for investigation workflows

---

## Data Flow

---

## Technology Choices
- **Kafka**: chosen for real-time order and return event streaming
- **HDFS**: chosen for scalable distributed storage across all layers
- **Spark**: chosen for distributed data processing and transformations
- **Hive**: chosen for SQL queryability and metadata management
- **Airflow**: chosen for reliable orchestration and scheduling
- **Docker**: chosen to make the entire platform reproducible