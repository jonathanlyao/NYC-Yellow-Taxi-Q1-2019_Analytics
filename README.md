# NYC Yellow Taxi Q1 2019 – Operations & Performance Analytics Engineering

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytics%20Engine-yellow)
![dbt](https://img.shields.io/badge/dbt-Data%20Transformation-orange)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-F2C811)
![Data Engineering](https://img.shields.io/badge/Data%20Engineering-Pipeline-green)

This project analyzes the operational performance of NYC Yellow Taxi services during Q1 2019 using a full end-to-end analytics pipeline.

Instead of a simple notebook-based analysis, the project focuses on building a reproducible analytics warehouse pipeline and a semantic BI model that can support real analytical workloads.

The pipeline covers the entire process from:

raw data ingestion → warehouse modeling → analytics dashboard

with particular attention to:

- data quality issues
- anomaly detection
- star schema design

---

# Project Architecture

The project follows a modern analytics engineering workflow.

<img width="3400" height="527" alt="ETL diagram" src="https://github.com/user-attachments/assets/fc5c7895-fbf0-46a6-aaca-d1a4bcd57e30" />

## Pipeline stages

### 1. Data ingestion

DuckDB directly reads remote Parquet files from the NYC Taxi public dataset.

### 2. Data storage

Cleaned datasets are loaded into a PostgreSQL data warehouse.

### 3. Data transformation

dbt is used to implement layered transformations and dimensional modeling.

### 4. Analytics layer

Power BI connects to the mart tables to build a semantic model and dashboards.


---

# Architecture Stack

- DuckDB – efficient remote Parquet ingestion
- PostgreSQL – local data warehouse
- dbt – layered transformation and modeling
- Docker – reproducible environment
- Power BI – semantic layer and visualization


---

# Total Dataset Size

- 22,611,788 taxi trips
- NYC Yellow Taxi dataset
- January – March 2019


---

# Data Warehouse Design

The warehouse follows a layered modeling approach implemented in dbt.


raw → staging → core → mart



## Raw Layer

Stores raw NYC taxi trip data ingested from Parquet files without transformation.


## Staging Layer

Handles:

- schema normalization
- type conversion
- timestamp filtering
- basic data validation

Key transformations include:

- filtering timestamps outside Q1 2019 (01/01/2019 – 03/31/2019)
- removing impossible timestamp orders
- standardizing column naming (mapping)


## Core Layer

Core business logic is implemented here.

Key derived metrics:

- trip_duration_hours
- avg_speed_mph
- trip validity flags
- clean_trip flag

Example data quality logic:

- invalid trip duration  
  (dropoff time ≤ pickup time)

- abnormal speeds  
  (< 0.1 mph or > 80 mph)

Instead of deleting these rows, they are flagged and tracked, allowing downstream analysis of data quality.


## Mart Layer

Two fact tables are designed to support different analytical workloads.


### Trip-Level Fact Table

`mart_tripdata_joined_2019_q1`

Contains full trip-level data enriched with dimensions such as:

- pickup zone
- dropoff zone
- payment type

Used for:

- geographic analysis
- payment analysis
- operational efficiency studies

Total rows:

22,611,788


### Daily Aggregation Fact Table

`mart_daily_trip_metrics`

Aggregates daily operational metrics such as:

- total trips
- total revenue
- weighted average speed
- clean trip ratio

Total rows:

90 days (Q1 2019)

This table powers executive-level KPI dashboards.


---

# Star Schema Model

The final semantic model follows a clean star schema design.


## Fact tables

- mart_tripdata_joined_2019_q1
- mart_daily_trip_metrics


## Dimension tables

- dim_date
- dim_zone
- dim_payment_type


## Design principles

- no fact-to-fact joins
- single-direction filtering
- one-to-many relationships
- no many-to-many joins
- no bidirectional filters

This structure keeps the BI model simple, interpretable, and scalable.


---

# Data Quality Handling

Real-world datasets always contain anomalies.

Several issues were discovered during this project.


## Timestamp anomalies

Some records contained pickup timestamps outside the expected year range (e.g., 2001, 2003, 2018).

These records caused BI time-series charts to collapse because the axis spanned decades instead of months.

Solution:

- enforce strict Q1 2019 timestamp filters in the mart layer
- ensure pickup_ts within valid date range


## Duration anomalies

Trips where:


dropoff_time ≤ pickup_time


were marked as invalid.

Trips longer than 5 hours were treated as abnormal outliers.


## Speed outliers

Average trip speed was calculated as:


distance / duration


Outliers were flagged using:


0.1 mph ≤ speed ≤ 80 mph


Approximately 49,000 rows (~0.2%) were identified as abnormal.

These rows were flagged instead of removed, enabling data quality tracking.


---

# Analytics Dashboard

The Power BI dashboard is organized into seven analytical pages.


## Page 1 — Executive Overview

![dashboard_overview](https://github.com/user-attachments/assets/6cb88093-2361-4413-a7ef-c47242d777d6)

Purpose:

Provide a 30-second summary of Q1 performance.

Includes:

- KPI cards (Total Trips, Revenue, Avg Speed, Clean Ratio)
- Daily revenue trend
- Weekday vs Weekend comparison
- Holiday impact comparison


## Page 2 — Revenue & Payment Structure

![revenue_payment_analysis](https://github.com/user-attachments/assets/b8ca4c68-dcca-4a6a-9bb1-61b8d6f8d0b7)

Focus:

Revenue composition and customer behavior.

Includes:

- revenue by payment type
- trip volume by payment type
- revenue per trip
- tip rate comparison

Key modeling lesson:

Revenue per trip must be calculated from the trip-level fact table, not daily aggregates.


## Page 3 — Geographic Analysis

![geographic_analysis](https://github.com/user-attachments/assets/b5abac25-9abf-40fe-908f-b0dc1246cc2c)

Focus:

Spatial demand and revenue distribution.

Includes:

- revenue by borough
- top pickup zones
- top dropoff zones
- distance vs revenue bubble chart


## Page 4 — Operational Efficiency

Focus:

Efficiency and trip structure.

Includes:

- weighted average speed by borough
- clean trip percentage
- duration bucket analysis
- revenue efficiency by duration


## Page 5 — Time & Seasonality

Focus:

Temporal patterns.

Includes:

- daily trend
- monthly comparison
- weekend effect


## Page 6 — Policy Impact (Congestion Surcharge)

Policy introduced:

February 1, 2019

Analysis includes:

- revenue change before vs after policy
- trip volume impact
- borough-level sensitivity


## Page 7 — Data Quality & Modeling

Focus:

Transparency of engineering decisions.

Includes:

- clean trip ratio
- speed outlier percentage
- timestamp anomaly handling
- explanation of star schema design


---

# Key Metrics (Q1 2019)

| Metric | Value |
|------|------|
| Total Trips | ~23 million |
| Total Revenue | ~$357 million |
| Avg Revenue/Trip | $15.63 |
| Weighted Avg Speed | 12.66 mph |
| Clean Data Ratio | 99.89% |


---

# Tech Stack

- DuckDB
- PostgreSQL
- dbt
- Docker
- Power BI


---
## Repository Structure

![repository structure](https://github.com/user-attachments/assets/584211d6-c502-4b82-8bcd-2d9009abbdc3)

**Folder description**

- **images/** – dashboard preview screenshots used in the README  
- **ingestion/** – data ingestion scripts (DuckDB → PostgreSQL)  
- **sql/** – warehouse SQL logic and queries  
- **dbt_project/** – dbt models implementing the layered transformation pipeline

---
## How to Reproduce This Project

This section describes how to run the analytics pipeline locally and rebuild the data warehouse and dashboards.

### 1. Install Dependencies

Make sure the following tools are installed:

- Docker
- PostgreSQL
- DuckDB
- dbt
- Power BI Desktop

---

### 2. Start the PostgreSQL Data Warehouse

Create a local PostgreSQL container to act as the analytics warehouse.

Example `docker-compose.yml`:

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:15
    container_name: nyc_taxi_postgres
    restart: always
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: taxi_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Start the container:

```bash
docker compose up -d
```

Verify the container is running:

```bash
docker ps
```

---

## 3. Create Database Schemas

Connect to PostgreSQL and create the schemas used in the warehouse.

```sql
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA core;
CREATE SCHEMA mart;
```

These schemas correspond to the layered warehouse design used in the project:

```
raw → staging → core → mart
```

---

### 4. Ingest Taxi Data with DuckDB

Use DuckDB to read Parquet files directly from the NYC Taxi public dataset and load them into PostgreSQL.

Example:

```sql
INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=taxi_db user=postgres password=postgres host=localhost'
AS taxi_db (TYPE POSTGRES);

CREATE TABLE taxi_db.raw.yellow_tripdata_2019_01 AS
SELECT *
FROM read_parquet(
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet'
);
```

Repeat the process for:

- February 2019
- March 2019

Total rows loaded:

```
22,611,788 taxi trips
```

---

### 5. Run dbt Transformations

Navigate to the dbt project directory:

```bash
cd dbt_project
```

Run the dbt models:

```bash
dbt run
```

Optional: run data quality tests

```bash
dbt test
```

This step builds the warehouse layers:

```
raw → staging → core → mart
```

---

### 6. Open the Power BI Dashboard

Open the Power BI report file:

```
dashboards/NYC_Taxi_PowerBI.pbix
```

Connect Power BI to the PostgreSQL database and refresh the model.

The dashboard will load:

- trip-level fact table
- daily KPI metrics
- dimension tables
---

# Future Improvements

Possible extensions for this project:

- migrate warehouse to Snowflake / BigQuery
- orchestrate pipeline with Airflow
- deploy dashboards via Power BI Service
- build automated data quality tests with dbt


---

# Why This Project Matters

This project demonstrates how raw operational data can be transformed into:

- a structured warehouse
- a dimensional data model
- a reliable semantic layer
- actionable operational insights

The focus is not only on metrics, but on building a clean and scalable analytical foundation.









