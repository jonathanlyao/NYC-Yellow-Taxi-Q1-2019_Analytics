# NYC Yellow Taxi Q1 2019 – Operations & Performance Analytics
This project analyzes the operational performance of NYC Yellow Taxi services during Q1 2019, using a full end-to-end analytics pipeline.
Instead of a simple notebook-based analysis, the project focuses on building a reproducible analytics warehouse pipeline and a semantic BI model that can support real analytical workloads.
The pipeline covers the entire process from raw data ingestion → warehouse modeling → analytics dashboard, with particular attention to data quality issues, anomaly detection, and star schema design.
The goal of this project is to demonstrate practical skills in:
•	Data Engineering
•	Analytics Engineering
•	Data Warehouse Modeling
•	BI Analytics and Business Insight Generation
________________________________________
Project Architecture
The project follows a modern analytics engineering workflow.

<img width="523" height="1729" alt="data pipeline diagram-2" src="https://github.com/user-attachments/assets/5bfb582f-008d-447c-9e86-ddd58e568dce" />

Pipeline stages:
1.	Data ingestion
DuckDB directly reads remote Parquet files from the NYC Taxi public dataset.
2.	Data storage
Cleaned datasets are loaded into a PostgreSQL data warehouse.
3.	Data transformation
dbt is used to implement layered transformations and dimensional modeling.
4.	Analytics layer
Power BI connects to the mart tables to build a semantic model and dashboards.
________________________________________
Architecture stack:
•	DuckDB – efficient remote Parquet ingestion
•	PostgreSQL – local data warehouse
•	dbt – layered transformation & modeling
•	Docker – reproducible environment
•	Power BI – semantic layer and visualization
________________________________________
Total dataset size:
•	22,611,788 taxi trips 
•	NYC Yellow Taxi
•	January – March 2019
________________________________________
Data Warehouse Design
The warehouse follows a layered modeling approach implemented in dbt.
raw -> staging -> core -> mart
Raw Layer
Stores raw NYC taxi trip data ingested from Parquet files without transformation.

Staging Layer
Handles:
•	schema normalization
•	type conversion
•	timestamp filtering
•	basic data validation
Key transformations include:
•	filtering timestamps outside Q1 2019 (01/01/2019 – 03/31/2019)
•	removing impossible timestamp orders
•	standardizing column naming (mapping)

Core Layer
Core business logic is implemented here.
Key derived metrics:
•	trip_duration_hours
•	avg_speed_mph
•	trip validity flags
•	clean_trip flag
Example data quality logic:
•	invalid trip duration
(dropoff time ≤ pickup time)
•	abnormal speeds
(< 0.1 mph or > 80 mph)
Instead of deleting these rows, they are flagged and tracked, allowing downstream analysis of data quality.

Mart Layer
Two fact tables are designed to support different analytical workloads.
Trip-Level Fact Table: mart_tripdata_joined_2019_q1
Contains full trip-level data enriched with dimensions such as:
•	pickup zone
•	dropoff zone
•	payment type
Used for:
•	geographic analysis
•	payment analysis
•	operational efficiency studies
Total rows: 22,611,788
Daily Aggregation Fact Table: mart_daily_trip_metrics 
Aggregates daily operational metrics such as:
•	total trips
•	total revenue
•	weighted average speed
•	clean trip ratio
Total rows: 90 days (Q1, 2019)
This table powers executive-level KPI dashboards.
Star Schema Model
The final semantic model follows a clean star schema design.
Fact tables:
mart_tripdata_joined_2019_q1
mart_daily_trip_metrics
Dimension tables:
dim_date
dim_zone
dim_payment_type
Design principles used in the model:
•	no fact-to-fact joins
•	single-direction filtering
•	one-to-many relationships
•	no many-to-many joins
•	no bidirectional filters
This structure keeps the BI model simple, interpretable, and scalable.
Data Quality Handling
Real-world datasets always contain anomalies.
Several issues were discovered during this project.
Timestamp anomalies
Some records contained pickup timestamps outside the expected year range (e.g., 2001, 2003, 2018).
These records caused BI time-series charts to collapse because the axis spanned decades instead of months.
Solution:
•	enforce strict Q1 2019 timestamp filters in the mart layer
•	ensure pickup_ts within valid date range

Duration anomalies
Trips where: dropoff_time ≤ pickup_time
were marked as invalid.
Trips longer than 5 hours were treated as abnormal outliers.
Speed outliers
Average trip speed was calculated as: distance / duration
Outliers were flagged using: 0.1 mph ≤ speed ≤ 80 mph
Approximately 49,000 rows (roughly 0.2% of data) were identified as abnormal.
These rows were flagged instead of removed, enabling data quality tracking.
________________________________________
Analytics Dashboard
The Power BI dashboard is organized into seven analytical pages.
Page 1 - Executive Overview
Purpose:
Provide a 30-second summary of Q1 performance.
Includes:
•	KPI cards (Total Trips, Revenue, Avg Speed, Clean Ratio)
•	Daily revenue trend
•	Weekday vs Weekend comparison
•	Holiday impact comparison

Page 2 - Revenue & Payment Structure
Focus:
Revenue composition and customer behavior.
Includes:
•	Revenue by payment type
•	Trip volume by payment type
•	Revenue per trip
•	Tip rate comparison
Key modeling lesson:
Revenue per trip must be calculated from the trip-level fact table, not daily aggregates.

Page 3 - Geographic Analysis
Focus:
Spatial demand and revenue distribution.
Includes:
•	Revenue by borough
•	Top pickup zones
•	Top dropoff zones
•	Distance vs Revenue bubble chart
Insight example:
Manhattan has high volume but lower revenue per trip,
while outer boroughs show higher per-trip revenue but lower volume.

Page 4 - Operational Efficiency
Focus:
Efficiency and trip structure.
Includes:
•	Weighted average speed by borough
•	Clean trip percentage
•	Duration bucket analysis
•	Revenue efficiency by duration
Key observation:
Short trips dominate volume,
but longer trips contribute disproportionately to revenue per trip.
________________________________________
Page 5 - Time & Seasonality
Focus:
Temporal patterns.
Includes:
•	Daily trend
•	Monthly comparison
•	Weekend effect
________________________________________
Page 6 - Policy Impact (Congestion Surcharge)
Policy introduced: February 1, 2019
Analysis includes:
•	Revenue change before vs after policy
•	Trip volume impact
•	Borough-level sensitivity
Manhattan shows the most visible structural change.
________________________________________
Page 7 - Data Quality & Modeling
Focus:
Transparency of engineering decisions.
Includes:
•	Clean trip ratio
•	Speed outlier percentage
•	Timestamp anomaly handling
•	Explanation of star schema design
This page exists to show engineering discipline behind the visuals.

Key Metrics (Q1 2019)
Total Trips        ~23 million
Total Revenue      ~$357 million
Avg Revenue/Trip   $15.63
Weighted Avg Speed 12.66 mph
Clean Data Ratio   99.89%
These figures were derived after applying data validation and anomaly filtering logic.

Key Challenges & Lessons Learned
Several real-world issues appeared during the project.
Time-axis distortion
A small number of corrupted timestamps caused time-series visualizations to appear flat for February and March.
The issue was traced to historical timestamp pollution in the dataset and fixed through defensive filtering in the mart layer.
________________________________________
Scatter plot overplotting
Initial scatter plots attempted to plot 22M individual trips, resulting in a dense visual "noise cloud".
Solution:
•	aggregate data before visualization
•	group by borough
•	use bubble size to represent trip volume
________________________________________
Semantic modeling matters
Early calculations mistakenly used the daily metrics table to compute metrics requiring payment_type dimensions.
Correct solution: use the trip-level fact table for dimensional analysis.
This reinforced the importance of choosing the correct grain for analysis.
________________________________________
Tech Stack
DuckDB
PostgreSQL
dbt
Docker
Power BI
________________________________________
How to Reproduce This Project Locally
This section explains how to run the project locally and rebuild the analytics pipeline.
1 Install Dependencies
You will need:
•	Docker
•	PostgreSQL
•	DuckDB
•	dbt
•	Power BI Desktop
________________________________________
2 Start the Database
Run the PostgreSQL container: docker compose up -d
This launches the local data warehouse.
________________________________________
3 Ingest Taxi Data
Use DuckDB to read Parquet files and load them into PostgreSQL.
Example:
INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=taxi_db user=postgres password=postgres host=localhost' 
AS taxi_db (TYPE POSTGRES);

CREATE TABLE taxi_db.raw.yellow_tripdata_2019_01 AS
SELECT *
FROM read_parquet(
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet'
);
Repeat for:
•	February 2019
•	March 2019
Total rows loaded:
22,611,788
________________________________________
4 Run dbt Transformations
Navigate to the dbt project directory:
cd dbt_project
Run the models:
dbt run
Optional:
dbt test
This will build the full warehouse structure:
raw → staging → core → mart
________________________________________
5 Connect Power BI
Open the Power BI dashboard file:
dashboards/NYC_Taxi_PowerBI.pbix
Connect to the PostgreSQL database and refresh the model.
The Power BI report will automatically load:
•	trip-level fact table
•	daily KPI metrics
•	dimension tables
________________________________________
Future Improvements
Possible extensions for this project:
•	migrate warehouse to Snowflake / BigQuery
•	orchestrate pipeline with Airflow
•	deploy dashboards via Power BI Service
•	build automated data quality tests with dbt
________________________________________
Final Thoughts
This project demonstrates how a public dataset can be transformed into a structured analytics warehouse and BI model.
Beyond visualization, the focus is on:
•	reproducible pipelines
•	layered data modeling
•	star-schema design
•	data quality handling
•	analytical storytelling

Why This Project Matters
This project goes beyond visualization. It demonstrates how raw operational data can be transformed into:
•	A structured warehouse
•	A dimensional data model
•	A reliable semantic layer
•	Actionable operational insights
The focus is not only on metrics, but on building a clean and scalable analytical foundation.









