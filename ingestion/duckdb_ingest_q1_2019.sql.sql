-- DuckDB ingestion script for NYC Yellow Taxi Q1 2019
INSTALL postgres;
LOAD postgres;

$conn = "host=hostname port=port# user=username password=password dbname=databasename"
dbname=databasename

ATTACH '$conn'
AS taxi_db (TYPE POSTGRES);

CREATE TABLE taxi_db.raw.yellow_tripdata_2019_01 AS
SELECT *
FROM read_parquet(
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet'
);

-- Import dataset for Feb
INSTALL postgres; LOAD postgres;
ATTACH '$conn' AS taxi_db (TYPE POSTGRES);
CREATE TABLE IF NOT EXISTS taxi_db.raw.yellow_tripdata_2019_02 AS 
SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-02.parquet');

-- Import dataset for March
INSTALL postgres; LOAD postgres;
ATTACH '$conn' AS taxi_db (TYPE POSTGRES);
CREATE TABLE IF NOT EXISTS taxi_db.raw.yellow_tripdata_2019_03 AS 
SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-03.parquet');

