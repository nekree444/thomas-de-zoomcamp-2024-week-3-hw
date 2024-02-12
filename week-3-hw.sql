CREATE OR REPLACE EXTERNAL TABLE `nomadic-pipe-407907.week_3_hw.green-tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://thomasm-de-zoomcamp-mage/green-taxi-trip-records-2022.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `nomadic-pipe-407907.week_3_hw.green-tripdata-csv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://thomasm-de-zoomcamp-mage/green-taxi-trip-records-2022.csv']
);

SELECT count(*) FROM `nomadic-pipe-407907.week_3_hw.green-tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `week_3_hw.green-tripdata`;

SELECT COUNT(fare_amount) FROM `week_3_hw.green-tripdata` WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `nomadic-pipe-407907.week_3_hw.green-tripdata-partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `nomadic-pipe-407907.week_3_hw.green-tripdata-csv`
);

SELECT DISTINCT(PUlocationID) FROM `nomadic-pipe-407907.week_3_hw.green-tripdata`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'