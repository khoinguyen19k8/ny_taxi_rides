-- Create the external table
CREATE
OR REPLACE EXTERNAL TABLE `dtc-de-372302.nytaxi.external_fhv_tripdata_2019` OPTIONS (
    FORMAT = 'PARQUET',
    uris = ['gs://dtc_data_lake_dtc-de-372302/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- Create a native table
CREATE
OR REPLACE TABLE `dtc-de-372302.nytaxi.fhv_tripdata_2019` AS
SELECT
    *
FROM
    `dtc-de-372302.nytaxi.external_fhv_tripdata_2019`;

-- Question 1
SELECT
    COUNT(*)
FROM
    `dtc-de-372302.nytaxi.external_fhv_tripdata_2019`;

-- Question 2
SELECT
    COUNT(DISTINCT Affiliated_base_number)
FROM
    `dtc-de-372302.nytaxi.external_fhv_tripdata_2019`;

SELECT
    COUNT(DISTINCT Affiliated_base_number)
FROM
    `dtc-de-372302.nytaxi.fhv_tripdata_2019`;

-- Question 3
SELECT
    COUNT(*)
FROM
    `dtc-de-372302.nytaxi.external_fhv_tripdata_2019`
WHERE
    PUlocationID = -999
    AND DOlocationID = -999;

-- Question 4
CREATE
OR REPLACE TABLE `dtc-de-372302.nytaxi.partitioned_fhv_tripdata_2019` PARTITION BY DATE(pickup_datetime) CLUSTER BY affiliated_base_number AS
SELECT
    *
FROM
    `dtc-de-372302.nytaxi.external_fhv_tripdata_2019`;

SELECT
    COUNT(DISTINCT Affiliated_base_number)
FROM
    `dtc-de-372302.nytaxi.partitioned_fhv_tripdata_2019`
WHERE
    pickup_datetime BETWEEN '2019-03-01'
    AND '2019-03-31';

SELECT
    COUNT(DISTINCT Affiliated_base_number)
FROM
    `dtc-de-372302.nytaxi.fhv_tripdata_2019`
WHERE
    pickup_datetime BETWEEN '2019-03-01'
    AND '2019-03-31';