SELECT count(*) as trips FROM `dtc-de-339016.trips_data_all.external_table_fhv_taxis_manual`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;

SELECT count(*) as trips FROM `dtc-de-339016.trips_data_all.external_table_fhv_taxis_manual`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-01-01';

SELECT COUNT(DISTINCT dispatching_base_num) FROM `dtc-de-339016.trips_data_all.external_table_fhv_taxis_manual`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;

CREATE OR REPLACE TABLE `dtc-de-339016.trips_data_all.external_table_fhv_taxis_partitoned_clustered`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-339016.trips_data_all.external_table_fhv_taxis_manual`;

SELECT count(*) as trips FROM `dtc-de-339016.trips_data_all.external_table_fhv_taxis_partitoned_clustered`
WHERE DATE(dropoff_datetime) BETWEEN  "2019-01-01" AND "2019-03-31" AND
(dispatching_base_num = "B00987" OR dispatching_base_num = "B02060" OR dispatching_base_num = "B02279");

