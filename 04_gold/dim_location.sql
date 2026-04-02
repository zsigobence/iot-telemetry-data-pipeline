CREATE OR REPLACE TABLE dim_location AS
SELECT
    monotonically_increasing_id() AS location_key,
    loc_id,
    country,
    city
FROM silver_locations;