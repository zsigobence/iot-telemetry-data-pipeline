CREATE OR REPLACE TABLE silver_locations AS
SELECT 
    loc_id, 
    country, 
    city
FROM raw_locations
WHERE loc_id IS NOT NULL;