CREATE OR REPLACE TABLE dim_engine AS
SELECT
    monotonically_increasing_id() AS engine_key,
    engine_id,
    model_name,
    build_year
FROM silver_engines;