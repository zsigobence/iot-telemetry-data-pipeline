CREATE OR REPLACE TABLE silver_engines AS
SELECT 
    engine_id, 
    UPPER(model) AS model_name, 
    build_year
FROM raw_engines
WHERE engine_id IS NOT NULL;