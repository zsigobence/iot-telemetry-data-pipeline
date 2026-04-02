CREATE OR REPLACE TABLE silver_maint_types AS
SELECT 
    maint_type_id, 
    description, 
    interval_hours
FROM raw_maint_types
WHERE maint_type_id IS NOT NULL;