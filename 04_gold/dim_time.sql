CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    event_hour,
    year(event_hour) AS year,
    month(event_hour) AS month,
    day(event_hour) AS day,
    hour(event_hour) AS hour
FROM fact_engine_events;