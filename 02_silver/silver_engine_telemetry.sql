CREATE OR REPLACE TABLE silver_engine_telemetry AS WITH sorszamozott_adatok AS (
    SELECT 
        engine_id, 
        loc_id,
    event_timestamp::timestamp as event_timestamp,
    sensor_payload:temp::double AS temp,
    sensor_payload:vibration::double AS vibration,
    sensor_payload:status_code::string AS status_code,
    ROW_NUMBER() OVER(PARTITION BY engine_id, event_timestamp,loc_id ORDER BY event_timestamp) AS rn
    FROM raw_telemetry
    WHERE engine_id IS NOT NULL
)
SELECT * FROM sorszamozott_adatok 
WHERE rn = 1
AND temp BETWEEN 200 AND 800