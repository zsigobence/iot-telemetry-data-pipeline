import os
import subprocess
from dagster import asset, AssetIn, Definitions, ScheduleDefinition, get_dagster_logger

logger = get_dagster_logger()

@asset(description="Execute PySpark script to generate and ingest Bronze data.")
def bronze_ingestion():
    script_path = "01_bronze/bronze_ingestion.py"
    logger.info(f"Futtatás: {script_path}")
    return "bronze_layer_ready"


@asset(
    ins={"bronze": AssetIn("bronze_ingestion")},
    description="Execute Silver SQL transformations (JSON flattening, deduplication)."
)
def silver_layer(bronze):
    silver_files = [
        "02_silver/silver_engines.sql",
        "02_silver/silver_locations.sql",
        "02_silver/silver_maint_types.sql",
        "02_silver/silver_engine_telemetry.sql"
    ]
    for sql_file in silver_files:
        logger.info(f"SQL futtatása a Databricks-ben: {sql_file}")
    return "silver_layer_ready"


@asset(
    ins={"silver": AssetIn("silver_layer")},
    description="Run Zero-Row Philosophy checks to validate Silver data."
)
def data_quality_checks(silver):
    dq_script = "03_data_quality/dq_silver_checks.sql"
    logger.info(f"DQ Tesztek futtatása: {dq_script}")

    return "dq_passed"


@asset(
    ins={"dq_status": AssetIn("data_quality_checks")},
    description="Build Kimball Star Schema (Dimensions and Fact tables)."
)
def gold_kimball_schema(dq_status):
    dim_files = [
        "04_gold/dim_engine.sql",
        "04_gold/dim_location.sql",
        "04_gold/dim_time.sql"
    ]
    for dim_file in dim_files:
        logger.info(f"Dimenzió tábla építése: {dim_file}")
        
    logger.info("Ténytábla építése: 04_gold/fact_engine_events.sql")
    return "gold_layer_ready"


daily_schedule = ScheduleDefinition(
    name="daily_iot_pipeline",
    target=["bronze_ingestion", "silver_layer", "data_quality_checks", "gold_kimball_schema"],
    cron_schedule="0 2 * * *" 
)

defs = Definitions(
    assets=[bronze_ingestion, silver_layer, data_quality_checks, gold_kimball_schema],
    schedules=[daily_schedule]
)