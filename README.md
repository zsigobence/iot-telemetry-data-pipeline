# IoT Telemetry Data Pipeline - Medallion Architecture

This repository contains an end-to-end industrial IoT data engineering pipeline. It processes high-frequency engine telemetry and maintenance data from raw ingestion to a BI-ready Kimball-style Data Warehouse.

## Architecture Overview
The project follows the **Medallion Architecture** managed by **Dagster** orchestration:

1.  **Bronze Layer**: Raw ingestion of nested JSON telemetry and master data using PySpark.
2.  **Silver Layer**: Flattening, type casting, and deduplication with SQL. Includes automated mapping of location and engine IDs.
3.  **Data Quality Gate**: A dedicated validation step implementing "Zero-Row Philosophy" to ensure data integrity before reaching the Gold layer.
4.  **Gold Layer**: A dimensional model (Kimball) featuring Surrogate Keys and a denormalized Fact table for high-performance BI reporting.

## Tech Stack
* **Orchestration:** Dagster (Software-Defined Assets)
* **Processing:** PySpark & Spark SQL
* **Environment:** Databricks
* **Modeling:** Kimball Star Schema (Fact & Dimension tables)
* **Quality Assurance:** SQL-based automated DQ checks

## Project Structure
* `01_bronze/`: PySpark ingestion scripts.
* `02_silver/`: SQL transformations and cleaning logic.
* `03_data_quality/`: Automated integrity tests (Uniqueness, Null-checks, Domain validation).
* `04_gold/`: Final Star Schema and OBT models.
* `orchestration.py`: Dagster pipeline definition and scheduling.

## How to Run Locally
1. Clone the repository:
   ```bash
   git clone [https://github.com/zsigobence/iot-telemetry-data-pipeline.git](https://github.com/zsigobence/iot-telemetry-data-pipeline.git)
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate # Windows: .\venv\Scripts\activate
   ```

3. Install Dagster:
   ```bash
   pip install dagster dagster-webserver
   ```

4. Start the Dagster development server:
   ```bash
   dagster dev -f orchestration.py
   ```

5. Open `http://localhost:3000` to visualize the asset graph and materialize the pipeline.

## Business Value
This pipeline transforms raw sensor anomalies into actionable insights. It includes custom KPI generation, such as `warning_count` derived from real-time status codes, enabling predictive maintenance monitoring in tools like Tableau.
