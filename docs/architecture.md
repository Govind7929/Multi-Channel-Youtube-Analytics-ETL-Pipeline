# Architecture

## Flow
1. `ingestion/generator.py`
   - Generates simulated multi-channel YouTube data
   - Saves raw CSV in `data_samples/raw/`

2. `etl/run_pipeline.py`
   - Initializes schemas and tables
   - Reads raw CSV
   - Cleans and transforms data
   - Loads raw and mart tables into PostgreSQL
   - Saves processed CSV snapshots to `data_samples/processed/`

3. `orchestration/airflow/dags/youtube_analytics_etl_dag.py`
   - Runs pipeline daily through Airflow

4. Power BI
   - Connects to PostgreSQL
   - Uses mart views and fact tables

## Schemas
- `raw`: landing area
- `mart`: analytics-ready star-style tables and views
