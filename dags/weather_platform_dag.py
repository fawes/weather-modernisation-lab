"""
Production Weather Intelligence Platform DAG (Airflow 2.x TaskFlow API)
Replaces Legacy BP Cron/NFS Airflow DAGs with Event-Driven Lakehouse & Circuit-Breaker Orchestration.

AZURE DATA FACTORY (ADF) / SYNAPSE EQUIVALENT MAPPING:
  - Event Trigger      : Azure Event Grid Trigger (BlobCreated on ADLS Gen2 landing container)
  - Task 1 (Zarr)      : ADF Azure Batch / Databricks Python Activity
  - Task 2 (Delta)     : ADF Mapping Data Flow or Synapse Notebook Activity
  - Task 3 (dbt ELT)   : ADF Web Activity -> dbt Cloud API (or Azure Container Instances dbt runner)
  - Task 4 (Wave Cube) : ADF Databricks / Batch Activity
  - Task 5 (STAC/OGC)  : ADF Web Hook / Azure Function Activity
  - Task 6 (Kafka EIP) : Azure Event Hubs / Logic Apps SAP & Salesforce Publisher
"""

from datetime import datetime, timedelta
import subprocess
import sys
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "climate-data-architecture",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=15),
}

@dag(
    dag_id="uk_north_sea_weather_intelligence_v2",
    default_args=DEFAULT_ARGS,
    description="End-to-End ECMWF NetCDF/GRIB -> Zarr -> Delta Lake -> dbt -> STAC/OGC -> Kafka/SAP/SFDC",
    schedule="0 6,18 * * *",  # Runs at 06:00 & 18:00 UTC (00z and 12z ECMWF availability) or via Dataset trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "zarr", "lakehouse", "dbt", "enterprise-integration"],
)
def weather_intelligence_pipeline():

    @task(task_id="ingest_and_build_zarr_cube")
    def ingest_and_build_zarr():
        """Stage 1 & 2: Ingest UK ERA5 NetCDF, chunk into Zarr, and extract Hive Parquet."""
        subprocess.run([sys.executable, "src/01_prepare_era5_uk.py"], check=True)
        subprocess.run([sys.executable, "src/05_day2_zarr_and_lakehouse.py"], check=True)
        return {"zarr_uri": "data/zarr/era5_uk.zarr", "status": "ZARR_READY"}

    @task(task_id="commit_acid_delta_lakehouse")
    def commit_delta_lakehouse(zarr_meta: dict):
        """Stage 3A: Commit 00z/12z revisions to ACID Delta Lake table with Time-Travel."""
        subprocess.run([sys.executable, "src/06_day3_lakehouse_time_travel.py"], check=True)
        return {"delta_table": "data/lakehouse/uk_energy_delta", "version": 1}

    @task(task_id="run_dbt_transformations_and_dataops_tests")
    def run_dbt_elt_and_quality_gate(delta_meta: dict):
        """
        Stage 3B (CIRCUIT BREAKER): Materialize dbt Staging & Gold Marts and run 8 DataOps tests.
        If physical weather bounds (-50C to +55C, 870 to 1085 hPa) fail, subprocess raises
        CalledProcessError and HALTS the pipeline before bad alerts reach SAP or Salesforce!
        """
        subprocess.run([sys.executable, "src/07_setup_and_run_dbt.py"], check=True)
        return {"dbt_mart": "main.mart_uk_trading_desk_alerts", "tests_passed": 8}

    @task(task_id="process_north_sea_wave_hazards")
    def process_north_sea_waves():
        """Stage 3.5: Build North Sea Wave Zarr cube & evaluate >= 5.0m SWH platform limits."""
        subprocess.run([sys.executable, "src/08_north_sea_wave_alerts.py"], check=True)
        return {"wave_zarr": "data/zarr/north_sea_wave.zarr", "threshold_m": 5.0}

    @task(task_id="publish_stac_catalog_and_verify_ogc_api")
    def publish_stac_and_verify_api(dbt_meta: dict, wave_meta: dict):
        """Stage 4A: Update STAC Catalog JSONs and run health/smoke checks on FastAPI OGC EDR."""
        subprocess.run([sys.executable, "src/09_day4_stac_and_ogc_api.py"], check=True)
        return {"stac_catalog": "data/stac/catalog.json", "ogc_api_status": "HEALTHY"}

    @task(task_id="route_cloud_events_to_sap_and_salesforce")
    def emit_enterprise_events(api_meta: dict):
        """Stage 4B: Execute Camel/Kafka Content-Based Router to SAP PM and Salesforce Trading."""
        subprocess.run([sys.executable, "src/10_day4_enterprise_event_router.py"], check=True)
        return {"events_published": True}

    # Define DAG Parallelism & Dependency Lineage
    zarr_state = ingest_and_build_zarr()
    delta_state = commit_delta_lakehouse(zarr_state)
    dbt_verified = run_dbt_elt_and_quality_gate(delta_state)

    # North Sea Wave processing runs in parallel with the UK Onshore/Trading Lakehouse branch!
    wave_state = process_north_sea_waves()

    # Fan-in: Both branches must succeed and pass DataOps gates before updating STAC & firing Kafka alerts
    api_ready = publish_stac_and_verify_api(dbt_verified, wave_state)
    emit_enterprise_events(api_ready)

dag_instance = weather_intelligence_pipeline()
