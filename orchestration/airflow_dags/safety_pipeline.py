from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "safety-data",
    "retries": 2,
}


with DAG(
    dag_id="safety_risk_intelligence",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["safety", "risk", "snowflake"],
) as dag:
    ingest_incidents = BashOperator(
        task_id="ingest_incidents",
        bash_command="python src/etl/ingest_incidents.py",
    )

    ingest_near_miss = BashOperator(
        task_id="ingest_near_miss",
        bash_command="python src/etl/ingest_near_miss.py",
    )

    ingest_observations = BashOperator(
        task_id="ingest_observations",
        bash_command="python src/etl/ingest_observations.py",
    )

    ingest_reference = BashOperator(
        task_id="ingest_reference",
        bash_command="python src/etl/ingest_reference.py",
    )

    load_incidents = BashOperator(
        task_id="load_incidents_raw",
        bash_command="python src/etl/load_raw_to_snowflake.py --path data/raw/incidents.jsonl --table incidents_api",
    )

    load_near_miss = BashOperator(
        task_id="load_near_miss_raw",
        bash_command="python src/etl/load_raw_to_snowflake.py --path data/raw/near_miss.jsonl --table near_miss_api",
    )

    load_observations = BashOperator(
        task_id="load_observations_raw",
        bash_command="python src/etl/load_raw_to_snowflake.py --path data/raw/safety_observations.jsonl --table safety_observation_api",
    )

    load_reference = BashOperator(
        task_id="load_reference_raw",
        bash_command="python src/etl/load_raw_to_snowflake.py --path data/raw/employees.jsonl --table employees_api && python src/etl/load_raw_to_snowflake.py --path data/raw/locations.jsonl --table locations_api && python src/etl/load_raw_to_snowflake.py --path data/raw/assets.jsonl --table assets_api && python src/etl/load_raw_to_snowflake.py --path data/raw/hours_worked.jsonl --table hours_worked_api",
    )

    stage = BashOperator(
        task_id="stage_data",
        bash_command="snowsql -f sql/staging/02_parse_raw_to_staging.sql",
    )

    warehouse = BashOperator(
        task_id="build_warehouse",
        bash_command="snowsql -f sql/marts/01_create_dimensions.sql",
    )

    facts = BashOperator(
        task_id="build_facts",
        bash_command="snowsql -f sql/marts/02_create_fact_exposure.sql && snowsql -f sql/marts/03_create_fact_incidents.sql && snowsql -f sql/marts/04_create_fact_near_miss.sql && snowsql -f sql/marts/05_create_fact_safety_observation.sql",
    )

    marts = BashOperator(
        task_id="build_marts",
        bash_command="snowsql -f sql/marts/06_create_kpi_marts.sql",
    )

    audits = BashOperator(
        task_id="run_audits",
        bash_command="snowsql -f sql/audits/02_quality_checks.sql && snowsql -f sql/audits/03_referential_integrity.sql && snowsql -f sql/audits/04_masking_validation.sql",
    )

    ingest_incidents >> load_incidents
    ingest_near_miss >> load_near_miss
    ingest_observations >> load_observations
    ingest_reference >> load_reference

    [load_incidents, load_near_miss, load_observations, load_reference] >> stage
    stage >> warehouse >> facts >> marts >> audits
