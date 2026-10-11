"""Ejecuta modelos dbt de BoticaVenta como tareas nativas de Airflow mediante Cosmos."""
from datetime import datetime, timedelta

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/botica_venta"
profile_config = ProfileConfig(
    profile_name="botica_venta",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)
@dag(
    dag_id="transformacion_botica_venta",
    description="Transforma las ventas de botica para obtener indicadores diarios por producto.",
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["botica-venta", "dbt", "snowflake"],
    default_args={"owner": "equipo-datos", "retries": 2, "retry_delay": timedelta(minutes=3)},
)
def transformacion_botica_venta():
    DbtTaskGroup(
        group_id="transformar_ventas",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
    )
transformacion_botica_venta()
