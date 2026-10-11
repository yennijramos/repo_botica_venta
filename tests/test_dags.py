import pytest
from airflow.models.dagbag import DagBag


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_hay_errores_de_import(dagbag):
    assert len(dagbag.import_errors) == 0, dagbag.import_errors


@pytest.mark.parametrize(
    "dag_id",
    ["ingesta_botica_venta", "transformacion_botica_venta"],
)
def test_dags_principales_cargan(dagbag, dag_id):
    assert dagbag.get_dag(dag_id) is not None


def test_dags_tienen_schedule_y_catchup_desactivado(dagbag):
    for dag in dagbag.dags.values():
        schedule = getattr(
            dag,
            "schedule",
            getattr(dag, "schedule_interval", None),
        )
        assert schedule is not None
        assert dag.catchup is False

def test_tareas_tienen_owner_y_reintentos(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.owner
            assert task.retries >= 1