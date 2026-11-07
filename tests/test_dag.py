"""DAG integrity tests: does the DAG file load, and is it wired the way we expect?

They need Airflow installed, so they are skipped in the normal local test run
(Airflow is a large install, and it lives in the Docker image instead). Run them
inside the Airflow container:

    docker exec mpesa_airflow bash -c "pip install -q pytest && cd /opt/mpesa-pipeline && python -m pytest tests/test_dag.py -p no:cacheprovider"
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow")

import pendulum  # noqa: E402
from airflow.models.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


def test_dag_files_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_tasks_run_in_pipeline_order(dagbag):
    dag = dagbag.get_dag("mpesa_daily")
    order = ["wait_for_file", "ingest", "build_staging", "check_staging", "build_warehouse", "check_warehouse"]
    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:]):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_one_run_at_a_time(dagbag):
    assert dagbag.get_dag("mpesa_daily").max_active_runs == 1


def test_business_day_is_read_in_nairobi_time():
    from dags.mpesa_daily import business_day

    # Nairobi midnight starting 1 Oct, as Airflow stores it (UTC): 30 Sep 21:00.
    start = pendulum.datetime(2026, 9, 30, 21, tz="UTC")
    assert business_day({"logical_date": start}) == pendulum.date(2026, 10, 1)
    # A manual run with no logical date falls back to the data interval.
    assert business_day({"logical_date": None, "data_interval_start": start}) == pendulum.date(2026, 10, 1)
