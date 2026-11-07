"""
### M-Pesa daily pipeline

One run per **business day** (Nairobi time). The run for 1 Oct starts at
midnight at the end of 1 Oct, once that day's data is complete:

    wait_for_file -> ingest -> build_staging -> check_staging -> build_warehouse -> check_warehouse

* **wait_for_file**: sensor. Waits (up to 6 h) for that day's export to land.
* **ingest**: loads that day's file (+ the shops snapshot) into `raw`. Idempotent.
* **build_staging / build_warehouse**: rebuild the layer from the one below.
* **check_\\***: data quality gates. A failed staging check stops the run
  *before* the warehouse is touched, so analysts keep yesterday's good numbers.

Backfill history from the CLI, for example:

    airflow backfill create --dag-id mpesa_daily --from-date 2026-10-01 --to-date 2026-10-30 --max-active-runs 1
"""
import logging

import pendulum
from airflow.sdk import PokeReturnValue, dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.interval import CronDataIntervalTimetable

# The DAG processor re-imports this file every few seconds just to read the DAG's
# shape. So only cheap imports up here; the pipeline code is imported inside each
# task, where it runs once per task execution.

TZ = "Africa/Nairobi"
log = logging.getLogger(__name__)


def business_day(context: dict) -> pendulum.Date:
    """The day this run is responsible for.

    For scheduled runs and backfills, logical_date is the start of the day's
    interval. A manual run can be given any logical_date ("re-run 3 Oct"), or none
    at all ("run now"). Then it falls back to the data interval, which Airflow 3
    works out from the trigger time: the last complete day, i.e. yesterday.

    Airflow stores times in UTC. Nairobi midnight on 1 Oct is 21:00 UTC on
    30 Sep, so reading the date straight off the UTC value is off by one day.
    Convert to Nairobi time first.
    """
    moment = context.get("logical_date") or context["data_interval_start"]
    return moment.in_timezone(TZ).date()


def alert(context) -> None:
    """Runs when a task fails for good (after its retries). In production this would
    page someone: email, Slack, PagerDuty. Here it writes a loud line to the task log."""
    ti = context["ti"]
    log.error("ALERT: %s.%s failed for run %s", ti.dag_id, ti.task_id, ti.run_id)


@dag(
    dag_id="mpesa_daily",
    # Interval semantics: the run for day D covers [D 00:00, D+1 00:00) Nairobi time
    # and is triggered when that interval ends. (Airflow 3's default for cron
    # strings is different: the run is labelled with its trigger time instead.)
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=TZ),
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,           # past days are loaded deliberately, with a backfill
    max_active_runs=1,       # every run rebuilds the same staging/warehouse tables: one at a time
    default_args={
        "retries": 2,                                  # for transient problems (DB restart, network)
        "retry_delay": pendulum.duration(minutes=1),
        "on_failure_callback": alert,
    },
    tags=["mpesa"],
    doc_md=__doc__,
)
def mpesa_daily():

    # A sensor is a task that waits for a condition. mode="reschedule" frees the
    # worker slot between checks instead of sleeping in it.
    @task.sensor(poke_interval=60, timeout=6 * 60 * 60, mode="reschedule")
    def wait_for_file(**context) -> PokeReturnValue:
        from mpesa_pipeline.ingest import daily_file

        path = daily_file(business_day(context))
        return PokeReturnValue(is_done=path.exists(), xcom_value=path.name)

    @task
    def ingest(**context) -> dict:
        from mpesa_pipeline.ingest import ingest_all

        summary = ingest_all(day=business_day(context))
        return {"loaded": summary["loaded"], "skipped": summary["skipped"]}

    @task
    def build_staging() -> dict:
        from mpesa_pipeline.staging import build_staging as build

        return build()

    @task
    def build_warehouse() -> dict:
        from mpesa_pipeline.warehouse import build_warehouse as build

        return build()

    def quality_gate(layer: str):
        @task(task_id=f"check_{layer}")
        def check() -> dict:
            from mpesa_pipeline.checks import DataQualityError, run_checks

            try:
                return run_checks(layer)
            except DataQualityError as exc:
                # Retrying won't change the data, so fail now, skip the retries,
                # and alert straight away.
                raise AirflowFailException(str(exc)) from exc

        return check()

    (
        wait_for_file()
        >> ingest()
        >> build_staging()
        >> quality_gate("staging")
        >> build_warehouse()
        >> quality_gate("warehouse")
    )


mpesa_daily()
