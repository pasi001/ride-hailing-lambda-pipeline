"""
Phase 4 — Airflow Orchestration DAG
Runs every 3 minutes:
  1. Checks /data/batch_inbox/ for new unprocessed expenses_day_X.csv files
     (short-circuits cleanly if none exist, saving RAM on 8GB laptops).
  2. Executes PySpark batch_reconciliation.py with retries=2.
  3. Validates Postgres row counts across daily_vehicle_profitability,
     realtime_zone_metrics, and fleet_alerts.
"""

import glob
import os
import re
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_INBOX_DIR = "/data/batch_inbox"
RAW_EVENTS_DIR = "/data/raw_events"
CSV_PATTERN = re.compile(r"^expenses_day_(\d+)\.csv$")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "fleet_db")
PG_USER = os.getenv("POSTGRES_USER", "fleet_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "fleet_pass")


def _get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def _has_raw_parquet(sim_day: int) -> bool:
    part_dir = os.path.join(RAW_EVENTS_DIR, f"sim_day={sim_day}")
    return os.path.isdir(part_dir) and bool(glob.glob(os.path.join(part_dir, "*.parquet")))


def check_for_new_csv(**context) -> bool:
    """
    Task 1: Detect the latest unprocessed expenses_day_X.csv.
    Supports optional manual override via dag_run.conf = {"sim_day": <int>}.
    Returns False to short-circuit downstream tasks when no new CSV is waiting.
    """
    dag_run = context.get("dag_run")
    ti = context["ti"]

    # Optional manual override: airflow dags trigger ... --conf '{"sim_day": 10}'
    if dag_run and dag_run.conf and dag_run.conf.get("sim_day") is not None:
        forced_day = int(dag_run.conf["sim_day"])
        csv_path = os.path.join(BATCH_INBOX_DIR, f"expenses_day_{forced_day}.csv")
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"Requested sim_day={forced_day} CSV not found: {csv_path}")
        ti.xcom_push(key="target_sim_day", value=forced_day)
        print(f"[check_new_csv_files] Manual override active -> sim_day={forced_day}")
        return True

    if not os.path.isdir(BATCH_INBOX_DIR):
        print(f"[check_new_csv_files] Directory missing: {BATCH_INBOX_DIR}. Skipping.")
        return False

    csv_days = sorted(
        int(m.group(1))
        for fname in os.listdir(BATCH_INBOX_DIR)
        if (m := CSV_PATTERN.match(fname))
    )
    if not csv_days:
        print("[check_new_csv_files] No expenses_day_X.csv files found. Skipping.")
        return False

    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sim_day FROM daily_vehicle_profitability")
            processed_days = {int(row[0]) for row in cur.fetchall()}
    finally:
        conn.close()

    unprocessed = [d for d in csv_days if d not in processed_days]
    if not unprocessed:
        print(
            f"[check_new_csv_files] All {len(csv_days)} CSV file(s) already reconciled "
            f"(days={csv_days}). Short-circuiting."
        )
        return False

    with_raw = [d for d in unprocessed if _has_raw_parquet(d)]
    target_day = max(with_raw) if with_raw else max(unprocessed)

    ti.xcom_push(key="target_sim_day", value=target_day)
    print(
        f"[check_new_csv_files] Found unprocessed CSVs={unprocessed}. "
        f"Selected target sim_day={target_day} (has_raw_parquet={target_day in with_raw})."
    )
    return True


def validate_postgres_row_counts(**context) -> dict:
    """
    Task 3: Verify reconciled rows exist for target_sim_day and log counts across all 3 tables.
    """
    ti = context["ti"]
    target_day = ti.xcom_pull(task_ids="check_new_csv_files", key="target_sim_day")

    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_vehicles,
                    SUM(CASE WHEN is_unprofitable THEN 1 ELSE 0 END) AS unprofitable_vehicles,
                    COALESCE(SUM(total_fare), 0) AS sum_fare,
                    COALESCE(SUM(net_profit), 0) AS sum_net_profit
                FROM daily_vehicle_profitability
                WHERE sim_day = %s
                """,
                (target_day,),
            )
            day_count, unprof_count, sum_fare, sum_profit = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM daily_vehicle_profitability")
            total_dvp = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM realtime_zone_metrics")
            total_rzm = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fleet_alerts")
            total_alerts = cur.fetchone()[0]
    finally:
        conn.close()

    if day_count == 0:
        raise ValueError(
            f"Validation FAILED: 0 rows found in daily_vehicle_profitability for sim_day={target_day}"
        )

    summary = {
        "validated_sim_day": int(target_day),
        "day_vehicle_rows": int(day_count),
        "day_unprofitable_vehicles": int(unprof_count or 0),
        "day_total_fare": float(sum_fare),
        "day_net_profit": float(sum_profit),
        "table_counts": {
            "daily_vehicle_profitability": int(total_dvp),
            "realtime_zone_metrics": int(total_rzm),
            "fleet_alerts": int(total_alerts),
        },
    }
    print(f"[validate_postgres_counts] Validation PASSED: {summary}")
    return summary


default_args = {
    "owner": "fleet_ops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_fleet_reconciliation",
    description="Reconciles daily vehicle expenses with Spark Parquet trip earnings every 3 minutes",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lambda", "batch", "pyspark", "fleet"],
) as dag:

    check_new_csv_files = ShortCircuitOperator(
        task_id="check_new_csv_files",
        python_callable=check_for_new_csv,
    )

    run_batch_reconciliation = BashOperator(
        task_id="run_batch_reconciliation",
        retries=2,
        retry_delay=timedelta(seconds=30),
        bash_command=(
            "python3 /opt/airflow/spark/batch_reconciliation.py "
            "--sim-day {{ ti.xcom_pull(task_ids='check_new_csv_files', key='target_sim_day') }}"
        ),
    )

    validate_postgres_counts = PythonOperator(
        task_id="validate_postgres_counts",
        python_callable=validate_postgres_row_counts,
    )

    check_new_csv_files >> run_batch_reconciliation >> validate_postgres_counts