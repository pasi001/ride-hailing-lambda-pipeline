"""
Phase 4 — Batch Layer (PySpark Daily Profitability Reconciliation)
Reads /data/raw_events/sim_day={X} Parquet data and /data/batch_inbox/expenses_day_{X}.csv,
computes per-vehicle total_trips, total_fare, net_profit, and is_unprofitable,
upserts into Postgres `daily_vehicle_profitability`, and appends structured JSONL metrics
to /data/logs/spark_batch.jsonl.
"""

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg2
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Ensure local imports work regardless of current working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common_logic import create_spark_session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "fleet_db")
PG_USER = os.getenv("POSTGRES_USER", "fleet_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "fleet_pass")

RAW_EVENTS_DIR = "/data/raw_events"
BATCH_INBOX_DIR = "/data/batch_inbox"
LOG_FILE = "/data/logs/spark_batch.jsonl"

CSV_PATTERN = re.compile(r"^expenses_day_(\d+)\.csv$")

EXPENSES_SCHEMA = T.StructType([
    T.StructField("sim_day", T.IntegerType(), True),
    T.StructField("vehicle_id", T.StringType(), True),
    T.StructField("fuel_cost", T.DoubleType(), True),
    T.StructField("maintenance_cost", T.DoubleType(), True),
    T.StructField("distance_covered", T.DoubleType(), True),
    T.StructField("service_flag", T.StringType(), True),
])


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def append_jsonl_log(record: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


def get_available_csv_days() -> list[int]:
    """Return sorted list of sim_days that have an expenses CSV in /data/batch_inbox/."""
    if not os.path.isdir(BATCH_INBOX_DIR):
        return []
    days: list[int] = []
    for fname in os.listdir(BATCH_INBOX_DIR):
        match = CSV_PATTERN.match(fname)
        if match:
            days.append(int(match.group(1)))
    return sorted(days)


def get_processed_sim_days() -> set[int]:
    """Fetch sim_days already reconciled in Postgres `daily_vehicle_profitability`."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sim_day FROM daily_vehicle_profitability")
            return {int(row[0]) for row in cur.fetchall()}
    finally:
        conn.close()


def has_raw_parquet(sim_day: int) -> bool:
    part_dir = os.path.join(RAW_EVENTS_DIR, f"sim_day={sim_day}")
    return os.path.isdir(part_dir) and bool(glob.glob(os.path.join(part_dir, "*.parquet")))


def detect_target_sim_day() -> int | None:
    """
    Auto-detect the latest unprocessed `expenses_day_X.csv`.
    Prefers the latest unprocessed day that already has Parquet files in /data/raw_events/.
    """
    csv_days = get_available_csv_days()
    if not csv_days:
        return None

    processed_days = get_processed_sim_days()
    unprocessed = [d for d in csv_days if d not in processed_days]
    if not unprocessed:
        return None

    with_raw = [d for d in unprocessed if has_raw_parquet(d)]
    return max(with_raw) if with_raw else max(unprocessed)


def upsert_profitability(rows: list[dict[str, Any]]) -> int:
    """Idempotent upsert into Postgres `daily_vehicle_profitability` using (sim_day, vehicle_id) PK."""
    if not rows:
        return 0

    sql = """
        INSERT INTO daily_vehicle_profitability (
            sim_day, vehicle_id, total_trips, total_fare,
            fuel_cost, maintenance_cost, distance_covered,
            service_flag, net_profit, is_unprofitable, processed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (sim_day, vehicle_id)
        DO UPDATE SET
            total_trips = EXCLUDED.total_trips,
            total_fare = EXCLUDED.total_fare,
            fuel_cost = EXCLUDED.fuel_cost,
            maintenance_cost = EXCLUDED.maintenance_cost,
            distance_covered = EXCLUDED.distance_covered,
            service_flag = EXCLUDED.service_flag,
            net_profit = EXCLUDED.net_profit,
            is_unprofitable = EXCLUDED.is_unprofitable,
            processed_at = CURRENT_TIMESTAMP
    """

    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        sql,
                        (
                            r["sim_day"],
                            r["vehicle_id"],
                            r["total_trips"],
                            r["total_fare"],
                            r["fuel_cost"],
                            r["maintenance_cost"],
                            r["distance_covered"],
                            r["service_flag"],
                            r["net_profit"],
                            r["is_unprofitable"],
                        ),
                    )
    finally:
        conn.close()

    return len(rows)


def run_reconciliation(sim_day: int) -> dict[str, Any]:
    csv_path = os.path.join(BATCH_INBOX_DIR, f"expenses_day_{sim_day}.csv")
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"Expense CSV not found: {csv_path}")

    part_dir = os.path.join(RAW_EVENTS_DIR, f"sim_day={sim_day}")
    raw_found = has_raw_parquet(sim_day)

    spark = create_spark_session(
        app_name=f"RideHailingBatchDay{sim_day}",
        driver_memory="512m",
    )

    try:
        # 1. Read daily vehicle expenses CSV
        expenses_df = (
            spark.read
            .option("header", "true")
            .schema(EXPENSES_SCHEMA)
            .csv(csv_path)
            .filter(F.col("vehicle_id").isNotNull() & (F.trim(F.col("vehicle_id")) != ""))
            .withColumn("sim_day", F.lit(sim_day).cast("int"))
            .withColumn("fuel_cost", F.coalesce(F.col("fuel_cost"), F.lit(0.0)))
            .withColumn("maintenance_cost", F.coalesce(F.col("maintenance_cost"), F.lit(0.0)))
            .withColumn("distance_covered", F.coalesce(F.col("distance_covered"), F.lit(0.0)))
            .withColumn(
                "service_flag",
                F.lower(F.trim(F.coalesce(F.col("service_flag"), F.lit("false")))) == F.lit("true"),
            )
        )

        # 2. Read & aggregate raw Parquet events for this sim_day
        if raw_found:
            raw_df = spark.read.parquet(part_dir)
            trips_agg_df = (
                raw_df
                .groupBy("vehicle_id")
                .agg(
                    F.countDistinct(
                        F.when(
                            F.col("trip_id").isNotNull() & (F.trim(F.col("trip_id")) != ""),
                            F.col("trip_id"),
                        )
                    ).cast("int").alias("total_trips"),
                    F.round(F.sum(F.coalesce(F.col("fare"), F.lit(0.0))), 2).alias("total_fare"),
                )
            )
        else:
            empty_schema = T.StructType([
                T.StructField("vehicle_id", T.StringType(), True),
                T.StructField("total_trips", T.IntegerType(), True),
                T.StructField("total_fare", T.DoubleType(), True),
            ])
            trips_agg_df = spark.createDataFrame([], schema=empty_schema)

        # 3. Join expenses (left) with trip earnings & compute profitability
        reconciled_df = (
            expenses_df
            .join(trips_agg_df, on="vehicle_id", how="left")
            .withColumn("total_trips", F.coalesce(F.col("total_trips"), F.lit(0)).cast("int"))
            .withColumn("total_fare", F.round(F.coalesce(F.col("total_fare"), F.lit(0.0)), 2))
            .withColumn(
                "net_profit",
                F.round(
                    F.col("total_fare") - (F.col("fuel_cost") + F.col("maintenance_cost")),
                    2,
                ),
            )
            .withColumn("is_unprofitable", F.col("net_profit") < F.lit(0.0))
            .orderBy("vehicle_id")
        )

        collected = reconciled_df.collect()
        records = [
            {
                "sim_day": int(r["sim_day"]),
                "vehicle_id": str(r["vehicle_id"]),
                "total_trips": int(r["total_trips"]),
                "total_fare": round(float(r["total_fare"]), 2),
                "fuel_cost": round(float(r["fuel_cost"]), 2),
                "maintenance_cost": round(float(r["maintenance_cost"]), 2),
                "distance_covered": round(float(r["distance_covered"]), 2),
                "service_flag": bool(r["service_flag"]),
                "net_profit": round(float(r["net_profit"]), 2),
                "is_unprofitable": bool(r["is_unprofitable"]),
            }
            for r in collected
        ]

        # 4. Upsert into Postgres
        upserted = upsert_profitability(records)

        unprofitable_list = [r["vehicle_id"] for r in records if r["is_unprofitable"]]
        total_fare_sum = round(sum(r["total_fare"] for r in records), 2)
        total_cost_sum = round(sum(r["fuel_cost"] + r["maintenance_cost"] for r in records), 2)
        total_net_profit = round(sum(r["net_profit"] for r in records), 2)

        log_entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "job": "spark_batch_reconciliation",
            "status": "success",
            "sim_day": sim_day,
            "csv_file": os.path.basename(csv_path),
            "raw_events_found": raw_found,
            "vehicles_reconciled": upserted,
            "unprofitable_count": len(unprofitable_list),
            "unprofitable_vehicles": unprofitable_list,
            "total_fleet_fare": total_fare_sum,
            "total_fleet_cost": total_cost_sum,
            "total_net_profit": total_net_profit,
        }
        append_jsonl_log(log_entry)
        print(f"[Batch Reconciliation] {json.dumps(log_entry)}", flush=True)
        return log_entry

    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily Vehicle Profitability Batch Reconciliation")
    parser.add_argument(
        "--sim-day",
        type=int,
        default=None,
        help="Specific sim_day to reconcile. If omitted, auto-detects latest unprocessed CSV.",
    )
    args = parser.parse_args()

    target_day = args.sim_day if args.sim_day is not None else detect_target_sim_day()

    if target_day is None:
        skip_log = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "job": "spark_batch_reconciliation",
            "status": "skipped_no_unprocessed_csv",
            "sim_day": None,
        }
        append_jsonl_log(skip_log)
        print(f"[Batch Reconciliation] {json.dumps(skip_log)}", flush=True)
        return

    run_reconciliation(target_day)


if __name__ == "__main__":
    main()