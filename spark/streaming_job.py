"""
Phase 4 — Speed Layer (PySpark Structured Streaming)
Reads Kafka `ride_events`, filters dirty rows with JSONL audit logs,
enriches Colombo GPS zones, writes partitioned Parquet to /data/raw_events/,
upserts 30s tumbling window zone metrics into Postgres `realtime_zone_metrics`,
and emits non-spammy >45s `HIGH_IDLE_ALERT` rows into Postgres `fleet_alerts`.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Ensure local imports work regardless of current working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common_logic import (
    RIDE_EVENT_SCHEMA,
    clean_ride_events,
    create_spark_session,
    enrich_with_zone,
)

# ---------------------------------------------------------------------------
# Configuration (container defaults; overridable via .env)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ride_events")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "fleet_db")
PG_USER = os.getenv("POSTGRES_USER", "fleet_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "fleet_pass")

RAW_EVENTS_DIR = "/data/raw_events"
CHECKPOINT_DIR = "/data/checkpoints/streaming_job"
LOG_FILE = "/data/logs/spark_streaming.jsonl"

IDLE_ALERT_THRESHOLD_SEC = 45.0
WINDOW_STATE_RETENTION_MIN = 10

# ---------------------------------------------------------------------------
# Lightweight In-Memory State (safe & tiny: 5 zones, ~20 vehicles)
# ---------------------------------------------------------------------------
# Key: (window_start, window_end, zone) -> metrics accumulator across micro-batches
WINDOW_STATE: dict[tuple[datetime, datetime, str], dict[str, Any]] = {}

# Key: vehicle_id -> {"start_ts": datetime | None, "alerted": bool}
IDLE_TRACKER: dict[str, dict[str, Any]] = {}


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


def prune_old_window_state() -> None:
    """Keep memory bounded by evicting windows older than 10 minutes from latest seen."""
    if not WINDOW_STATE:
        return
    latest_end = max(k[1] for k in WINDOW_STATE.keys())
    cutoff = latest_end - timedelta(minutes=WINDOW_STATE_RETENTION_MIN)
    stale_keys = [k for k in WINDOW_STATE.keys() if k[1] < cutoff]
    for k in stale_keys:
        del WINDOW_STATE[k]


def upsert_zone_metrics(conn, touched_keys: list[tuple[datetime, datetime, str]]) -> int:
    """Upsert 30s zone window metrics into `realtime_zone_metrics` without requiring a UNIQUE constraint."""
    if not touched_keys:
        return 0

    upserted = 0
    with conn.cursor() as cur:
        for key in touched_keys:
            win_start, win_end, zone = key
            state = WINDOW_STATE[key]

            active_vehicles = len(state["vehicles"])
            total_events = state["total_events"]
            idle_events = state["idle_events"]
            idle_ratio = round(idle_events / total_events, 4) if total_events > 0 else 0.0
            trip_count = len(state["trips"])
            total_earnings = round(state["total_earnings"], 2)

            cur.execute(
                """
                UPDATE realtime_zone_metrics
                SET active_vehicles = %s,
                    total_events = %s,
                    idle_events = %s,
                    idle_ratio = %s,
                    trip_count = %s,
                    total_earnings = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE zone = %s AND window_start = %s AND window_end = %s
                """,
                (
                    active_vehicles,
                    total_events,
                    idle_events,
                    idle_ratio,
                    trip_count,
                    total_earnings,
                    zone,
                    win_start,
                    win_end,
                ),
            )

            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO realtime_zone_metrics (
                        window_start, window_end, zone,
                        active_vehicles, total_events, idle_events,
                        idle_ratio, trip_count, total_earnings, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        win_start,
                        win_end,
                        zone,
                        active_vehicles,
                        total_events,
                        idle_events,
                        idle_ratio,
                        trip_count,
                        total_earnings,
                    ),
                )
            upserted += 1

    return upserted


def detect_idle_alerts(status_rows: list) -> list[dict[str, Any]]:
    """
    Detect vehicles continuously `idle` for > 45 seconds.
    Fires once per continuous idle streak (resets when status becomes enroute/on_trip).
    """
    new_alerts: list[dict[str, Any]] = []

    for row in status_rows:
        vid = row["vehicle_id"]
        status = row["status"]
        ts = row["event_ts"]
        zone = row["zone"]

        tracker = IDLE_TRACKER.setdefault(vid, {"start_ts": None, "alerted": False})

        if status == "idle":
            if tracker["start_ts"] is None:
                tracker["start_ts"] = ts
                tracker["alerted"] = False
            elif ts < tracker["start_ts"]:
                tracker["start_ts"] = ts

            idle_sec = (ts - tracker["start_ts"]).total_seconds()
            if idle_sec > IDLE_ALERT_THRESHOLD_SEC and not tracker["alerted"]:
                new_alerts.append(
                    {
                        "vehicle_id": vid,
                        "alert_type": "HIGH_IDLE_ALERT",
                        "idle_duration_sec": round(idle_sec, 2),
                        "zone": zone,
                        "message": (
                            f"Vehicle {vid} continuously idle for {idle_sec:.1f}s "
                            f"in {zone} (threshold {IDLE_ALERT_THRESHOLD_SEC:.0f}s)"
                        ),
                    }
                )
                tracker["alerted"] = True
        else:
            # Vehicle moved to enroute/on_trip -> reset streak for next cycle
            tracker["start_ts"] = None
            tracker["alerted"] = False

    return new_alerts


def insert_fleet_alerts(conn, alerts: list[dict[str, Any]]) -> int:
    if not alerts:
        return 0

    with conn.cursor() as cur:
        for alert in alerts:
            cur.execute(
                """
                INSERT INTO fleet_alerts (
                    vehicle_id, alert_type, idle_duration_sec, zone, message, created_at
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (
                    alert["vehicle_id"],
                    alert["alert_type"],
                    alert["idle_duration_sec"],
                    alert["zone"],
                    alert["message"],
                ),
            )
    return len(alerts)


def process_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Single-pass micro-batch processor for Parquet lake, zone metrics, alerts, and JSONL logs."""
    raw_count = batch_df.count()
    if raw_count == 0:
        return

    parsed_df = (
        batch_df
        .select(F.from_json(F.col("value").cast("string"), RIDE_EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    clean_df = enrich_with_zone(clean_ride_events(parsed_df)).persist()

    try:
        valid_count = clean_df.count()
        dropped_count = raw_count - valid_count

        upserted_windows = 0
        alerts_inserted = 0

        if valid_count > 0:
            # 1. Write cleaned events to partitioned Parquet lake (/data/raw_events/sim_day=X)
            (
                clean_df
                .select(
                    "trip_id", "driver_id", "vehicle_id", "lat", "lon",
                    "speed", "status", "fare", "event_timestamp", "event_ts",
                    "zone", "sim_day",
                )
                .coalesce(1)
                .write
                .mode("append")
                .partitionBy("sim_day")
                .parquet(RAW_EVENTS_DIR)
            )

            # 2. Aggregate 30-second tumbling windows per zone
            window_rows = (
                clean_df
                .groupBy(F.window(F.col("event_ts"), "30 seconds").alias("w"), F.col("zone"))
                .agg(
                    F.collect_set("vehicle_id").alias("vehicle_ids"),
                    F.collect_set(
                        F.when(
                            F.col("trip_id").isNotNull() & (F.trim(F.col("trip_id")) != ""),
                            F.col("trip_id"),
                        )
                    ).alias("trip_ids"),
                    F.count("*").alias("total_events"),
                    F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)).alias("idle_events"),
                    F.sum(F.col("fare")).alias("total_earnings"),
                )
                .select(
                    F.col("w.start").alias("window_start"),
                    F.col("w.end").alias("window_end"),
                    F.col("zone"),
                    F.col("vehicle_ids"),
                    F.col("trip_ids"),
                    F.col("total_events"),
                    F.col("idle_events"),
                    F.col("total_earnings"),
                )
                .collect()
            )

            touched_keys: list[tuple[datetime, datetime, str]] = []
            for row in window_rows:
                key = (row["window_start"], row["window_end"], row["zone"])
                state = WINDOW_STATE.setdefault(
                    key,
                    {
                        "vehicles": set(),
                        "trips": set(),
                        "total_events": 0,
                        "idle_events": 0,
                        "total_earnings": 0.0,
                    },
                )
                state["vehicles"].update(v for v in (row["vehicle_ids"] or []) if v)
                state["trips"].update(t for t in (row["trip_ids"] or []) if t)
                state["total_events"] += int(row["total_events"] or 0)
                state["idle_events"] += int(row["idle_events"] or 0)
                state["total_earnings"] += float(row["total_earnings"] or 0.0)
                touched_keys.append(key)

            # 3. Detect >45s continuous idle streaks
            status_rows = (
                clean_df
                .select("vehicle_id", "status", "event_ts", "zone")
                .orderBy("event_ts")
                .collect()
            )
            alerts = detect_idle_alerts(status_rows)

            # 4. Commit metrics + alerts in one Postgres transaction
            conn = get_pg_connection()
            try:
                with conn:
                    upserted_windows = upsert_zone_metrics(conn, touched_keys)
                    alerts_inserted = insert_fleet_alerts(conn, alerts)
            finally:
                conn.close()

            prune_old_window_state()

        # 5. Structured JSONL audit log
        log_entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "job": "spark_streaming",
            "batch_id": batch_id,
            "total_records": raw_count,
            "valid_records": valid_count,
            "dropped_bad_records": dropped_count,
            "zone_windows_upserted": upserted_windows,
            "alerts_inserted": alerts_inserted,
        }
        append_jsonl_log(log_entry)
        print(f"[Batch {batch_id}] {json.dumps(log_entry)}", flush=True)

    finally:
        clean_df.unpersist()


def main() -> None:
    os.makedirs(RAW_EVENTS_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    print(
        f"Starting Spark Speed Layer -> Kafka={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}",
        flush=True,
    )

    spark = create_spark_session(
        app_name="RideHailingSpeedLayer",
        driver_memory="512m",
        packages=KAFKA_PACKAGE,
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 2000)
        .load()
    )

    query = (
        kafka_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()