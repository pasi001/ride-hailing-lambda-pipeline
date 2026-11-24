"""
Lightweight Pipeline Health Watchdog for Ride-Hailing Lambda Architecture.

Checks three core observability rules strictly in UTC:
  1. Data Freshness (Speed Layer) : realtime_zone_metrics MAX(updated_at) > 60s -> NO_DATA_RECEIVED (CRITICAL)
  2. Data Quality (Error Rate)    : last 10 batches of spark_streaming.jsonl > 5% dropped -> HIGH_ERROR_RATE (WARNING)
  3. Batch SLA (Batch Layer)      : data/batch_inbox/expenses_day_<N>.csv older than 240s
                                    without reconciled daily_vehicle_profitability row -> BATCH_SLA_BREACH (CRITICAL)

Features:
  - Auto-maps POSTGRES_HOST ('postgres' / 'fleet_postgres') to '127.0.0.1' when running on Windows host.
  - Alert deduplication cooldown so the 15s loop never spams Postgres.
  - Supports --once for quick verification and continuous 15s loop by default.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv

# Ensure project root is importable whether run as a script or module
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from observability.logger import get_logger  # noqa: E402

SPARK_STREAMING_LOG = PROJECT_ROOT / "data" / "logs" / "spark_streaming.jsonl"
BATCH_INBOX_DIR = PROJECT_ROOT / "data" / "batch_inbox"
BATCH_FILE_PATTERN = re.compile(r"^expenses_day_(\d+)\.csv$", re.IGNORECASE)

log = get_logger("health_monitor")


def resolve_pg_host(raw_host: str | None) -> str:
    """Map Docker-internal Postgres hostnames to 127.0.0.1 when running on host."""
    host = (raw_host or "127.0.0.1").strip()
    running_in_docker = Path("/.dockerenv").exists()
    if not running_in_docker and host.lower() in {"postgres", "fleet_postgres"}:
        return "127.0.0.1"
    return host


def get_db_connection():
    """Open a short-lived psycopg2 connection using .env without modifying .env."""
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    host = resolve_pg_host(os.getenv("POSTGRES_HOST"))
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "fleet_db")
    user = os.getenv("POSTGRES_USER", "fleet_user")
    password = os.getenv("POSTGRES_PASSWORD", "fleet_pass")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=5,
    )


def utc_now() -> datetime:
    """Return current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def as_utc(dt: datetime | None) -> datetime | None:
    """Treat naive Postgres timestamps as UTC and normalize aware timestamps to UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def insert_health_alert(
    conn,
    stage: str,
    alert_type: str,
    severity: str,
    message: str,
    cooldown_sec: int,
) -> tuple[bool, int | None]:
    """
    Insert into pipeline_health_alerts with UTC deduplication cooldown.
    Returns (inserted_bool, alert_id_or_None).
    """
    now_utc_dt = utc_now()
    now_naive_utc = now_utc_dt.replace(tzinfo=None)
    cutoff_naive_utc = (now_utc_dt - timedelta(seconds=cooldown_sec)).replace(tzinfo=None)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM pipeline_health_alerts
            WHERE stage = %s
              AND alert_type = %s
              AND created_at >= %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (stage, alert_type, cutoff_naive_utc),
        )
        existing = cur.fetchone()
        if existing is not None:
            return False, int(existing[0])

        cur.execute(
            """
            INSERT INTO pipeline_health_alerts (stage, alert_type, severity, message, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (stage, alert_type, severity, message, now_naive_utc),
        )
        new_id = int(cur.fetchone()[0])

    conn.commit()
    return True, new_id


# ---------------------------------------------------------------------------
# Rule 1: Data Freshness / Dead Pipeline (Speed Layer)
# ---------------------------------------------------------------------------
def check_speed_layer_freshness(
    conn,
    freshness_threshold_sec: int,
    cooldown_sec: int,
) -> dict[str, Any]:
    now = utc_now()
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(updated_at) FROM realtime_zone_metrics;")
        row = cur.fetchone()
        max_updated = as_utc(row[0]) if row else None

    if max_updated is None:
        msg = (
            "Speed layer table realtime_zone_metrics has no records yet "
            f"(threshold={freshness_threshold_sec}s)."
        )
        inserted, alert_id = insert_health_alert(
            conn,
            stage="speed_layer",
            alert_type="NO_DATA_RECEIVED",
            severity="CRITICAL",
            message=msg,
            cooldown_sec=cooldown_sec,
        )
        metrics = {
            "last_updated_utc": None,
            "age_seconds": None,
            "threshold_seconds": freshness_threshold_sec,
            "db_inserted": inserted,
            "alert_id": alert_id,
        }
        log.critical("NO_DATA_RECEIVED", metrics=metrics, message=msg)
        return {"status": "CRITICAL", **metrics}

    age_sec = max(0.0, (now - max_updated).total_seconds())
    last_updated_str = max_updated.strftime("%Y-%m-%dT%H:%M:%SZ")

    if age_sec > freshness_threshold_sec:
        msg = (
            f"No new realtime_zone_metrics data for {age_sec:.1f}s "
            f"(threshold={freshness_threshold_sec}s, last_updated_utc={last_updated_str})."
        )
        inserted, alert_id = insert_health_alert(
            conn,
            stage="speed_layer",
            alert_type="NO_DATA_RECEIVED",
            severity="CRITICAL",
            message=msg,
            cooldown_sec=cooldown_sec,
        )
        metrics = {
            "last_updated_utc": last_updated_str,
            "age_seconds": round(age_sec, 1),
            "threshold_seconds": freshness_threshold_sec,
            "db_inserted": inserted,
            "alert_id": alert_id,
        }
        log.critical("NO_DATA_RECEIVED", metrics=metrics, message=msg)
        return {"status": "CRITICAL", **metrics}

    return {
        "status": "OK",
        "last_updated_utc": last_updated_str,
        "age_seconds": round(age_sec, 1),
    }


# ---------------------------------------------------------------------------
# Rule 2: Error Rate Threshold (Data Quality - spark_streaming.jsonl)
# ---------------------------------------------------------------------------
def check_streaming_error_rate(
    conn,
    last_n_batches: int,
    error_rate_threshold_pct: float,
    cooldown_sec: int,
) -> dict[str, Any]:
    if not SPARK_STREAMING_LOG.exists():
        return {"status": "NO_LOG_FILE", "batches_checked": 0}

    recent_batches: deque[dict[str, Any]] = deque(maxlen=last_n_batches)
    with SPARK_STREAMING_LOG.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                entry = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if "total_records" in entry and "dropped_bad_records" in entry:
                recent_batches.append(entry)

    if not recent_batches:
        return {"status": "NO_BATCH_METRICS", "batches_checked": 0}

    total_records = sum(int(b.get("total_records", 0) or 0) for b in recent_batches)
    dropped_records = sum(int(b.get("dropped_bad_records", 0) or 0) for b in recent_batches)

    if total_records <= 0:
        return {
            "status": "OK",
            "batches_checked": len(recent_batches),
            "total_records": 0,
            "dropped_bad_records": 0,
            "error_rate_pct": 0.0,
        }

    error_rate_pct = (dropped_records / total_records) * 100.0

    if error_rate_pct > error_rate_threshold_pct:
        msg = (
            f"Streaming dropped/malformed record rate is {error_rate_pct:.2f}% "
            f"({dropped_records}/{total_records} across last {len(recent_batches)} batches, "
            f"threshold={error_rate_threshold_pct:.1f}%)."
        )
        inserted, alert_id = insert_health_alert(
            conn,
            stage="data_quality",
            alert_type="HIGH_ERROR_RATE",
            severity="WARNING",
            message=msg,
            cooldown_sec=cooldown_sec,
        )
        metrics = {
            "batches_checked": len(recent_batches),
            "total_records": total_records,
            "dropped_bad_records": dropped_records,
            "error_rate_pct": round(error_rate_pct, 2),
            "threshold_pct": error_rate_threshold_pct,
            "db_inserted": inserted,
            "alert_id": alert_id,
        }
        log.warning("HIGH_ERROR_RATE", metrics=metrics, message=msg)
        return {"status": "WARNING", **metrics}

    return {
        "status": "OK",
        "batches_checked": len(recent_batches),
        "total_records": total_records,
        "dropped_bad_records": dropped_records,
        "error_rate_pct": round(error_rate_pct, 2),
    }


# ---------------------------------------------------------------------------
# Rule 3: Batch SLA (data/batch_inbox/expenses_day_<N>.csv vs Postgres)
# ---------------------------------------------------------------------------
def check_batch_sla(
    conn,
    batch_sla_sec: int,
    cooldown_sec: int,
) -> dict[str, Any]:
    if not BATCH_INBOX_DIR.exists():
        return {"status": "NO_INBOX_DIR", "files_checked": 0}

    csv_candidates: list[tuple[int, Path]] = []
    for path in sorted(BATCH_INBOX_DIR.iterdir()):
        if not path.is_file():
            continue
        match = BATCH_FILE_PATTERN.match(path.name)
        if match:
            csv_candidates.append((int(match.group(1)), path))

    if not csv_candidates:
        return {"status": "OK", "files_checked": 0, "breaches": 0}

    # Fetch latest UTC processed_at per sim_day from daily_vehicle_profitability
    processed_by_day: dict[int, datetime] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sim_day, MAX(processed_at)
            FROM daily_vehicle_profitability
            GROUP BY sim_day;
            """
        )
        for sim_day_raw, processed_at_raw in cur.fetchall():
            if sim_day_raw is None or processed_at_raw is None:
                continue
            try:
                day_int = int(sim_day_raw)
            except (TypeError, ValueError):
                continue
            processed_utc = as_utc(processed_at_raw)
            if processed_utc is not None:
                processed_by_day[day_int] = processed_utc

    now = utc_now()
    breaches: list[dict[str, Any]] = []

    for sim_day, csv_path in csv_candidates:
        mtime_utc = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
        age_sec = max(0.0, (now - mtime_utc).total_seconds())
        if age_sec <= batch_sla_sec:
            continue

        processed_utc = processed_by_day.get(sim_day)
        is_reconciled = processed_utc is not None and processed_utc >= mtime_utc
        if not is_reconciled:
            breaches.append(
                {
                    "file": csv_path.name,
                    "sim_day": sim_day,
                    "file_age_seconds": round(age_sec, 1),
                    "file_mtime_utc": mtime_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "db_processed_at_utc": (
                        processed_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                        if processed_utc is not None
                        else None
                    ),
                }
            )

    if breaches:
        breach_summary = ", ".join(
            f"{b['file']} (day={b['sim_day']}, age={b['file_age_seconds']}s)"
            for b in breaches[:5]
        )
        extra = f" (+{len(breaches) - 5} more)" if len(breaches) > 5 else ""
        msg = (
            f"Batch SLA breach (threshold={batch_sla_sec}s): unreconciled inbox CSVs: "
            f"{breach_summary}{extra}."
        )
        inserted, alert_id = insert_health_alert(
            conn,
            stage="batch_layer",
            alert_type="BATCH_SLA_BREACH",
            severity="CRITICAL",
            message=msg,
            cooldown_sec=cooldown_sec,
        )
        metrics = {
            "files_checked": len(csv_candidates),
            "breach_count": len(breaches),
            "breached_days": [b["sim_day"] for b in breaches],
            "oldest_age_seconds": max(b["file_age_seconds"] for b in breaches),
            "threshold_seconds": batch_sla_sec,
            "db_inserted": inserted,
            "alert_id": alert_id,
        }
        log.critical("BATCH_SLA_BREACH", metrics=metrics, message=msg)
        return {"status": "CRITICAL", **metrics}

    return {
        "status": "OK",
        "files_checked": len(csv_candidates),
        "breaches": 0,
    }


def run_check_cycle(args: argparse.Namespace) -> None:
    """Execute all three observability checks in one lightweight DB transaction."""
    conn = None
    try:
        conn = get_db_connection()
        r1 = check_speed_layer_freshness(
            conn,
            freshness_threshold_sec=args.freshness_sec,
            cooldown_sec=args.cooldown,
        )
        r2 = check_streaming_error_rate(
            conn,
            last_n_batches=args.last_batches,
            error_rate_threshold_pct=args.error_rate_pct,
            cooldown_sec=args.cooldown,
        )
        r3 = check_batch_sla(
            conn,
            batch_sla_sec=args.batch_sla_sec,
            cooldown_sec=args.cooldown,
        )
        log.info(
            "HEALTH_CHECK_CYCLE",
            metrics={
                "speed_layer": r1["status"],
                "speed_age_sec": r1.get("age_seconds"),
                "data_quality": r2["status"],
                "error_rate_pct": r2.get("error_rate_pct"),
                "batch_layer": r3["status"],
                "batch_breaches": r3.get("breach_count", 0),
            },
        )
    except psycopg2.Error as exc:
        log.error(
            "POSTGRES_CONNECTION_ERROR",
            metrics={"error_type": type(exc).__name__},
            message=str(exc).strip(),
        )
    except Exception as exc:
        log.error(
            "HEALTH_CHECK_UNEXPECTED_ERROR",
            metrics={"error_type": type(exc).__name__},
            message=str(exc).strip(),
        )
    finally:
        if conn is not None:
            conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lightweight observability watchdog for Ride-Hailing Lambda Pipeline."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single health check cycle and exit.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=15,
        help="Polling interval in seconds for continuous mode (default: 15).",
    )
    parser.add_argument(
        "--cooldown",
        type=int,
        default=120,
        help="Deduplication cooldown in seconds per (stage, alert_type) (default: 120).",
    )
    parser.add_argument(
        "--freshness-sec",
        type=int,
        default=60,
        help="Rule 1 max allowed speed-layer age in seconds (default: 60).",
    )
    parser.add_argument(
        "--error-rate-pct",
        type=float,
        default=5.0,
        help="Rule 2 max allowed streaming dropped-record rate %% (default: 5.0).",
    )
    parser.add_argument(
        "--last-batches",
        type=int,
        default=10,
        help="Rule 2 number of recent streaming batches to evaluate (default: 10).",
    )
    parser.add_argument(
        "--batch-sla-sec",
        type=int,
        default=240,
        help="Rule 3 max allowed unreconciled batch CSV age in seconds (default: 240).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log.info(
        "HEALTH_MONITOR_START",
        metrics={
            "mode": "once" if args.once else "continuous",
            "interval_sec": args.interval,
            "cooldown_sec": args.cooldown,
            "freshness_sec": args.freshness_sec,
            "error_rate_pct": args.error_rate_pct,
            "batch_sla_sec": args.batch_sla_sec,
        },
    )

    if args.once:
        run_check_cycle(args)
        return

    try:
        while True:
            run_check_cycle(args)
            time.sleep(max(1, args.interval))
    except KeyboardInterrupt:
        log.info("HEALTH_MONITOR_STOPPED", message="Stopped by user (Ctrl+C).")


if __name__ == "__main__":
    main()