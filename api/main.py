import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from psycopg2.extras import RealDictCursor

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

STREAMING_LOG_PATH = BASE_DIR / "data" / "logs" / "spark_streaming.jsonl"

app = FastAPI(
    title="Ride-Hailing Fleet Operations API",
    description="Lightweight Lambda Architecture Serving Layer (Speed + Batch)",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_config() -> dict[str, Any]:
    raw_host = os.getenv(
        "API_DB_HOST",
        os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "127.0.0.1")),
    )
    # If running on Windows/local host (not inside Docker), Docker service names resolve to 127.0.0.1
    running_in_docker = Path("/.dockerenv").exists()
    if not running_in_docker and raw_host in ("postgres", "fleet_postgres"):
        db_host = "127.0.0.1"
    else:
        db_host = raw_host

    return {
        "host": db_host,
        "port": int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432"))),
        "dbname": os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "fleet_db")),
        "user": os.getenv("POSTGRES_USER", os.getenv("DB_USER", "fleet_user")),
        "password": os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", "fleet_pass")),
        "connect_timeout": 5,
    }


@contextmanager
def get_db_cursor():
    """Lightweight short-lived Postgres connection (RAM-friendly for 8GB host)."""
    conn = psycopg2.connect(**get_db_config())
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


def utc_now_naive() -> datetime:
    """Return current UTC time as naive datetime to match Postgres timestamp without time zone."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def compute_age_seconds(ts: Optional[datetime]) -> Optional[float]:
    """Compute age in seconds strictly in UTC (immune to local Windows +05:30 offset)."""
    if ts is None:
        return None
    if ts.tzinfo is not None:
        ts_naive = ts.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        ts_naive = ts
    delta = (utc_now_naive() - ts_naive).total_seconds()
    return round(max(0.0, delta), 2)


def serialize_value(val: Any) -> Any:
    """Recursively convert Decimal -> float and datetime -> UTC ISO string."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return val.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(val, dict):
        return {k: serialize_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [serialize_value(item) for item in val]
    return val


def safe_table_count(cur, table_name: str) -> int:
    """Safely count rows in a table without aborting the transaction if missing."""
    try:
        cur.execute("SAVEPOINT sp_count;")
        cur.execute(f"SELECT COUNT(*) AS cnt FROM {table_name};")
        row = cur.fetchone()
        cur.execute("RELEASE SAVEPOINT sp_count;")
        return int(row["cnt"]) if row else 0
    except psycopg2.Error:
        cur.execute("ROLLBACK TO SAVEPOINT sp_count;")
        return 0


def read_dropped_bad_records(log_path: Path = STREAMING_LOG_PATH) -> int:
    """Safely parse spark_streaming.jsonl to extract total dropped bad records."""
    if not log_path.exists():
        return 0

    total_dropped = 0
    count_keys = (
        "dropped_bad_records",
        "bad_records",
        "dropped_records",
        "invalid_records",
        "corrupt_records",
        "malformed_records",
    )

    try:
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if not isinstance(entry, dict):
                    continue

                # Check top-level or nested details/metrics dicts for explicit numeric counts
                found_numeric = False
                search_dicts = [entry]
                for nested_key in ("metrics", "details", "data", "extra"):
                    if isinstance(entry.get(nested_key), dict):
                        search_dicts.append(entry[nested_key])

                for d in search_dicts:
                    for key in count_keys:
                        if key in d and isinstance(d[key], (int, float)):
                            total_dropped += int(d[key])
                            found_numeric = True
                            break
                    if found_numeric:
                        break

                # Fallback: count discrete bad-record log events if no numeric counter field exists
                if not found_numeric:
                    event_text = " ".join(
                        str(entry.get(k, "")).lower()
                        for k in ("event", "event_type", "alert_type", "message", "msg")
                    )
                    if any(
                        token in event_text
                        for token in ("bad_record", "dropped record", "corrupt_record", "malformed")
                    ):
                        total_dropped += 1
    except OSError:
        return 0

    return total_dropped


@app.get("/health")
def health_check():
    """Verify Postgres connectivity, return row counts summary and UTC server timestamp."""
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT 1;")
            counts = {
                "realtime_zone_metrics": safe_table_count(cur, "realtime_zone_metrics"),
                "fleet_alerts": safe_table_count(cur, "fleet_alerts"),
                "daily_vehicle_profitability": safe_table_count(cur, "daily_vehicle_profitability"),
                "pipeline_health_alerts": safe_table_count(cur, "pipeline_health_alerts"),
            }
        return {
            "status": "healthy",
            "database": "connected",
            "row_counts": counts,
            "server_time_utc": utc_now_iso(),
        }
    except psycopg2.Error as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(exc),
                "server_time_utc": utc_now_iso(),
            },
        )


@app.get("/api/v1/fleet/realtime")
def get_realtime_fleet():
    """Latest per-zone metrics via DISTINCT ON (zone), last 20 windows trend, and fleet totals."""
    try:
        with get_db_cursor() as cur:
            # 1. Latest snapshot per zone (prevents missing zones / dashboard flickering)
            cur.execute(
                """
                SELECT DISTINCT ON (zone)
                    id,
                    window_start,
                    window_end,
                    zone,
                    active_vehicles,
                    total_events,
                    idle_events,
                    idle_ratio,
                    trip_count,
                    total_earnings,
                    updated_at
                FROM realtime_zone_metrics
                ORDER BY zone, window_end DESC, updated_at DESC;
                """
            )
            latest_zones = [serialize_value(dict(r)) for r in cur.fetchall()]

            # 2. Recent 20 aggregated windows for trend charts (chronological ascending)
            cur.execute(
                """
                SELECT * FROM (
                    SELECT
                        window_start,
                        window_end,
                        SUM(active_vehicles) AS active_vehicles,
                        SUM(total_events) AS total_events,
                        SUM(idle_events) AS idle_events,
                        AVG(idle_ratio) AS avg_idle_ratio,
                        SUM(trip_count) AS trip_count,
                        SUM(total_earnings) AS total_earnings
                    FROM realtime_zone_metrics
                    GROUP BY window_start, window_end
                    ORDER BY window_end DESC
                    LIMIT 20
                ) recent
                ORDER BY window_end ASC;
                """
            )
            window_history = [serialize_value(dict(r)) for r in cur.fetchall()]

            # 3. Latest timestamp for UTC freshness
            cur.execute(
                "SELECT MAX(updated_at) AS latest_updated_at FROM realtime_zone_metrics;"
            )
            ts_row = cur.fetchone()
            latest_age_sec = compute_age_seconds(
                ts_row["latest_updated_at"] if ts_row else None
            )

        # Fleet totals from latest per-zone snapshot
        total_active_vehicles = sum(int(z.get("active_vehicles") or 0) for z in latest_zones)
        total_trips = sum(int(z.get("trip_count") or 0) for z in latest_zones)
        total_earnings = round(
            sum(float(z.get("total_earnings") or 0.0) for z in latest_zones), 2
        )
        avg_idle_ratio = (
            round(
                sum(float(z.get("idle_ratio") or 0.0) for z in latest_zones)
                / len(latest_zones),
                4,
            )
            if latest_zones
            else 0.0
        )

        return {
            "server_time_utc": utc_now_iso(),
            "latest_event_age_sec": latest_age_sec,
            "fleet_totals": {
                "active_vehicles": total_active_vehicles,
                "avg_idle_ratio": avg_idle_ratio,
                "total_trips": total_trips,
                "total_earnings": total_earnings,
            },
            "zones": latest_zones,
            "recent_windows": window_history,
        }
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


@app.get("/api/v1/alerts")
def get_alerts(limit: int = Query(default=25, ge=1, le=200)):
    """Return latest fleet_alerts and pipeline_health_alerts gracefully."""
    try:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT id, vehicle_id, alert_type, idle_duration_sec, zone, message, created_at
                FROM fleet_alerts
                ORDER BY created_at DESC, id DESC
                LIMIT %s;
                """,
                (limit,),
            )
            fleet_alerts = [serialize_value(dict(r)) for r in cur.fetchall()]

            # Handle pipeline_health_alerts gracefully if empty or not yet populated
            pipeline_health_alerts = []
            try:
                cur.execute("SAVEPOINT sp_health_alerts;")
                cur.execute(
                    """
                    SELECT id, stage, alert_type, severity, message, created_at
                    FROM pipeline_health_alerts
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
                pipeline_health_alerts = [serialize_value(dict(r)) for r in cur.fetchall()]
                cur.execute("RELEASE SAVEPOINT sp_health_alerts;")
            except psycopg2.Error:
                cur.execute("ROLLBACK TO SAVEPOINT sp_health_alerts;")
                pipeline_health_alerts = []

        return {
            "server_time_utc": utc_now_iso(),
            "limit": limit,
            "counts": {
                "fleet_alerts": len(fleet_alerts),
                "pipeline_health_alerts": len(pipeline_health_alerts),
                "total": len(fleet_alerts) + len(pipeline_health_alerts),
            },
            "fleet_alerts": fleet_alerts,
            "pipeline_health_alerts": pipeline_health_alerts,
        }
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


@app.get("/api/v1/reports/daily")
def get_daily_report(sim_day: Optional[int] = Query(default=None)):
    """Daily vehicle profitability reconciliation report (defaults to latest MAX(sim_day))."""
    try:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT sim_day
                FROM daily_vehicle_profitability
                ORDER BY sim_day DESC;
                """
            )
            available_days = [int(r["sim_day"]) for r in cur.fetchall()]

            if not available_days:
                return {
                    "server_time_utc": utc_now_iso(),
                    "available_sim_days": [],
                    "selected_sim_day": None,
                    "kpi_summary": {
                        "total_vehicles": 0,
                        "total_trips": 0,
                        "total_fare": 0.0,
                        "total_fuel_cost": 0.0,
                        "total_maintenance_cost": 0.0,
                        "total_cost": 0.0,
                        "net_profit": 0.0,
                        "unprofitable_count": 0,
                    },
                    "rows": [],
                    "unprofitable_vehicles": [],
                }

            selected_day = sim_day if sim_day is not None else available_days[0]

            cur.execute(
                """
                SELECT
                    sim_day,
                    vehicle_id,
                    total_trips,
                    total_fare,
                    fuel_cost,
                    maintenance_cost,
                    distance_covered,
                    service_flag,
                    net_profit,
                    is_unprofitable,
                    processed_at
                FROM daily_vehicle_profitability
                WHERE sim_day = %s
                ORDER BY vehicle_id ASC;
                """,
                (selected_day,),
            )
            rows = [serialize_value(dict(r)) for r in cur.fetchall()]

        total_trips = sum(int(r.get("total_trips") or 0) for r in rows)
        total_fare = round(sum(float(r.get("total_fare") or 0.0) for r in rows), 2)
        total_fuel_cost = round(sum(float(r.get("fuel_cost") or 0.0) for r in rows), 2)
        total_maintenance_cost = round(
            sum(float(r.get("maintenance_cost") or 0.0) for r in rows), 2
        )
        total_cost = round(total_fuel_cost + total_maintenance_cost, 2)
        net_profit = round(sum(float(r.get("net_profit") or 0.0) for r in rows), 2)
        unprofitable_vehicles = [r for r in rows if bool(r.get("is_unprofitable"))]

        return {
            "server_time_utc": utc_now_iso(),
            "available_sim_days": available_days,
            "selected_sim_day": selected_day,
            "kpi_summary": {
                "total_vehicles": len(rows),
                "total_trips": total_trips,
                "total_fare": total_fare,
                "total_fuel_cost": total_fuel_cost,
                "total_maintenance_cost": total_maintenance_cost,
                "total_cost": total_cost,
                "net_profit": net_profit,
                "unprofitable_count": len(unprofitable_vehicles),
            },
            "rows": rows,
            "unprofitable_vehicles": unprofitable_vehicles,
        }
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")


@app.get("/metrics")
def get_metrics(format: Optional[str] = Query(default=None)):
    """Pipeline observability metrics: Prometheus text exposition by default, or ?format=json."""
    try:
        with get_db_cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT (window_start, window_end)) AS processed_windows,
                    MAX(updated_at) AS latest_updated_at
                FROM realtime_zone_metrics;
                """
            )
            rt_row = cur.fetchone()
            processed_windows = int(rt_row["processed_windows"]) if rt_row else 0
            latest_event_age_sec = compute_age_seconds(
                rt_row["latest_updated_at"] if rt_row else None
            )

            total_fleet_alerts = safe_table_count(cur, "fleet_alerts")
            total_health_alerts = safe_table_count(cur, "pipeline_health_alerts")

            cur.execute(
                "SELECT COUNT(DISTINCT sim_day) AS reconciled_days FROM daily_vehicle_profitability;"
            )
            day_row = cur.fetchone()
            reconciled_days = int(day_row["reconciled_days"]) if day_row else 0

        dropped_bad_records = read_dropped_bad_records()

        metrics_payload = {
            "server_time_utc": utc_now_iso(),
            "pipeline_processed_windows_total": processed_windows,
            "pipeline_fleet_alerts_total": total_fleet_alerts,
            "pipeline_health_alerts_total": total_health_alerts,
            "pipeline_reconciled_days_total": reconciled_days,
            "pipeline_latest_event_age_seconds": (
                latest_event_age_sec if latest_event_age_sec is not None else -1.0
            ),
            "pipeline_dropped_bad_records_total": dropped_bad_records,
        }

        if format and format.lower() == "json":
            return JSONResponse(content=metrics_payload)

        age_prom_value = (
            metrics_payload["pipeline_latest_event_age_seconds"]
            if latest_event_age_sec is not None
            else -1.0
        )

        prometheus_lines = [
            "# HELP pipeline_processed_windows_total Total distinct streaming windows processed.",
            "# TYPE pipeline_processed_windows_total counter",
            f"pipeline_processed_windows_total {processed_windows}",
            "",
            "# HELP pipeline_fleet_alerts_total Total vehicle idle alerts generated.",
            "# TYPE pipeline_fleet_alerts_total counter",
            f"pipeline_fleet_alerts_total {total_fleet_alerts}",
            "",
            "# HELP pipeline_health_alerts_total Total pipeline health alerts generated.",
            "# TYPE pipeline_health_alerts_total counter",
            f"pipeline_health_alerts_total {total_health_alerts}",
            "",
            "# HELP pipeline_reconciled_days_total Total simulation days reconciled by batch layer.",
            "# TYPE pipeline_reconciled_days_total gauge",
            f"pipeline_reconciled_days_total {reconciled_days}",
            "",
            "# HELP pipeline_latest_event_age_seconds Age in seconds since latest streaming update (UTC).",
            "# TYPE pipeline_latest_event_age_seconds gauge",
            f"pipeline_latest_event_age_seconds {age_prom_value}",
            "",
            "# HELP pipeline_dropped_bad_records_total Total dropped bad records read from spark_streaming.jsonl.",
            "# TYPE pipeline_dropped_bad_records_total counter",
            f"pipeline_dropped_bad_records_total {dropped_bad_records}",
            "",
        ]
        return PlainTextResponse(
            content="\n".join(prometheus_lines),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")