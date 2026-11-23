import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SIM_DAY_SECONDS = int(os.getenv("SIM_DAY_SECONDS", "180"))
CLOCK_FILE = PROJECT_ROOT / "data" / "sim_start_epoch.txt"
BATCH_INBOX_DIR = PROJECT_ROOT / "data" / "batch_inbox"
LOG_FILE = PROJECT_ROOT / "data" / "logs" / "batch_generator.jsonl"

VEHICLES = [f"VEH-{i:03d}" for i in range(1, 16)]
UNPROFITABLE_VEHICLES = {"VEH-007", "VEH-011", "VEH-014"}

CSV_COLUMNS = [
    "sim_day",
    "vehicle_id",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
]


class JsonlFormatter(logging.Formatter):
    """Formats every log record as a clean single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "component": "batch_generator",
            "message": record.getMessage(),
        }
        extra_data = getattr(record, "extra_data", None)
        if isinstance(extra_data, dict):
            payload.update(extra_data)
        return json.dumps(payload)


def setup_logger() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("batch_generator")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = JsonlFormatter()

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def get_sim_start_epoch() -> float:
    """Reads or creates the same shared epoch used by stream_producer.py."""
    env_epoch = os.getenv("SIM_START_EPOCH")
    if env_epoch:
        return float(env_epoch)

    CLOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with CLOCK_FILE.open("x", encoding="utf-8") as f:
            now = time.time()
            f.write(f"{now:.6f}")
            return now
    except FileExistsError:
        for _ in range(10):
            content = CLOCK_FILE.read_text(encoding="utf-8").strip()
            if content:
                return float(content)
            time.sleep(0.05)
        raise RuntimeError(f"Shared clock file is empty: {CLOCK_FILE}")


def get_sim_day(start_epoch: float) -> int:
    elapsed = max(0.0, time.time() - start_epoch)
    return int(elapsed // SIM_DAY_SECONDS) + 1


def build_daily_expense_rows(sim_day: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for vehicle_id in VEHICLES:
        if vehicle_id in UNPROFITABLE_VEHICLES:
            # Deliberately high costs so reconciliation flags these vehicles as UNPROFITABLE
            fuel_cost = round(random.uniform(13500.0, 19500.0), 2)
            maintenance_cost = round(random.uniform(19000.0, 34000.0), 2)
            distance_covered = round(random.uniform(140.0, 230.0), 2)
            service_flag = True
        else:
            fuel_cost = round(random.uniform(3500.0, 6800.0), 2)
            maintenance_cost = round(random.uniform(400.0, 2200.0), 2)
            distance_covered = round(random.uniform(95.0, 210.0), 2)
            service_flag = random.random() < 0.10

        rows.append(
            {
                "sim_day": sim_day,
                "vehicle_id": vehicle_id,
                "fuel_cost": fuel_cost,
                "maintenance_cost": maintenance_cost,
                "distance_covered": distance_covered,
                "service_flag": service_flag,
            }
        )
    return rows


def write_expense_csv(sim_day: int, logger: logging.Logger) -> Path:
    """Writes CSV atomically (.tmp -> .csv) so downstream Spark jobs never read partial files."""
    BATCH_INBOX_DIR.mkdir(parents=True, exist_ok=True)
    final_path = BATCH_INBOX_DIR / f"expenses_day_{sim_day}.csv"
    tmp_path = BATCH_INBOX_DIR / f".expenses_day_{sim_day}.csv.tmp"

    rows = build_daily_expense_rows(sim_day)
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    os.replace(tmp_path, final_path)

    total_cost = round(sum(r["fuel_cost"] + r["maintenance_cost"] for r in rows), 2)
    logger.info(
        "Generated daily batch expenses CSV",
        extra={
            "extra_data": {
                "sim_day": sim_day,
                "file_path": str(final_path),
                "row_count": len(rows),
                "total_fleet_cost": total_cost,
                "high_cost_vehicles": sorted(UNPROFITABLE_VEHICLES),
            }
        },
    )
    return final_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily fleet expense CSV generator")
    parser.add_argument(
        "--emit-now",
        action="store_true",
        help="Immediately emit the current sim_day CSV once on startup for quick testing.",
    )
    args = parser.parse_args()

    logger = setup_logger()
    start_epoch = get_sim_start_epoch()
    current_day = get_sim_day(start_epoch)
    last_generated_day = 0

    logger.info(
        "Batch generator started",
        extra={
            "extra_data": {
                "sim_day_seconds": SIM_DAY_SECONDS,
                "sim_start_epoch": round(start_epoch, 2),
                "current_sim_day": current_day,
                "batch_inbox": str(BATCH_INBOX_DIR),
            }
        },
    )

    if args.emit_now:
        write_expense_csv(current_day, logger)
        last_generated_day = current_day

    try:
        while True:
            now_day = get_sim_day(start_epoch)
            # As soon as a simulated day completes (e.g. day 1 -> day 2), drop day 1's CSV
            completed_day = now_day - 1
            while last_generated_day < completed_day:
                last_generated_day += 1
                write_expense_csv(last_generated_day, logger)

            time.sleep(2)

    except KeyboardInterrupt:
        logger.info(
            "Batch generator stopped by user",
            extra={"extra_data": {"last_generated_day": last_generated_day}},
        )


if __name__ == "__main__":
    main()