import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load environment variables from project root .env if present
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ride_events")
SIM_DAY_SECONDS = int(os.getenv("SIM_DAY_SECONDS", "180"))
EMIT_INTERVAL_SECONDS = float(os.getenv("EMIT_INTERVAL_SECONDS", "2"))
MALFORMED_RATE = 0.02

CLOCK_FILE = PROJECT_ROOT / "data" / "sim_start_epoch.txt"
LOG_FILE = PROJECT_ROOT / "data" / "logs" / "stream_producer.jsonl"

# Colombo GPS Bounding Boxes: (min_lat, max_lat, min_lon, max_lon)
ZONES = {
    "Colombo_Central": (6.9150, 6.9400, 79.8400, 79.8650),
    "Nugegoda": (6.8550, 6.8800, 79.8800, 79.9050),
    "Dehiwala": (6.8350, 6.8600, 79.8600, 79.8800),
    "Borella": (6.9050, 6.9250, 79.8700, 79.8900),
    "Kaduwela": (6.9250, 6.9450, 79.9700, 79.9950),
}
ZONE_NAMES = list(ZONES.keys())


class JsonlFormatter(logging.Formatter):
    """Formats every log record as a clean single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "component": "stream_producer",
            "message": record.getMessage(),
        }
        extra_data = getattr(record, "extra_data", None)
        if isinstance(extra_data, dict):
            payload.update(extra_data)
        return json.dumps(payload)


def setup_logger() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("stream_producer")
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
    """Returns a shared epoch timestamp so stream and batch stay synced on sim_day."""
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


def random_coords_in_zone(zone_name: str) -> tuple[float, float]:
    min_lat, max_lat, min_lon, max_lon = ZONES[zone_name]
    return (
        round(random.uniform(min_lat, max_lat), 6),
        round(random.uniform(min_lon, max_lon), 6),
    )


class VehicleSimulator:
    """Maintains realistic per-vehicle state transitions across ticks."""

    def __init__(self, index: int) -> None:
        self.vehicle_id = f"VEH-{index:03d}"
        self.driver_id = f"DRV-{index:03d}"
        self.zone = random.choice(ZONE_NAMES)
        self.lat, self.lon = random_coords_in_zone(self.zone)
        self.status = "idle"
        self.ticks_remaining = random.randint(1, 3)
        self.trip_id: Optional[str] = None
        self.trip_counter = 0

    def _move_slightly(self) -> None:
        min_lat, max_lat, min_lon, max_lon = ZONES[self.zone]
        delta_lat = random.uniform(-0.0018, 0.0018)
        delta_lon = random.uniform(-0.0018, 0.0018)
        self.lat = round(min(max(self.lat + delta_lat, min_lat), max_lat), 6)
        self.lon = round(min(max(self.lon + delta_lon, min_lon), max_lon), 6)

    def next_event(self, sim_day: int, elapsed_sec: float) -> tuple[Dict[str, Any], Optional[str]]:
        """Generates the next telemetry event and optional anomaly tag."""
        force_long_idle = self.vehicle_id == "VEH-003" and (10.0 <= (elapsed_sec % 120.0) <= 75.0)
        fare = 0.0

        if force_long_idle:
            self.status = "idle"
            self.trip_id = None
            speed = 0.0
        else:
            self.ticks_remaining -= 1
            if self.ticks_remaining <= 0:
                if self.status == "idle":
                    self.status = "enroute"
                    self.trip_counter += 1
                    self.trip_id = f"TRP-D{sim_day}-{self.vehicle_id[-3:]}-{self.trip_counter:04d}"
                    self.ticks_remaining = random.randint(2, 3)
                elif self.status == "enroute":
                    self.status = "on_trip"
                    self.ticks_remaining = random.randint(3, 5)
                else:
                    self.status = "idle"
                    self.trip_id = None
                    if random.random() < 0.35:
                        self.zone = random.choice(ZONE_NAMES)
                        self.lat, self.lon = random_coords_in_zone(self.zone)
                    self.ticks_remaining = random.randint(1, 3)

            if self.status == "idle":
                speed = 0.0
            elif self.status == "enroute":
                self._move_slightly()
                speed = round(random.uniform(18.0, 48.0), 2)
            else:
                self._move_slightly()
                speed = round(random.uniform(15.0, 58.0), 2)
                if self.ticks_remaining == 1:
                    fare = round(random.uniform(950.0, 2600.0), 2)

        event: Dict[str, Any] = {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": self.lat,
            "lon": self.lon,
            "speed": speed,
            "status": self.status,
            "fare": fare,
            "event_timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "sim_day": sim_day,
        }

        # Inject ~2% malformed records (skip VEH-003 during its long-idle demo window)
        anomaly: Optional[str] = None
        if not force_long_idle and random.random() < MALFORMED_RATE:
            anomaly = random.choice(["negative_fare", "negative_speed", "null_vehicle_id"])
            if anomaly == "negative_fare":
                event["fare"] = round(-random.uniform(100.0, 900.0), 2)
            elif anomaly == "negative_speed":
                event["speed"] = round(-random.uniform(5.0, 45.0), 2)
            elif anomaly == "null_vehicle_id":
                event["vehicle_id"] = None

        return event, anomaly


def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        acks="all",
        retries=3,
        linger_ms=50,
    )


def main() -> None:
    logger = setup_logger()
    start_epoch = get_sim_start_epoch()
    fleet = [VehicleSimulator(i) for i in range(1, 16)]

    try:
        producer = create_kafka_producer()
    except KafkaError as exc:
        logger.error(
            "Failed to connect to Kafka broker",
            extra={"extra_data": {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS, "error": str(exc)}},
        )
        sys.exit(1)

    logger.info(
        "Stream producer started",
        extra={
            "extra_data": {
                "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "topic": KAFKA_TOPIC,
                "sim_day_seconds": SIM_DAY_SECONDS,
                "sim_start_epoch": round(start_epoch, 2),
                "fleet_size": len(fleet),
            }
        },
    )

    total_sent = 0
    total_malformed = 0

    try:
        while True:
            tick_start = time.time()
            elapsed = max(0.0, tick_start - start_epoch)
            sim_day = get_sim_day(start_epoch)
            tick_malformed = 0

            for vehicle in fleet:
                event, anomaly = vehicle.next_event(sim_day=sim_day, elapsed_sec=elapsed)
                key = event["vehicle_id"]
                producer.send(KAFKA_TOPIC, key=key, value=event)
                total_sent += 1

                if anomaly:
                    tick_malformed += 1
                    total_malformed += 1
                    logger.warning(
                        "Injected malformed record",
                        extra={
                            "extra_data": {
                                "anomaly": anomaly,
                                "sim_day": sim_day,
                                "original_vehicle": vehicle.vehicle_id,
                                "event": event,
                            }
                        },
                    )

            producer.flush()

            logger.info(
                "Published telemetry batch",
                extra={
                    "extra_data": {
                        "sim_day": sim_day,
                        "events_in_tick": len(fleet),
                        "malformed_in_tick": tick_malformed,
                        "total_sent": total_sent,
                        "total_malformed": total_malformed,
                        "veh003_long_idle_active": 10.0 <= (elapsed % 120.0) <= 75.0,
                    }
                },
            )

            sleep_for = max(0.0, EMIT_INTERVAL_SECONDS - (time.time() - tick_start))
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        logger.info(
            "Stream producer stopped by user",
            extra={"extra_data": {"total_sent": total_sent, "total_malformed": total_malformed}},
        )
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()