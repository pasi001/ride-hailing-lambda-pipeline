"""
Lightweight structured JSONL logger for the Ride-Hailing Lambda Pipeline.

- Zero external dependencies (Python standard library only).
- Strictly UTC ISO-8601 timestamps (e.g., 2026-11-24T09:55:00Z).
- Writes one JSON object per line to data/logs/<stage>.jsonl.
- Windows-safe append mode (does not hold persistent file locks).
- Non-intrusive: keeps existing spark_streaming.jsonl untouched for FastAPI /metrics.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Automatically resolve project root: .../ride-hailing-lambda-pipeline
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = PROJECT_ROOT / "data" / "logs"

VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format with trailing 'Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StructuredLogger:
    """Reusable UTC JSONL + concise console logger for any pipeline stage."""

    def __init__(
        self,
        stage: str,
        log_dir: Path | str | None = None,
        console: bool = True,
    ) -> None:
        self.stage = stage.strip()
        if not self.stage:
            raise ValueError("stage name must be a non-empty string")

        self.log_dir = Path(log_dir) if log_dir is not None else DEFAULT_LOG_DIR
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.log_dir / f"{self.stage}.jsonl"
        self.console = console

    def log(
        self,
        level: str,
        event: str,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        """Write a structured UTC JSONL event and optionally print a concise console line."""
        level_upper = level.upper().strip()
        if level_upper not in VALID_LEVELS:
            level_upper = "INFO"

        record: dict[str, Any] = {
            "timestamp": utc_now_iso(),
            "stage": self.stage,
            "level": level_upper,
            "event": event,
            "metrics": metrics if metrics is not None else {},
        }
        if message:
            record["message"] = message

        serialized = json.dumps(record, ensure_ascii=False, default=str)

        # Short-lived append avoids Windows file-locking conflicts with FastAPI readers
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(serialized + "\n")

        if self.console:
            metrics_str = json.dumps(record["metrics"], ensure_ascii=False, default=str)
            msg_part = f" | {message}" if message else ""
            print(
                f"[{record['timestamp']}] [{level_upper:<8}] [{self.stage}] "
                f"{event}{msg_part} | metrics={metrics_str}",
                flush=True,
            )

        return record

    def debug(
        self,
        event: str,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        return self.log("DEBUG", event, metrics=metrics, message=message)

    def info(
        self,
        event: str,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        return self.log("INFO", event, metrics=metrics, message=message)

    def warning(
        self,
        event: str,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        return self.log("WARNING", event, metrics=metrics, message=message)

    def error(
        self,
        event: str,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        return self.log("ERROR", event, metrics=metrics, message=message)

    def critical(
        self,
        event: str,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        return self.log("CRITICAL", event, metrics=metrics, message=message)


def get_logger(
    stage: str,
    log_dir: Path | str | None = None,
    console: bool = True,
) -> StructuredLogger:
    """Factory helper to create a StructuredLogger for a given pipeline stage."""
    return StructuredLogger(stage=stage, log_dir=log_dir, console=console)