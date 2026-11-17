"""JSON logging setup with explicit pipeline context fields."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

_CONTEXT_FIELDS = (
    "pipeline_run_id",
    "source_month",
    "component",
    "status",
    "input_count",
    "uploaded_count",
    "skipped_count",
    "output_count",
    "rejected_count",
    "warning_count",
    "monthly_rows",
    "hourly_rows",
    "zone_rows",
    "size_bytes",
    "elapsed_seconds",
    "download_elapsed_seconds",
    "object_key",
)


class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),  # noqa: UP017
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Configure root logging for concise structured console output."""
    logging.basicConfig(level=level, format="%(message)s", force=True)
    handler = logging.getLogger().handlers[0]
    handler.setFormatter(JsonFormatter())
