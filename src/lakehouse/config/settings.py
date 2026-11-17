"""Validated application settings sourced from environment variables."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_DEFAULT_MONTHS = ("2024-01", "2024-02")


class ConfigurationError(ValueError):
    """Raised when application configuration is invalid."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Settings used by local application components."""

    source_months: tuple[str, ...] = _DEFAULT_MONTHS
    log_level: str = "INFO"


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    """Load and validate settings from an environment mapping."""
    values = os.environ if environ is None else environ
    raw_months = values.get("LAKEHOUSE_SOURCE_MONTHS", ",".join(_DEFAULT_MONTHS))
    month_values = [month.strip() for month in raw_months.split(",")]
    if not month_values or any(not month for month in month_values):
        raise ConfigurationError("LAKEHOUSE_SOURCE_MONTHS must contain at least one YYYY-MM month")
    months = tuple(month_values)
    invalid_months = [month for month in months if not _MONTH_PATTERN.fullmatch(month)]
    if invalid_months:
        raise ConfigurationError(
            "LAKEHOUSE_SOURCE_MONTHS contains invalid month(s): " + ", ".join(invalid_months)
        )
    if len(set(months)) != len(months):
        raise ConfigurationError("LAKEHOUSE_SOURCE_MONTHS must not contain duplicate months")

    log_level = values.get("LAKEHOUSE_LOG_LEVEL", "INFO").strip().upper()
    if log_level not in _LOG_LEVELS:
        allowed_levels = ", ".join(sorted(_LOG_LEVELS))
        raise ConfigurationError(f"LAKEHOUSE_LOG_LEVEL must be one of: {allowed_levels}")

    return Settings(source_months=months, log_level=log_level)
