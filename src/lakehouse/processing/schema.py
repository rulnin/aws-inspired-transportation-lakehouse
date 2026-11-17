"""Canonical Yellow Taxi schema and source-column resolution."""

from __future__ import annotations

import re
from dataclasses import dataclass


class SourceSchemaError(ValueError):
    """Raised when a source schema cannot be mapped without ambiguity."""


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One canonical field and its accepted source aliases."""

    name: str
    spark_type: str
    aliases: tuple[str, ...]
    required: bool = False


CANONICAL_FIELDS = (
    FieldSpec("vendor_id", "int", ("VendorID", "vendor_id")),
    FieldSpec("pickup_datetime", "timestamp", ("tpep_pickup_datetime", "pickup_datetime"), True),
    FieldSpec("dropoff_datetime", "timestamp", ("tpep_dropoff_datetime", "dropoff_datetime"), True),
    FieldSpec("passenger_count", "double", ("passenger_count",)),
    FieldSpec("trip_distance", "double", ("trip_distance",), True),
    FieldSpec("ratecode_id", "double", ("RatecodeID", "ratecode_id")),
    FieldSpec("store_and_fwd_flag", "string", ("store_and_fwd_flag",)),
    FieldSpec("pu_location_id", "int", ("PULocationID", "pu_location_id"), True),
    FieldSpec("do_location_id", "int", ("DOLocationID", "do_location_id"), True),
    FieldSpec("payment_type", "int", ("payment_type",)),
    FieldSpec("fare_amount", "double", ("fare_amount",)),
    FieldSpec("extra", "double", ("extra",)),
    FieldSpec("mta_tax", "double", ("mta_tax",)),
    FieldSpec("tip_amount", "double", ("tip_amount",)),
    FieldSpec("tolls_amount", "double", ("tolls_amount",)),
    FieldSpec("improvement_surcharge", "double", ("improvement_surcharge",)),
    FieldSpec("total_amount", "double", ("total_amount",)),
    FieldSpec("congestion_surcharge", "double", ("congestion_surcharge",)),
    FieldSpec("airport_fee", "double", ("Airport_fee", "airport_fee")),
)

LOCATION_ID_MIN = 1
LOCATION_ID_MAX = 265
MONEY_FIELDS = (
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
)


def normalize_column_name(name: str) -> str:
    """Convert source names from mixed case and punctuation to snake_case."""
    value = name.strip()
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return value.strip("_").lower()


def resolve_source_columns(source_columns: list[str] | tuple[str, ...]) -> dict[str, str | None]:
    """Map canonical names to source columns; missing optional fields map to None."""
    normalized: dict[str, list[str]] = {}
    for source_name in source_columns:
        normalized.setdefault(normalize_column_name(source_name), []).append(source_name)

    resolved: dict[str, str | None] = {}
    for field in CANONICAL_FIELDS:
        candidates = {
            source_name
            for alias in field.aliases
            for source_name in normalized.get(normalize_column_name(alias), [])
        }
        if len(candidates) > 1:
            raise SourceSchemaError(
                f"Multiple source columns map to {field.name}: {sorted(candidates)}"
            )
        source_name = next(iter(candidates), None)
        if source_name is None and field.required:
            raise SourceSchemaError(
                f"Required source field {field.name} is missing; expected one of {field.aliases}"
            )
        resolved[field.name] = source_name
    return resolved
