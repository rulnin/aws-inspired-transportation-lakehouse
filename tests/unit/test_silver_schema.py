"""Unit tests for Yellow Taxi source schema resolution."""

import pytest

from lakehouse.processing.schema import (
    CANONICAL_FIELDS,
    LOCATION_ID_MAX,
    LOCATION_ID_MIN,
    MONEY_FIELDS,
    SourceSchemaError,
    normalize_column_name,
    resolve_source_columns,
)


def test_normalize_source_names_handles_tlc_casing_and_punctuation():
    assert normalize_column_name("VendorID") == "vendor_id"
    assert normalize_column_name("PULocationID") == "pu_location_id"
    assert normalize_column_name("Airport_fee") == "airport_fee"
    assert normalize_column_name(" pickup datetime ") == "pickup_datetime"


def test_resolves_actual_selected_month_schema():
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
    ]
    resolved = resolve_source_columns(columns)
    assert len(resolved) == 19
    assert resolved["pickup_datetime"] == "tpep_pickup_datetime"
    assert resolved["ratecode_id"] == "RatecodeID"
    assert resolved["pu_location_id"] == "PULocationID"
    assert resolved["airport_fee"] == "Airport_fee"


def test_optional_columns_can_be_absent_and_known_alias_is_supported():
    resolved = resolve_source_columns(
        [
            "pickup_datetime",
            "dropoff_datetime",
            "trip_distance",
            "pu_location_id",
            "do_location_id",
            "airport_fee",
        ]
    )
    assert resolved["airport_fee"] == "airport_fee"
    assert resolved["passenger_count"] is None
    assert resolved["congestion_surcharge"] is None


def test_missing_required_column_and_ambiguous_alias_fail_clearly():
    with pytest.raises(SourceSchemaError, match="Required source field pickup_datetime"):
        resolve_source_columns(
            ["dropoff_datetime", "trip_distance", "pu_location_id", "do_location_id"]
        )

    fields = [
        "pickup_datetime",
        "dropoff_datetime",
        "trip_distance",
        "pu_location_id",
        "do_location_id",
        "Airport_fee",
        "airport_fee",
    ]
    with pytest.raises(SourceSchemaError, match="Multiple source columns map to airport_fee"):
        resolve_source_columns(fields)


def test_quality_contract_has_documented_location_domain_and_money_fields():
    assert (LOCATION_ID_MIN, LOCATION_ID_MAX) == (1, 265)
    assert "fare_amount" in MONEY_FIELDS
    assert "airport_fee" in MONEY_FIELDS
    assert len(CANONICAL_FIELDS) == 19
