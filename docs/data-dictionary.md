# Yellow Taxi data dictionary

## Bronze source

Bronze preserves the official TLC monthly Parquet bytes unchanged. The January and February 2024 files were inspected directly from MinIO during Phase 4; each contains 19 nullable columns and their schemas match. They contain 2,964,624 and 3,007,526 rows respectively. Other months may have different optional fields or casing, so the Silver mapping below is explicit and does not infer that all historical files share this schema.

| Source field | Silver field | Silver type | Use |
|---|---|---|---|
| `VendorID` | `vendor_id` | integer | TLC technology vendor code. |
| `tpep_pickup_datetime` | `pickup_datetime` | timestamp | Pickup time used for temporal checks and downstream analysis. |
| `tpep_dropoff_datetime` | `dropoff_datetime` | timestamp | Dropoff time used for temporal checks. |
| `passenger_count` | `passenger_count` | double | Reported passenger count; nullable. |
| `trip_distance` | `trip_distance` | double | Reported trip distance; required and non-negative. |
| `RatecodeID` | `ratecode_id` | double | TLC rate code; nullable. |
| `store_and_fwd_flag` | `store_and_fwd_flag` | string | Store-and-forward indicator; nullable. |
| `PULocationID` | `pu_location_id` | integer | Pickup TLC location ID; required and range checked. |
| `DOLocationID` | `do_location_id` | integer | Dropoff TLC location ID; required and range checked. |
| `payment_type` | `payment_type` | integer | TLC payment code; nullable. |
| `fare_amount` | `fare_amount` | double | Metered fare component. |
| `extra` | `extra` | double | Miscellaneous extras and surcharges. |
| `mta_tax` | `mta_tax` | double | MTA tax component. |
| `tip_amount` | `tip_amount` | double | Reported tip. |
| `tolls_amount` | `tolls_amount` | double | Reported tolls. |
| `improvement_surcharge` | `improvement_surcharge` | double | TLC improvement surcharge. |
| `total_amount` | `total_amount` | double | Source-reported total; not recomputed in Silver. |
| `congestion_surcharge` | `congestion_surcharge` | double | Congestion surcharge; optional in other periods. |
| `Airport_fee` / `airport_fee` | `airport_fee` | double | Airport fee; optional and case-normalized. |

Every Silver row also contains `source_month`, `source_year`, `source_file`, `source_url`, `ingested_at` (from the Bronze manifest), `pipeline_run_id` (from the Bronze manifest), and `processed_at`. Valid rows have a `quality_warnings` string array. Quarantine rows additionally have a `rejection_reasons` string array.

The TLC user guide maps location IDs 1–263 to taxi zones. The selected 2024 source files also contain IDs 264 and 265; Silver retains those source codes while rejecting values outside 1–265. See the [TLC Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf).

## Iceberg tables

- `silver.yellow_taxi_trips`: rows meeting the required schema and quality rules.
- `silver.yellow_taxi_quarantine`: rows that failed at least one rule, retaining their cast canonical values and lineage with explicit reason codes.

Both tables are partitioned by the time-derived `source_month` (`YYYY-MM`). This matches the monthly input/reprocessing unit and supports replacing one month's data without appending duplicates.
