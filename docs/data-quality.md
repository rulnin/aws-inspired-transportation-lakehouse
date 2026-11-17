# Silver data quality rules

The Silver job converts the known TLC source aliases into the canonical schema in [the data dictionary](data-dictionary.md). Missing optional fields become typed nulls. Missing required source columns fail the month before a write. Values that cannot be cast are routed to quarantine with an `unparseable_<field>` reason; they are not silently converted to null in valid Silver rows.

| Rule | Behavior | Reason/warning |
|---|---|---|
| Pickup and dropoff timestamps | Both are required and must parse as timestamps. | `missing_pickup_datetime`, `missing_dropoff_datetime`, or `unparseable_<field>` |
| Timestamp order | Dropoff must be at or after pickup. | `dropoff_before_pickup` |
| Trip distance | Required, numeric, and non-negative. | Missing/unparseable reason or `negative_trip_distance` |
| Passenger count | Optional; when present it must parse and be non-negative. | `unparseable_passenger_count` or `negative_passenger_count` |
| Pickup/dropoff location IDs | Required integers from 1–265. IDs 1–263 are mapped taxi zones; 264 and 265 are retained special source codes observed in the selected files. Integer truncation is rejected. | Missing, unparseable, non-integral, or `out_of_range_<field>` |
| Other numeric fields | When provided, values must cast to their canonical numeric type. | `unparseable_<field>`; integer fields also reject fractional values. |
| Monetary values | Negative values remain in Silver because unusual adjustments/records should not be discarded without domain evidence. They are explicitly flagged. | `negative_<field>` in `quality_warnings` |

Any rejection reason sends the complete canonicalized row and its lineage to `silver.yellow_taxi_quarantine`. No reason means the row is written to `silver.yellow_taxi_trips`. A valid row may still carry monetary warning flags. The Spark job logs input, Silver, quarantine, and warning row counts for every source month.

Monthly replacement is applied independently to the valid and quarantine Iceberg tables using the `source_month` partition filter. Re-running a month replaces the prior logical month in each table. If a process fails between the two table commits, the run is failed; rerunning the month repairs both outputs. Bronze remains unchanged.
