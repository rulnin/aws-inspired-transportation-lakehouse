# Gold metrics

Gold tables summarize rows accepted into `lakehouse.silver.yellow_taxi_trips`. Quarantined
records are outside every Gold metric. Reprocessing a source month replaces that month's
partition in each table.

## Business questions

- How many valid trips and how much source-reported revenue were recorded in each source month?
- When and where does pickup demand occur by hour?
- Which pickup zones account for the most trips and source-reported revenue?

## Metric definitions and table grains

| Table | Grain | Measures and definitions |
|---|---|---|
| `gold.monthly_trip_summary` | One row per `month` (`YYYY-MM`, the Bronze source month). | `trip_count` counts valid Silver rows. `total_revenue` is the sum of `total_amount`; null amounts contribute zero. `average_trip_distance`, `average_fare`, and `average_tip` are averages of the corresponding non-null Silver fields. |
| `gold.hourly_trip_demand` | One row per source month, pickup calendar date, pickup hour (0–23), and pickup location ID. | `trip_count` counts valid Silver rows. `average_trip_distance` averages non-null distances. Pickup date/hour come from `pickup_datetime`. |
| `gold.pickup_zone_summary` | One row per source month and pickup location ID. | `trip_count` counts valid Silver rows. `total_revenue` sums `total_amount`, with null amounts contributing zero. `pickup_zone` and `borough` come from the TLC Taxi Zone Lookup. |

Negative monetary values retained by Silver are included as reported; these measures are not
cash-settlement or profit estimates. A missing zone match keeps its location ID and has null
labels. The lookup includes special IDs outside the TLC guide's standard 1–263 map. Averages
ignore null values, so they may have different denominators from `trip_count`.

The batch checks monthly, hourly, and zone trip counts against the selected Silver month before
publishing. Monthly and zone revenue, plus the monthly averages, are also reconciled to Silver.
Hourly aggregation is grouped by source month to keep reruns and source-month comparisons
unambiguous.

## Trino examples

See [monthly summary](../sql/trino/monthly_trip_summary.sql),
[hourly pickup demand](../sql/trino/hourly_trip_demand.sql), and
[pickup zones](../sql/trino/pickup_zone_summary.sql). They use Trino's `iceberg` catalog.

The optional zone table uses the official [NYC TLC trip-record data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
lookup CSV, stored in MinIO at `reference/taxi_zone_lookup.csv`.
