-- Rank pickup zones by valid trip volume and source-reported revenue.
SELECT pickup_location_id,
       pickup_zone,
       borough,
       sum(trip_count) AS trip_count,
       sum(total_revenue) AS total_revenue
FROM iceberg.gold.pickup_zone_summary
WHERE source_month IN ('2024-01', '2024-02')
GROUP BY pickup_location_id, pickup_zone, borough
ORDER BY trip_count DESC
LIMIT 25;
