-- Top pickup date/hour/location combinations by valid trip volume.
SELECT source_month,
       pickup_date,
       pickup_hour,
       pickup_location_id,
       trip_count,
       average_trip_distance
FROM iceberg.gold.hourly_trip_demand
WHERE source_month IN ('2024-01', '2024-02')
ORDER BY trip_count DESC
LIMIT 25;
