-- Monthly trip volume, source-reported revenue, and average trip metrics.
SELECT month,
       trip_count,
       total_revenue,
       average_trip_distance,
       average_fare,
       average_tip
FROM iceberg.gold.monthly_trip_summary
WHERE month BETWEEN '2024-01' AND '2024-02'
ORDER BY month;
