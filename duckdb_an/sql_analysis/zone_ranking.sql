--Top 5 Pickup Zones
SELECT z.Zone AS pickup_zone,
       COUNT(*) AS pickup_count
    FROM taxi_data AS y
        JOIN taxi_zone_lookup AS z 
            ON y.PULocationID = z.LocationID
  GROUP BY z.Zone
 ORDER BY pickup_count DESC
LIMIT 5
;
--Top 5 Drop-off Zones in 2024
SELECT z.Zone AS dropoff_zone,
       COUNT(*) AS dropoff_count
    FROM taxi_data AS y
        JOIN taxi_zone_lookup AS z 
            ON y.DOLocationID = z.LocationID
   WHERE EXTRACT(YEAR FROM y.tpep_pickup_datetime) = 2024
  GROUP BY z.Zone
 ORDER BY dropoff_count DESC
LIMIT 5
;
--Trips by Borough
SELECT z.Borough,
       COUNT(*) AS total_trips
    FROM taxi_data AS y
        JOIN taxi_zone_lookup AS z 
            ON y.PULocationID = z.LocationID
  GROUP BY z.Borough
ORDER BY total_trips DESC
;
--Monthly Zone Popularity (2024)
WITH monthly_zone_counts AS (
  SELECT EXTRACT(MONTH FROM y.tpep_pickup_datetime) AS month,
         z.Zone,
         COUNT(*) AS trip_count
    FROM taxi_data AS y
        JOIN taxi_zone_lookup AS z 
            ON y.PULocationID = z.LocationID
  WHERE EXTRACT(YEAR FROM y.tpep_pickup_datetime) = 2024
  GROUP BY month, z.Zone
)
SELECT m.month, m.Zone AS top_zone, m.trip_count
    FROM monthly_zone_counts AS m
        JOIN (
            SELECT month, MAX(trip_count) AS max_trips
                FROM monthly_zone_counts
              GROUP BY month
        ) AS tops 
  ON m.month = tops.month AND m.trip_count = tops.max_trips
ORDER BY m.month
;
--Yearly Zone Rankings
SELECT year, pickup_zone, trips_count, zone_rank
    FROM (
    SELECT EXTRACT(YEAR FROM y.tpep_pickup_datetime) AS year,
            z.Zone AS pickup_zone,
            COUNT(*) AS trips_count,
            RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM y.tpep_pickup_datetime)
            ORDER BY COUNT(*) DESC
            ) AS zone_rank
        FROM taxi_data AS y
            JOIN taxi_zone_lookup AS z 
                ON y.PULocationID = z.LocationID
    GROUP BY year, z.Zone
    ) sub
WHERE zone_rank <= 3
ORDER BY year, zone_rank
;