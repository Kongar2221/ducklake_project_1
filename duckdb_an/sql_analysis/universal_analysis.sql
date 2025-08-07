--Yearly trip Trends Analysis
SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
       COUNT(*) AS total_trips
    FROM taxi_data
  GROUP BY year
ORDER BY year
;
--Trips Per Year By Vendor
SELECT EXTRACT(YEAR FROM y.tpep_pickup_datetime) AS year,
       v.vendor_name,
       COUNT(*) AS trips_count
    FROM taxi_data AS y
        JOIN vendor_names AS v 
            ON y.vendorid = v.vendorid
  GROUP BY year, v.vendor_name
ORDER BY year, v.vendor_name
;
--Average Trip Distance by Year
SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
       AVG(trip_distance) AS avg_trip_distance
    FROM taxi_data
  GROUP BY year
ORDER BY year
;
--Monthly Trip Trend per Year 
SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
       EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
       COUNT(*) AS trips_count
    FROM taxi_data
  GROUP BY year, month
ORDER BY year, month
;
--Year-over-Year Trip Growth
WITH yearly_trips AS (
    SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
           COUNT(*) AS total_trips
        FROM taxi_data
    GROUP BY year
)
SELECT year, total_trips,
       LAG(total_trips) OVER (ORDER BY year) AS prev_year_trips,
       ROUND( (total_trips - LAG(total_trips) OVER (ORDER BY year))
         * 100.0 / LAG(total_trips) OVER (ORDER BY year),
       2) AS pct_change_from_prev_year
    FROM yearly_trips
ORDER BY year
;
--