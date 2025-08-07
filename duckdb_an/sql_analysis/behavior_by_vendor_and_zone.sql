--Average Tip by Vendor
SELECT v.vendor_name,
       AVG(y.tip_amount) AS avg_tip_amount
    FROM taxi_data AS y
        JOIN vendor_names AS v 
            ON y.vendorid = v.vendorid
GROUP BY v.vendor_name
;
--Top 5 Zones by Average Tip
SELECT z.zone,
       AVG(y.tip_amount) AS avg_tip_amount
    FROM taxi_data AS y
        JOIN zone_lookup AS z
            ON y.pickup_zone = z.zone_id
GROUP BY z.zone
ORDER BY avg_tip_amount DESC
LIMIT 5
;
--Average Tip by Vendor across Boroughs
SELECT v.vendor_name,
       z.Borough,
       AVG(y.tip_amount) AS avg_tip_amount
    FROM taxi_data AS y
        JOIN vendor_names AS v 
            ON y.vendorid = v.vendorid
        JOIN taxi_zone_lookup AS z 
            ON y.PULocationID = z.LocationID
  GROUP BY v.vendor_name, z.Borough
ORDER BY v.vendor_name, z.Borough
;
--Average Tip Percentage by Vendor
SELECT v.vendor_name,
       ROUND(AVG(y.tip_amount / y.fare_amount) * 100, 2) AS avg_tip_percentage
    FROM taxi_data AS y
        JOIN vendor_names AS v 
            ON y.vendorid = v.vendorid
  WHERE y.fare_amount > 0
GROUP BY v.vendor_name
;
--Top 3 Tipping Zones per Vendor
SELECT vendor_name, pickup_zone, avg_tip_amount
FROM (
  SELECT v.vendor_name,
         z.Zone AS pickup_zone,
         AVG(y.tip_amount) AS avg_tip_amount,
         RANK() OVER (
           PARTITION BY v.vendor_name
           ORDER BY AVG(y.tip_amount) DESC
         ) AS tip_rank
    FROM taxi_data AS y
        JOIN vendor_names AS v 
            ON y.vendorid = v.vendorid
        JOIN taxi_zone_lookup AS z 
            ON y.PULocationID = z.LocationID
  GROUP BY v.vendor_name, z.Zone
) sub
WHERE tip_rank <= 3
ORDER BY vendor_name, tip_rank
;
