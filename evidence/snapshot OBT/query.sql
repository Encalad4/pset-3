-- Describe
DESCRIBE TABLE ANALYTICS.OBT_TRIPS;

-- Select
SELECT * FROM ANALYTICS.OBT_TRIPS LIMIT 5;

-- Count
SELECT
service_type,
source_year,
COUNT(*) AS total_rows,
FROM ANALYTICS.OBT_TRIPS
GROUP BY service_type, source_year
ORDER BY service_type, source_year;