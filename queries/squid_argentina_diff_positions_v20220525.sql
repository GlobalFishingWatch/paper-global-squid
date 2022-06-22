--------------------------------------
-- Query to identify squid vessels
-- operating in national waters,
-- regulated waters (NPFC after
-- 2019-11-01), or in unregulated waters
--
-- Table saved here `paper_global_squid.squid_argentina_diff_positions_v20220525`
--------------------------------------
--
CREATE TEMPORARY FUNCTION format_gridcode (lon FLOAT64, lat FLOAT64) AS (
    FORMAT("lon:%+07.2f_lat:%+07.2f", ROUND(lon /0.01)*0.01, ROUND(lat /0.01)*0.01)
);
--------------------------------------
-- Get positions for squid vessels
--------------------------------------
WITH filtered_positions AS (
SELECT
*  EXCEPT(first_timestamp, last_timestamp)
FROM (
SELECT
*,
format_gridcode(lon, lat) as gridcode
FROM
paper_global_squid.squid_vessel_positions_2017_2020_v20220525
WHERE date >= DATE('2017-01-01') AND date <= DATE('2020-12-31'))
INNER JOIN
 (SELECT
 ssvid,
 CASE
 WHEN ssvid = '412331123' THEN TIMESTAMP('2017-09-01')
 ELSE first_timestamp
 END AS first_timestamp,
 last_timestamp,
 flag
 FROM
paper_global_squid.final_squid_vessel_list_v20220525
 )
 USING (ssvid)
 WHERE date BETWEEN CAST(first_timestamp AS DATE) AND CAST(last_timestamp AS DATE)
),
--------------------------------------
-- Filter positions away from port
--------------------------------------
squid_positions_from_shore AS (
SELECT
a.* EXCEPT(gridcode)
FROM (
SELECT
*
FROM
filtered_positions) AS a
INNER JOIN
(SELECT
gridcode,
distance_from_port_m
FROM
pipe_static.spatial_measures_20201105
WHERE distance_from_port_m > 10000)
USING (gridcode)
),
--------------------------------------
-- Get squid positions data
-- calculate summaries
--------------------------------------
-- squid_positions AS (
-- SELECT
-- ssvid,
-- EXTRACT(year FROM date) AS year,
-- EXTRACT(month FROM date) as month,
-- date,
-- COUNT(*) positions,
-- SUM(hours) total_hours,
-- SUM(IF(night_loitering = 1, hours, 0)) AS fishing_hours
-- FROM
-- squid_positions_from_shore
-- GROUP BY 1,2,3,4,5
-- ),
--------------------------------------
-- Assign all positions to an REGION
-- or high seas (NULL).
--------------------------------------
region_positions AS(
SELECT
ssvid,
flag,
EXTRACT(year from date) AS year,
date,
COUNT(*) AS regional_positions,
SUM(hours) regional_total_hours,
SUM(IF(night_loitering = 1, hours, 0)) AS regional_fishing_hours
FROM (
SELECT
*
FROM
squid_positions_from_shore)
CROSS JOIN
(SELECT
ST_GEOGFROMTEXT(WKT, make_valid => TRUE) AS shape
FROM
paper_global_squid.argentina_eez_diff_wkt)
WHERE ST_CONTAINS(shape,st_geogpoint(lon, lat))
GROUP BY 1,2,3,4
)

SELECT * FROM region_positions
