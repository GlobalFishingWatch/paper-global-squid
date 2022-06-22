--------------------------------------
-- Query to identify squid vessels
-- operating in national waters,
-- regulated waters (NPFC after
-- 2019-11-01), or in unregulated waters
--
-- Table saved here `paper_global_squid.squid_regulatory_positions_eez_v20220525`
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
fra_collaboration.squid_vessel_positions_2017_2020_v20211122
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
fra_collaboration.final_squid_vessel_list_v20211122
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
-- Assign all positions to an REGION.
--------------------------------------
region_positions AS(
SELECT
ssvid,
flag,
date,
area,
region,
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
area,
CASE
WHEN area IN ('nw_pacific','se_pacific','sw_atlantic','nw_indian') THEN 'UNREGULATED'
WHEN area IN ('FLK','CHL','URY','PER','ARG','ECU','OMN','YEM','KOR','JPN','PAK','IND','SOM','RUS') THEN 'NATIONAL'
ELSE NULL
END as region,
geometry
FROM
`paper_global_squid.final_unmerged_squid_eez_regulatory_regions_v20220525`)
--`paper_global_squid.final_squid_regulatory_regions_v20220525`)
WHERE ST_CONTAINS(geometry,st_geogpoint(lon, lat))
GROUP BY 1,2,3,4,5
)
--------------------------------------
-- Identify positions within the NPFC
-- after 2019-11-01 (when squid CMM
-- went into effect)
--------------------------------------
SELECT
*
FROM
(
SELECT
ssvid,
flag,
date,
area,
CASE
WHEN area = 'nw_pacific' AND date >= DATE('2019-11-29') THEN 'RFMO MANAGED'
ELSE region
END AS region,
regional_positions,
regional_total_hours,
regional_fishing_hours
FROM
region_positions
)
