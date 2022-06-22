-------------------------------------
-- Query to identify squid vessel
-- positions in each previously defined
-- squid fishing region.
-- Note: Pulls positions from the
-- previously created squid_vessel_positions
-- table.
-- Note: Pulls light lure/squid vessels
-- from most recent manually reviewed list.
--
-- Result of query saved as
-- paper_global_squid.light_luring_by_region_2017_2020_v20220525
-------------------------------------
--
WITH
-------------------------------------
-- Squid regions. These are also defined
-- in BQ
-------------------------------------
aoi AS (
SELECT
*
FROM
paper_global_squid.final_squid_regions_v20220525),
-------------------------------------
-- Positions for all squid vessels
-- from 2017 to 2021.
-------------------------------------
squid_vessel_positions AS (
SELECT
*
FROM
`paper_global_squid.squid_vessel_positions_2017_2020_v20220525`
),
-------------------------------------
-- List of all squid vessels following
-- manual review
-------------------------------------
full_light_vessels AS (
SELECT
ssvid,
geartype,
flag,
first_timestamp,
last_timestamp
--EXTRACT(year FROM year) AS year
FROM (
SELECT
*,
--GENERATE_DATE_ARRAY(CAST(first_timestamp AS DATE), CAST(last_timestamp AS DATE), INTERVAL 1 YEAR) AS year
FROM
`paper_global_squid.final_squid_vessel_list_v20220525`)
--CROSS JOIN UNNEST(year) as year
),
-------------------------------------
-- Fishing and presence in individual
-- squid regions
-------------------------------------
--
-------------------------------------
-- NW Pacific
-------------------------------------
ssvid_in_nw_pacific AS (
SELECT
ssvid,
EXTRACT(year FROM date) AS year,
EXTRACT(month FROM date) as month,
date,
'nw_pacific' AS aoi,
COUNT(*) positions,
SUM(hours) total_hours,
SUM(IF(night_loitering = 1, hours, 0)) AS fishing_hours
FROM
squid_vessel_positions
WHERE
IF
(ST_CONTAINS( (
SELECT
geometry
FROM
aoi
WHERE area = 'nw_pacific'),
ST_GEOGPOINT(lon,
         lat)),
TRUE,
FALSE)
GROUP BY 1,2,3,4),
-------------------------------------
-- SE Pacific
-------------------------------------
ssvid_in_se_pacific AS (
SELECT
ssvid,
EXTRACT(year FROM date) AS year,
EXTRACT(month FROM date) as month,
date,
'se_pacific' AS aoi,
COUNT(*) positions,
SUM(hours) total_hours,
SUM(IF(night_loitering = 1, hours, 0)) AS fishing_hours
FROM
squid_vessel_positions
WHERE
IF
(ST_CONTAINS( (
SELECT
geometry
FROM
aoi
WHERE area = 'se_pacific'),
ST_GEOGPOINT(lon,
       lat)),
TRUE,
FALSE)
GROUP BY 1,2,3,4),
-------------------------------------
-- SW Atlantic
-------------------------------------
ssvid_in_sw_atlantic AS (
SELECT
ssvid,
EXTRACT(year FROM date) AS year,
EXTRACT(month FROM date) as month,
date,
'sw_atlantic' AS aoi,
COUNT(*) positions,
SUM(hours) total_hours,
SUM(IF(night_loitering = 1, hours, 0)) AS fishing_hours
FROM
squid_vessel_positions
WHERE
IF
(ST_CONTAINS( (
SELECT
geometry
FROM
aoi
WHERE area = 'sw_atlantic'),
ST_GEOGPOINT(lon,
       lat)),
TRUE,
FALSE)
GROUP BY 1,2,3,4),
-------------------------------------
-- NW Indian
-------------------------------------
ssvid_in_nw_indian AS (
SELECT
ssvid,
EXTRACT(year FROM date) AS year,
EXTRACT(month FROM date) as month,
date,
'nw_indian' AS aoi,
COUNT(*) positions,
SUM(hours) total_hours,
SUM(IF(night_loitering = 1, hours, 0)) AS fishing_hours
FROM
squid_vessel_positions
WHERE
IF
(ST_CONTAINS( (
SELECT
geometry
FROM
aoi
WHERE
area = 'nw_indian'),
ST_GEOGPOINT(lon,
       lat)),
TRUE,
FALSE)
GROUP BY 1,2,3,4),

-------------------------------------
-- Combine fishing/presence from all
-- squid regions
-------------------------------------
ssvid_regions_combined AS (SELECT
*
FROM (
SELECT
*
FROM
ssvid_in_nw_pacific
UNION ALL
SELECT
*
FROM
ssvid_in_se_pacific
UNION ALL
SELECT
*
FROM
ssvid_in_sw_atlantic
UNION ALL
SELECT
*
FROM
ssvid_in_nw_indian
)
LEFT JOIN
(SELECT
*
FROM
full_light_vessels)
USING (ssvid)
WHERE date BETWEEN CAST(first_timestamp AS DATE) AND CAST(last_timestamp AS DATE)
),
-------------------------------------
--Identify any vessels (SSVID) represented
-- twice because of two gear types
-------------------------------------
dups AS (
SELECT
ssvid
FROM (
SELECT
ssvid,
COUNT(*) counts
FROM (
SELECT
ssvid,
geartype
FROM
ssvid_regions_combined
GROUP BY
1,2 )
GROUP BY
1 )
WHERE
counts = 2 )
-------------------------------------
-- Remove the duplicates by keeping
-- the non-squidjigger label (typically)
-- this will be 'lift-netter'
-------------------------------------
SELECT
*
FROM (
SELECT
*
FROM
ssvid_regions_combined
WHERE ssvid IN (SELECT ssvid FROM dups) AND
geartype != 'squid_jigger'
UNION ALL
SELECT
*
FROM
ssvid_regions_combined
WHERE ssvid NOT IN (SELECT ssvid FROM dups WHERE ssvid IS NOT NULL)
)
