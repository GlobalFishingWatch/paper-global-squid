#standardsql
-----------------------------------------
-- Identify carrier positions in squid
-- regions for carriers that have at
-- least one loitering event in a squid
-- region. These may still be carriers
-- that primarily meet longlines given
-- the overlap in the Pacific.
--
-- results are saved in paper_global_squid.carrier_positions_in_squidregions_v20220525
-----------------------------------------
--
--SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (timestamp("2017-01-01"));
--
--SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (timestamp("2020-12-31"));
-----------------------------------------------
#standardsql
-----------------------------------------
-- Squid AOIs
-----------------------------------------
WITH aoi AS (
    SELECT * FROM `paper_global_squid.final_squid_regions_v20220525`
),

-----------------------------------------
-- carrier list for vessels that have a
-- loitering event in the squid regions
-----------------------------------------
carrier_vessels AS (
    SELECT
         identity.ssvid AS ssvid,
         identity.imo AS imo_ais,
         identity.n_shipname AS shipname_ais,
         identity.n_callsign AS callsign_ais,
         identity.flag AS flag,
         first_timestamp,
         last_timestamp,
         array_to_string(feature.geartype, "|") AS class
    FROM `vessel_database.all_vessels_v20210901`
    LEFT JOIN unnest(activity)
    WHERE matched
           AND is_carrier
         AND identity.ssvid IN (
             SELECT carrier_ssvid
             FROM
                 `paper_global_squid.carriers_with_loitering_in_squid_regions_v20220525`
 )
         AND first_timestamp <= maximum()
         AND last_timestamp >= minimum()
-- duplicate reefer/specialized reefer
         AND NOT (identity.ssvid = '412440033'
             AND identity.n_callsign = 'BMWA')
         AND identity.ssvid != '441043000'),

-----------------------------------------
-- Get all carrier positions
-----------------------------------------
carrier_positions AS (
    SELECT
         ssvid,
         timestamp,
         lat,
         lon,
         hours,
         cast(timestamp AS DATE) AS date
    FROM (
             SELECT
                 ssvid,
                 timestamp,
                 lat,
                 lon,
                 hours
             FROM
                 `pipe_production_v20201001.research_messages`
             WHERE
                 _partitiontime BETWEEN minimum() AND maximum())
    INNER JOIN
         (SELECT
                 ssvid,
                 first_timestamp,
                 last_timestamp
             FROM
                 carrier_vessels)
         USING (ssvid)
    WHERE
         timestamp BETWEEN first_timestamp AND last_timestamp
),

-----------------------------------------
-- Carrier Positions in Individual Regions
-----------------------------------------
--
-----------------------------------------
-- NW Pacific
-----------------------------------------
ssvid_in_nw_pacific AS (
    SELECT
         ssvid,
         date,
         'nw_pacific' AS aoi,
         extract(year FROM date) AS year,
         extract(month FROM date) AS month,
         count(*) AS positions,
         sum(hours) AS total_hours
    --SUM(IF(nnet_score > 0.5, hours, 0)) AS fishing_hours
    FROM
         carrier_positions
    WHERE
         if(st_contains( (
                     SELECT
                     geometry
                     FROM
                         aoi
                         WHERE area = 'nw_pacific'),
                 st_geogpoint(lon,
                     lat)),
             TRUE,
             FALSE)
    GROUP BY 1, 2, 3, 4 ,5),

-----------------------------------------
-- SE Pacific
-----------------------------------------
ssvid_in_se_pacific AS (
    SELECT
         ssvid,
         date,
         'se_pacific' AS aoi,
         extract(year FROM date) AS year,
         extract(month FROM date) AS month,
         count(*) AS positions,
         sum(hours) AS total_hours
    --SUM(IF(nnet_score > 0.5, hours, 0)) AS fishing_hours
    FROM
         carrier_positions
    WHERE
         if(st_contains( (
                     SELECT
                     geometry
                     FROM
                         aoi
                         WHERE area = 'se_pacific'),
                 st_geogpoint(lon,
                     lat)),
             TRUE,
             FALSE)
    GROUP BY 1, 2, 3, 4 ,5),

-----------------------------------------
-- SW Atlantic
-----------------------------------------
ssvid_in_sw_atlantic AS (
    SELECT
         ssvid,
         date,
         'sw_atlantic' AS aoi,
         extract(year FROM date) AS year,
         extract(month FROM date) AS month,
         count(*) AS positions,
         sum(hours) AS total_hours
    --SUM(IF(nnet_score > 0.5, hours, 0)) AS fishing_hours
    FROM
         carrier_positions
    WHERE
         if(st_contains( (
                     SELECT
                     geometry
                     FROM
                         aoi
                         WHERE area = 'sw_atlantic'),
                 st_geogpoint(lon,
                     lat)),
             TRUE,
             FALSE)
    GROUP BY 1, 2, 3, 4, 5),
-----------------------------------------
-- NW Indian
-----------------------------------------
ssvid_in_nw_indian AS (
    SELECT
         ssvid,
         date,
         'nw_indian' AS aoi,
         extract(year FROM date) AS year,
         extract(month FROM date) AS month,
         count(*) AS positions,
         sum(hours) AS total_hours
    --SUM(IF(nnet_score > 0.5, hours, 0)) AS fishing_hours
    FROM
         carrier_positions
    WHERE
         if(st_contains( (
                     SELECT
                     geometry
                     FROM
                         aoi
                         WHERE area = 'nw_indian'),
                 st_geogpoint(lon,
                     lat)),
             TRUE,
             FALSE)
    GROUP BY 1, 2, 3, 4, 5),
------------------------------------------
-- Combine carrier positions from all regions
------------------------------------------
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
 ) AS a
    LEFT JOIN
         (SELECT
                 *
             FROM
                 carrier_vessels) AS b
         USING(ssvid)
    WHERE
         date BETWEEN cast(
             first_timestamp AS DATE
 ) AND cast(last_timestamp AS DATE)
)

------------------------------------------
-- Final table
------------------------------------------
SELECT
ssvid,
date,
aoi,
year,
month,
positions,
total_hours,
imo_ais,
shipname_ais,
callsign_ais,
flag,
first_timestamp,
last_timestamp,
class
FROM
ssvid_regions_combined
