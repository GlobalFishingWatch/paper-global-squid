-------------------------------------
-- Query to get positions for all
-- squid vessels. This makes later
-- queries cheaper, because all
-- positions have already been pulled.
-- Currently uses a temporary pipe table with
-- night loitering for all squid vessels
--
--N.Miller    ver.2022-05-25
--
-- saved in BQ as
-- paper_global_squid.squid_vessel_positions_2017_2020_v20220525
-------------------------------------
WITH
-------------------------------------
-- noise filter
-------------------------------------
good_segments AS (
  SELECT seg_id FROM
`pipe_production_v20201001.research_segs`
  WHERE
    good_seg IS TRUE
    AND positions > 5
    AND overlapping_and_short IS FALSE ),
-------------------------------------
-- Get positions from 2017 through 2021
-- (will later be filtered to just 2020)
-- note: special list of squid ssvid
-------------------------------------
squid_vessel_positions AS (
SELECT
ssvid,
lon,
lat,
EXTRACT(date FROM timestamp) as date,
night_loitering,
hours
FROM
`pipe_production_v20201001.research_messages` -- temp table with night loitering
WHERE _PARTITIONTIME BETWEEN TIMESTAMP("2017-01-01") AND TIMESTAMP("2020-12-31") AND
seg_id IN (SELECT seg_id FROM good_segments) AND
ssvid IN (SELECT ssvid FROM `paper_global_squid.final_squid_vessel_list_v20220525`
)
)
-------------------------------------
-- Final table
-------------------------------------
SELECT * FROM squid_vessel_positions
