#standardsql
------------------------------------------------------------
-- Query to get voyages by squid vessels from 2017 to 2020
--
-- NOTE this table using the proto_voyage table with confidence >=2
-- N.Miller Oct 2021
--
-- This is saved in
-- paper_global_squid.squid_vessel_port_visits_withaoi_2017_2020_v20220525
------------------------------------------------------------
--SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (TIMESTAMP("2017-01-01"));
--
  --SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (TIMESTAMP("2020-12-31"));
--
  -- port stops less this value (in hours) excluded and voyages merged
CREATE TEMP FUNCTION min_port_stop() AS (CAST(3 AS INT64));
--
  -- final voyages with durations of less this value (in hours) are excluded
CREATE TEMP FUNCTION min_trip_duration() AS (CAST(2 AS INT64));
--
--
  WITH
------------------------------------------------------------
-- Raw data from the voyage table, removing some known noise
------------------------------------------------------------
  trip_ids AS (
    SELECT *
      FROM (
        SELECT
        ssvid,
        vessel_id,
        CAST(IF(trip_start < TIMESTAMP("1900-01-01"), NULL, trip_start) AS TIMESTAMP) AS trip_start,
        CAST(IF(trip_end > TIMESTAMP("2099-12-31"), NULL, trip_end) AS TIMESTAMP) AS trip_end,
        trip_start_anchorage_id,
        trip_end_anchorage_id
        FROM (
          SELECT *
            FROM `world-fishing-827.pipe_production_v20201001.proto_voyages_c2`
          WHERE trip_start <= maximum()
          AND trip_end >= minimum()
          AND trip_start_anchorage_id != "10000001"
          AND trip_end_anchorage_id != "10000001"))
  ),
------------------------------------------------
-- anchorage ids that represent the Panama Canal
------------------------------------------------
  panama_canal_ids AS (
    SELECT s2id AS anchorage_id
    FROM `gfw_research.named_anchorages`
    WHERE sublabel="PANAMA CANAL"
  ),
-----------------------------------------------------
-- Add ISO3 flag code to trip start and end anchorage
-----------------------------------------------------
  add_trip_start_end_iso3 AS (
    SELECT
    ssvid,
    trip_start,
    trip_end,
    trip_start_anchorage_id,
    b.iso3 AS start_anchorage_iso3,
    trip_end_anchorage_id,
    c.iso3 AS end_anchorage_iso3,
    TIMESTAMP_DIFF(trip_end, trip_start, SECOND) / 3600 AS trip_duration_hr
    FROM trip_ids a
    LEFT JOIN `gfw_research.named_anchorages`  b
    ON a.trip_start_anchorage_id = b.s2id
    LEFT JOIN `gfw_research.named_anchorages`  c
    ON a.trip_end_anchorage_id = c.s2id
    GROUP BY 1,2,3,4,5,6,7,8
  ),
-------------------------------------------------------------------
-- Mark whether start anchorage or end anchorage is in Panama canal
-- This is to remove trips within Panama Canal
-------------------------------------------------------------------
  is_end_port_pan AS (
    SELECT
    ssvid,
    trip_start,
    trip_end,
    trip_start_anchorage_id ,
    start_anchorage_iso3,
    trip_end_anchorage_id,
    end_anchorage_iso3,
    IF (trip_end_anchorage_id IN (
      SELECT anchorage_id FROM panama_canal_ids ),
      TRUE, FALSE ) current_end_is_panama,
    IF (trip_start_anchorage_id IN (
      SELECT anchorage_id FROM panama_canal_ids ),
      TRUE, FALSE ) current_start_is_panama,
    FROM add_trip_start_end_iso3
  ),
------------------------------------------------
-- Add information about
-- whether previous and next ports are in Panama
------------------------------------------------
  add_prev_next_port AS (
    SELECT
    *,
    IFNULL (
      LAG (trip_start, 1) OVER (
        PARTITION BY ssvid
        ORDER BY trip_start ASC ),
      TIMESTAMP ("2000-01-01") ) AS prev_trip_start,
    IFNULL (
      LEAD (trip_end, 1) OVER (
        PARTITION BY ssvid
        ORDER BY trip_start ASC ),
      TIMESTAMP ("2100-01-01") ) AS next_trip_end,
    LAG (current_end_is_panama, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS prev_end_is_panama,
    LEAD (current_end_is_panama, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS next_end_is_panama,
    FROM is_end_port_pan
  ),
---------------------------------------------------------------------------------
-- Mark the start and end of the block. The start of the block is the anchorage
-- just before Panama canal, and the end of the block is the anchorage just after
-- Panama canal (all consecutive trips within Panama canal will be ignored later).
-- If there is no Panama canal involved in a trip, the start/end of the block are
-- the trip start/end of that trip.
---------------------------------------------------------------------------------
  block_start_end AS (
    SELECT
    *,
    IF (prev_end_is_panama, NULL, trip_start) AS block_start,
    IF (current_end_is_panama, NULL, trip_end) AS block_end
    --       IF (current_start_is_panama AND prev_end_is_panama, NULL, trip_start) AS block_start,
    --       IF (current_end_is_panama AND next_start_is_panama, NULL, trip_end) AS block_end
    FROM add_prev_next_port
  ),
-------------------------------------------
-- Find the closest non-Panama ports
-- by looking ahead and back of the records
-------------------------------------------
look_back_and_ahead AS (
    SELECT
    * EXCEPT(block_start, block_end),
    LAST_VALUE (block_start IGNORE NULLS) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS block_start,
    FIRST_VALUE (block_end IGNORE NULLS) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS block_end
    FROM block_start_end
  ),
-------------------------------------------------------------------
-- Within a block, all trips will have the same information
-- about their block (start / end of the block, anchorage start/end
-------------------------------------------------------------------
  blocks_to_be_collapsed_down AS (
    SELECT
    ssvid,
    block_start,
    block_end,
    FIRST_VALUE (trip_start_anchorage_id) OVER (
      PARTITION BY block_start, block_end
      ORDER BY trip_start ASC) AS trip_start_anchorage_id,
    FIRST_VALUE (start_anchorage_iso3) OVER (
      PARTITION BY block_start, block_end
      ORDER BY trip_start ASC) AS start_anchorage_iso3,
    FIRST_VALUE (trip_end_anchorage_id) OVER (
      PARTITION BY block_start, block_end
      ORDER BY trip_end DESC) AS trip_end_anchorage_id,
    FIRST_VALUE (end_anchorage_iso3) OVER (
      PARTITION BY block_start, block_end
      ORDER BY trip_end DESC) AS end_anchorage_iso3,
    FROM look_back_and_ahead
  ),
---------------------------------------------------------------------
-- Blocks get collapsed down to one row, which means a block of trips
-- becomes a complete trip
---------------------------------------------------------------------
  updated_pan_voyages AS (
    SELECT
    ssvid,
    block_start AS trip_start,
    block_end AS trip_end,
    trip_start_anchorage_id,
    start_anchorage_iso3,
    trip_end_anchorage_id,
    end_anchorage_iso3
    FROM blocks_to_be_collapsed_down
    GROUP BY 1,2,3,4,5,6,7
  ),
----------------------------------------------------------------------
-- Identify port stops that are too short, which indicates a vessel
-- to consider its trip as stopping there
-- First of all, add port stop duration (at the end of current voyage)
----------------------------------------------------------------------
  add_port_stop_duration AS (
    SELECT
    * EXCEPT (next_voyage_start),
    TIMESTAMP_DIFF(next_voyage_start, trip_end, SECOND) / 3600 AS port_stop_duration_hr
    FROM (
      SELECT
      *,
      LEAD(trip_start, 1) OVER (PARTITION BY ssvid ORDER BY trip_start ASC) AS next_voyage_start
      FROM updated_pan_voyages)
  ),
---------------------------------------------------------
-- Determine if the current, previous, or next port stops
-- are *too* short, with a threshold
---------------------------------------------------------
  is_port_too_short AS (
    SELECT
    *,
    LAG (current_port_too_short, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC) AS prev_port_too_short,
    LEAD (current_port_too_short, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC) AS next_port_too_short,
    FROM (
      SELECT
      *,
      IF (port_stop_duration_hr < min_port_stop() AND port_stop_duration_hr IS NOT NULL,
          TRUE, FALSE ) AS current_port_too_short
      FROM add_port_stop_duration)
  ),
---------------------------------------------------------------------------------------
-- Mark the start and end of the "voyage". Short port visits are to be combined
-- with the closest prev/next "long" port visit to ignore just "pass-by" trips to ports
---------------------------------------------------------------------------------------
  voyage_start_end AS (
    SELECT
    * EXCEPT (prev_port_too_short, current_port_too_short),
    IF (prev_port_too_short, NULL, trip_start) AS voyage_start,
    IF (current_port_too_short, NULL, trip_end) AS voyage_end
    FROM is_port_too_short
  ),
----------------------------------------------------------------
  -- Find the closest not-too-short port visits in prev/next ports
-- by looking ahead and back of the records
----------------------------------------------------------------
  look_back_and_ahead_for_voyage AS (
    SELECT
    * EXCEPT(voyage_start, voyage_end),
    LAST_VALUE (voyage_start IGNORE NULLS) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS voyage_start,
    FIRST_VALUE (voyage_end IGNORE NULLS) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS voyage_end
    FROM voyage_start_end
  ),
--------------------------------------------------------------------------
-- Within a "voyage", all trips that are to be grouped (due to short stops)
-- will contain the same information about its voyages start/end anchorage
---------------------------------------------------------------------------
  voyages_to_be_collapsed_down AS (
    SELECT
    ssvid,
    voyage_start,
    voyage_end,
    FIRST_VALUE (trip_start_anchorage_id) OVER (
      PARTITION BY voyage_start, voyage_end
      ORDER BY trip_start ASC) AS trip_start_anchorage_id,
    FIRST_VALUE (start_anchorage_iso3) OVER (
      PARTITION BY voyage_start, voyage_end
      ORDER BY trip_start ASC) AS start_anchorage_iso3,
    FIRST_VALUE (trip_end_anchorage_id) OVER (
      PARTITION BY voyage_start, voyage_end
      ORDER BY trip_start DESC) AS trip_end_anchorage_id,
    FIRST_VALUE (end_anchorage_iso3) OVER (
      PARTITION BY voyage_start, voyage_end
      ORDER BY trip_start DESC) AS end_anchorage_iso3,
    FIRST_VALUE (port_stop_duration_hr) OVER (
      PARTITION BY voyage_start, voyage_end
      ORDER BY trip_start DESC) AS port_stop_duration_hr,
    FROM look_back_and_ahead_for_voyage
  ),
----------------------------------------------------------------------
-- Blocks get collapsed down to one row, which means a block of voyage
-- becomes a complete voyage (combining all too-short port visits
----------------------------------------------------------------------
  updated_voyages AS (
    SELECT
    ssvid,
    voyage_start AS trip_start,
    voyage_end AS trip_end,
    trip_start_anchorage_id,
    start_anchorage_iso3,
    trip_end_anchorage_id,
    end_anchorage_iso3,
    port_stop_duration_hr
    FROM voyages_to_be_collapsed_down
    GROUP BY 1,2,3,4,5,6,7,8
  ),
-----------------------------------------------------------
  -- Add information about trip_start and trip_end anchorages
-----------------------------------------------------------
  trip_start_end_label AS (
    SELECT
    ssvid,
    trip_start,
    trip_end,
    trip_start_anchorage_id,
    b.lat AS start_anchorage_lat,
    b.lon AS start_anchorage_lon,
    b.label AS start_anchorage_label,
    b.iso3 AS start_anchorage_iso3,
    trip_end_anchorage_id,
    c.lat AS end_anchorage_lat,
    c.lon AS end_anchorage_lon,
    c.label AS end_anchorage_label,
    c.iso3 AS end_anchorage_iso3,
    TIMESTAMP_DIFF(trip_end, trip_start, SECOND) / 3600 AS trip_duration_hr,
    port_stop_duration_hr
    FROM updated_voyages AS a
    LEFT JOIN `gfw_research.named_anchorages`  AS b
    ON a.trip_start_anchorage_id = b.s2id
    LEFT JOIN `gfw_research.named_anchorages`  AS c
    ON a.trip_end_anchorage_id = c.s2id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  ),
------------------------------------------------------------
  -- Filter all trips to 2 hour duration or no start, or no end
------------------------------------------------------------
  generate_final_trips AS (
    SELECT *,
    EXTRACT(year FROM trip_end) AS year,
    IF(trip_start_anchorage_id = 'NO_PREVIOUS_DATA',
        concat(ssvid,"-",
               format("%012x",
                      timestamp_diff(TIMESTAMP('0001-02-03 00:00:00'),
                                     timestamp("1970-01-01"),
                                     MILLISECOND))),
        concat(ssvid, "-",
               format("%012x",
                      timestamp_diff(trip_start,
                                     timestamp("1970-01-01"),
                                     MILLISECOND))
    )) as gfw_trip_id
    FROM trip_start_end_label
    WHERE
    ((trip_end >= minimum()
      OR trip_end IS NULL) )
    AND (trip_end_anchorage_id = "ACTIVE_VOYAGE"
         OR trip_duration_hr > min_trip_duration()
         OR trip_start_anchorage_id = "NO_PREVIOUS_DATA")
    AND (trip_start <= maximum()
         OR trip_start IS NULL)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ),
----------------------------------------
-- Jigger SSVID in AOIs
----------------------------------------
  jigger_ssvid AS (
    SELECT
    ssvid,
    date,
    geartype,
    flag,
    aoi
    FROM
    `world-fishing-827.paper_global_squid.light_luring_by_region_2017_2020_v20220525`)
----------------------------------------
-- Get squid vessel voyages and
-- allow them to be "open-ended"
----------------------------------------
SELECT
ssvid,
gfw_trip_id,
trip_start,
trip_end,
year,
MIN(date) AS min_date,
MAX(date) AS max_date,
trip_start_anchorage_id,
start_anchorage_lat,
start_anchorage_lon,
start_anchorage_label,
start_anchorage_iso3,
trip_end_anchorage_id,
end_anchorage_lat,
end_anchorage_lon,
end_anchorage_label,
end_anchorage_iso3,
trip_duration_hr,
port_stop_duration_hr,
geartype,
flag,
aoi
FROM
generate_final_trips
INNER JOIN
jigger_ssvid
USING (ssvid)
WHERE ssvid != '659486000' AND
(
(date BETWEEN DATE(trip_start) AND DATE(trip_end)) OR
(trip_start_anchorage_id = 'NO_PREVIOUS_DATA' AND date < DATE(trip_end)) OR
(trip_end_anchorage_id = 'ACTIVE_VOYAGE' AND date > DATE(trip_start))
)
group by 1,2,3,4,5,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22


