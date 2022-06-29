# Create a table that contains all the AIS messages within squid fishing areas




-- CREATE TEMP FUNCTION START_DATE() AS (DATE('{date_start}'));
-- CREATE TEMP FUNCTION END_DATE() AS (DATE('{date_end}'));
CREATE TEMP FUNCTION START_DATE() AS (DATE('2017-01-01'));
CREATE TEMP FUNCTION END_DATE() AS (DATE('2022-01-15'));
-- CREATE TEMP FUNCTION START_DATE() AS (DATE('2017-01-01'));
-- CREATE TEMP FUNCTION END_DATE() AS (DATE('2017-01-04'));




##########################################################################
# The functions to convert UTC date to local night

# convert UTC timestamp to Local time using longitude
CREATE TEMP FUNCTION LOCAL_TIMESTAMP(utc_timestamp TIMESTAMP, lon FLOAT64) AS (
     TIMESTAMP_ADD(utc_timestamp, INTERVAL CAST(60*60*lon/15 AS INT64) SECOND)
);

# convert utc timestamp to local night date
# Local Night of 2020-01-01 means (2020-01-01 12:00 ~ 2020-01-02 11:59 in local time)
CREATE TEMP FUNCTION LOCAL_NIGHT(utc_timestamp TIMESTAMP, lon FLOAT64) AS (
    IF(EXTRACT(HOUR FROM LOCAL_TIMESTAMP(utc_timestamp, lon))<=11,
        # if local hour is 0 ~ 11 then subtract 1 day
        DATE_SUB(EXTRACT(DATE FROM LOCAL_TIMESTAMP(utc_timestamp, lon)), INTERVAL 1 DAY),
        # if local hour is 12 ~ 23 then just extract date
        EXTRACT(DATE FROM LOCAL_TIMESTAMP(utc_timestamp, lon)))
);


# Dateline is crossing the nw_pacific, 
# thus the same local night in the nw_pacific is recorded as different date.
# i.e. the local night of 2020-01-02 in the nw_pacific of lon>0 is actually the same as the local night of 2020-01-01 in the nw_pacific of lon<0
CREATE TEMP FUNCTION TREAT_DATELINE_FOR_NW_PACIFIC(local_date DATE, lon FLOAT64, area STRING) AS (
    # ADD 1 day for the area with lon<0 in the nw pacific
    IF(area = 'nw_pacific' and lon<0, DATE_ADD(local_date, INTERVAL 1 DAY), local_date)
);
############################################################################





#################################################################################################
CREATE OR REPLACE TABLE scratch_masaki.pipe_in_the_squid_area_local_night_v20220512 AS

WITH 

########################################
  # This subquery identifies good segments
  good_segments AS (
  SELECT
    seg_id
  FROM
    `pipe_production_v20201001.research_segs`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short
  ),

# AIS messages
ais_messages AS (
  SELECT
    EXTRACT(year FROM timestamp) as year,
    *
  FROM
    # The new AIS pipe table with night loitering 
    `pipe_production_v20201001.research_messages`

  WHERE 
    EXTRACT(date FROM _PARTITIONTIME) BETWEEN START_DATE() and END_DATE() 
    # Use good_segments subquery to only include positions from good segments
    AND seg_id IN (
      SELECT
        seg_id
      FROM
        good_segments)
),

# vessel info table, used for vessel class.
vessel_info_table as (
    select 
        year,
        ssvid,
        best.best_flag,
        best.best_vessel_class,
        best.best_length_m,
        -- best.best_tonnage_gt,
    from
        `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220101` 
    where
        -- This condition exclues best_vessel_class = 'gear' and retains records having `best.best_vessel_class is null`.
        not (best.best_vessel_class is not null and best.best_vessel_class = 'gear')   
),

-- Squid Fishing area
aoi_vertical as (
SELECT
    *
FROM 
    `world-fishing-827.fra_collaboration.squid_region_aois_v20211016`
),


########################################################################

# Extract AIS only in the squid area
ais_messages_squid_area as (
select
  * except(geometry)
from
  ais_messages a
  
  # Only extract points in the squid area
  INNER JOIN aoi_vertical c
  ON ST_CONTAINS(c.geometry , ST_GEOGPOINT(a.lon, a.lat))

  # add vessel info
  LEFT JOIN vessel_info_table
  USING(ssvid, year)
)

########################################################################
########################################################################
select
    # local night date
    TREAT_DATELINE_FOR_NW_PACIFIC(LOCAL_NIGHT(timestamp, lon), lon, area) as date,
    *,
from
    ais_messages_squid_area








