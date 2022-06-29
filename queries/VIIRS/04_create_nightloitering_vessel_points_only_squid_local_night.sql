# Create a table that contains daily night loitering information 
# for squid vessels in the squid fishing areas

#######################################################################
-- CREATE TEMP FUNCTION START_DATE() AS (DATE('{date_start}'));
-- CREATE TEMP FUNCTION END_DATE() AS (DATE('{date_end}'));
-- CREATE TEMP FUNCTION START_DATE() AS (DATE('2017-01-01'));
-- CREATE TEMP FUNCTION END_DATE() AS (DATE('2017-01-04'));
CREATE TEMP FUNCTION START_DATE() AS (DATE('2017-01-01'));
CREATE TEMP FUNCTION END_DATE() AS (DATE('2022-04-15'));



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


CREATE OR REPLACE TABLE scratch_masaki.nightloitering_vessel_points_only_squid_local_night_v20220512 AS


WITH 

# AIS messagges for squid vessels in the squid areas
ais_squid_area as (
  SELECT
    *,
    # 0.1 degree grid
    cast(round(10*lat) as int64) as lat_bin,
    cast(round(10*lon) as int64) as lon_bin,
  FROM
    `scratch_masaki.pipe_in_the_squid_area_local_night_v20220512`
  WHERE
    date between START_DATE() and END_DATE()
    # extract only Nate's squid vessels
    and ssvid IN (select distinct ssvid from fra_collaboration.squid_night_loitering_temp)
),






########################################################################
# By using cloud mask data, calculate the time differences between AIS messages and VIIRS detections.
# This time differences is not used for final analysis.
# These are just remnants of past analytical efforts.

# cloud mask data in the squid area
cloud_mask as (
SELECT
    # local night date
    TREAT_DATELINE_FOR_NW_PACIFIC(LOCAL_NIGHT(StartDateTime, lon), lon, area) as date,
    TIMESTAMP_ADD(StartDateTime, INTERVAL 3 MINUTE) as viirs_timestamp,
    * except(date),
    
FROM
    `scratch_masaki.cloud_mask_10th_degree_grid_squid_area`    
WHERE
    # utc date
    date between START_DATE() and END_DATE()
),

# Select orbit having smallest zenith angle for each grid and local night
daily_cloud_mask as (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY date, lat_bin, lon_bin ORDER BY mean_sensor_zenith) as rownum
FROM
    cloud_mask
QUALIFY
    rownum = 1
),

# Give each AIS message a time difference from VIIRS detection timing
# based on the AIS position
ais_messages_with_timediff AS (
  SELECT
    a.*,
    IF(night_loitering = 1, hours, 0.0) AS fishing_hours,
    b.viirs_timestamp,
    b.mean_integer_cloud_mask,
    # Absolute time difference between VIIRS and AIS messages
    ABS(TIMESTAMP_DIFF(a.timestamp, b.viirs_timestamp, SECOND)) as abs_time_diff_sec
  FROM 
    ais_squid_area a
    LEFT JOIN
    daily_cloud_mask b
    using(date, lon_bin, lat_bin)
),

# Extract only AIS messages having minimum time differences to VIIRS detection timing
closest_ais_message as (
select 
    *,
    ROW_NUMBER() OVER (PARTITION BY date, area, ssvid ORDER BY abs_time_diff_sec) as rownum
from
    ais_messages_with_timediff
QUALIFY
    rownum = 1
),

###########################################################################



###########################################################################
# Calculate daily night loitering hours for eash squid vessels
daily_squid_fishing as (
SELECT
    date,
    area,
    ssvid,
    sum(fishing_hours) as fishing_hours,
    sum(hours) as hours,
    AVG(mean_integer_cloud_mask) as mean_integer_cloud_mask_avg
FROM
    ais_messages_with_timediff 
GROUP BY 
    date, area, ssvid
-- HAVING 
--     fishing_hours > 0
)



###########################################################################
# Join the information of time differences to the night loitering information
select
  a.*,
  seg_id,
  timestamp,
  viirs_timestamp,
  abs_time_diff_sec,
  mean_integer_cloud_mask as mean_integer_cloud_mask_closest,
  lat,
  lon,
  best_flag,
  best_vessel_class,
  best_length_m,
  speed_knots,
  heading,
  night,
from
  daily_squid_fishing a
  left JOIN
  closest_ais_message b
  USING(date, area, ssvid)



