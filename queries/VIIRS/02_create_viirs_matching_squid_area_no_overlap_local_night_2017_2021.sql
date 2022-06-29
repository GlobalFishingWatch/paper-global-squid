# Create VIIRS-AIS matching table only within squid fishing areas
# and eliminate overlapping regions between successive orbits


-- CREATE TEMP FUNCTION START_DATE() AS (DATE('{date_start}'));
-- CREATE TEMP FUNCTION END_DATE() AS (DATE('{date_end}'));
CREATE TEMP FUNCTION START_DATE() AS (DATE('2017-01-01'));
CREATE TEMP FUNCTION END_DATE() AS (DATE('2022-01-15'));



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



# Create VIIRS-AIS matching table only within squid fishing areas
# and eliminate overlapping regions between successive orbits


CREATE OR REPLACE TABLE scratch_masaki.viirs_matching_squid_area_no_overlap_local_night_2017_2021_v20220512 AS

WITH 


##########################
# source tables


# VIIRS-AIS matching table
viirs_matching as (

select
    *,
from
    # only AIS match table
    `world-fishing-827.pipe_production_v20201001.proto_viirs_match_ais_matches`

    # AIS and VMS match table
    #`world-fishing-827.pipe_production_v20201001.proto_matches_raw_vbd_global_3top_v20210514`
where
    Date(detect_timestamp) >= START_DATE()
    AND Date(detect_timestamp) <= END_DATE()
    # for squid analyis we are focusing on only relatively bright vessels. 
    # and RAD_DNB > 30
),


# vessel info table
vessel_info_table as (
    select 
        ssvid,
        year,
        best.best_flag,
        best.best_vessel_class,
        best.best_length_m,
        best.best_tonnage_gt,
        case
          -- These vessel class should be compatible with `map_label()` function and `probability_table` subquery.
          when best.best_vessel_class in
                  ('drifting_longlines', 'purse_seines', 'other_purse_seines', 'tuna_purse_seines',
                  'cargo_or_tanker', 'cargo', 'tanker', 'tug', 'trawlers', 'fishing') 
                  then best.best_vessel_class
          when on_fishing_list_best then 'fishing' 
          else 'other'
        end label
    from
        `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220101` 
    where
        -- This condition retains records having `best.best_vessel_class is null`.
        -- These should go to label `other`
        not (best.best_vessel_class is not null and best.best_vessel_class = 'gear')
        
),


-- Squid Fishing area
aoi_vertical as (

SELECT
    *
FROM 
    `world-fishing-827.fra_collaboration.squid_region_aois_v20211016`
),


-- cloud mask in the squd area
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




###################################################
# Extract VIIRS-AIS matching only in the squid area
viirs_matching_squid_area as (
select
    extract(year from detect_timestamp) as year,

    # local night date
    TREAT_DATELINE_FOR_NW_PACIFIC(LOCAL_NIGHT(detect_timestamp, detect_lon), detect_lon, area) as date,

    # 0.1 degree grid for excluding overlapping area
    cast(round(detect_lat*10) as int64) as lat_bin,
    cast(round(detect_lon*10) as int64) as lon_bin,

    * except (geometry)
from
  viirs_matching a
  inner join
  aoi_vertical c
  ON ST_CONTAINS(c.geometry , ST_GEOGPOINT(a.detect_lon, a.detect_lat))
),


###########################################################################
# Select orbit having smallest zenith angle for each grid and local night
smallest_zenith_orbit as (
SELECT
    * except(area, GranuleID),
    ROW_NUMBER() OVER (PARTITION BY date, lat_bin, lon_bin ORDER BY mean_sensor_zenith) as rownum
FROM
    cloud_mask
QUALIFY
    rownum = 1
)



########################################################################
# Extract VIIRS-AIS matching only from smallest zenith orbit for each grid and day
select
  *
from
  viirs_matching_squid_area a

  inner join
  smallest_zenith_orbit
  using(date, OrbitNumber, lat_bin, lon_bin)

  left join
  vessel_info_table b
  using(ssvid, year)



