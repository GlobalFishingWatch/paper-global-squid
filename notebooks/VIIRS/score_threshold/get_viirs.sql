CREATE TEMP FUNCTION today() AS (DATE('{YYYY_MM_DD}'));
CREATE TEMP FUNCTION yesterday() AS (DATE_SUB(today(), INTERVAL 1 DAY));
CREATE TEMP FUNCTION tomorrow() AS (DATE_ADD(today(), INTERVAL 1 DAY));




with 

viirs_ais as (

  select
    _partitiontime as date,
    *
  from 
    `world-fishing-827.pipe_production_v20201001.proto_matches_raw_vbd_global_3top_v20210514` 
  where
    date(_partitiontime) between today() and today()
    and detect_lat between {lat_min} and {lat_max}
    and detect_lon between {lon_min} and {lon_max}
)


select * from viirs_ais