
WITH 


viirs_match as (
select 
    # define half month colum
    CONCAT(EXTRACT(YEAR from date), LPAD(CAST(EXTRACT(MONTH from date) AS STRING), 2, '0'), IF(EXTRACT(DAY from date) <= 15, 'F', 'S' )) as year_half_month,
    date,
    area,
    detect_timestamp,
    detect_lat,
    detect_lon,
    QF_Detect,
    Rad_DNB,
    ssvid,
    seg_id,
    score,
    best_flag,
    best_vessel_class,
    best_length_m,
    detect_id,
    OrbitNumber,
    GranuleID,
    source,
    CAST(round(detect_lat*10) AS INT64) as lat_bin,
    CAST(round(detect_lon*10) AS INT64) as lon_bin,
from
    `scratch_masaki.viirs_matching_squid_area_no_overlap_local_night_2017_2021_v20220512`
),





# good_seg AIS pipe data within squid area
ais_squid_area as (
  SELECT
    *
  FROM
    `scratch_masaki.pipe_in_the_squid_area_local_night_v20220512`
),



##################################################################################################
# count of VIIRS total greater than radiance threshold
count_viirs_total as (
SELECT 
    max(year_half_month ) as year_half_month,
    date,
    area,
    count(*) as count_viirs_total
FROM 
    viirs_match
GROUP BY 
    date,
    area
    
),

# get the dates that maximum number of VIIRS detections are observed for each area and half month
count_viirs_half_month_max_date as (
select
    *,
    ROW_NUMBER() OVER (PARTITION BY area, year_half_month ORDER BY a.count_viirs_total desc, date) as row_num
from
    count_viirs_total a
QUALIFY
    row_num=1
),
########################################################################




########################################################################
# only extract night loitering messages by squid vessels
squid_nl_hours AS (
SELECT
    *,
    hours AS nl_hours,
FROM 
    ais_squid_area 
WHERE
    night_loitering = 1
    # extract only Nate's squid vessels
    and ssvid IN (select distinct ssvid from fra_collaboration.squid_night_loitering_temp)  
),


daily_squid_nl_hours as (
SELECT
    date,
    area,
    ssvid,
    max(seg_id) seg_id,
    sum(nl_hours) as nl_hours,
    max(best_flag) best_flag,
    max(best_vessel_class) best_vessel_class,
    AVG(lat) as avg_lat,
    AVG(lon) as avg_lon,
    TIMESTAMP_ADD(MIN(timestamp), INTERVAL CAST(0.5*TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), SECOND) AS INT64) SECOND) as avg_timestamp,
FROM
    squid_nl_hours 
GROUP BY 
    date, area, ssvid
),
#########################################################################




viirs_match_cloud_nightloitering as (
select
    a.*,
    c.nl_hours,
    case 
        # the VBD matched to squid ssvid that did night loitering on that day
        when score > 0.01 and c.ssvid is not null then 'VIIRS_MATCHED_WITH_SQUID'
        # the VBD matched to any ssvid other than (squid ssvid that did night loitering on that day)
        when score > 0.01 then 'VIIRS_MATCHED_WITH_NONSQUID'
        # the VBD unmatched
        ELSE 'VIIRS_UNMATCHED'
    end as match
from
    viirs_match a
    left join
    daily_squid_nl_hours c
    using(date, area, ssvid)
)


# extract only dates when the maximum number of VBDs are observed for each half-month
select
    *
from
    viirs_match_cloud_nightloitering
    inner join
    count_viirs_half_month_max_date
    using(date, area)








