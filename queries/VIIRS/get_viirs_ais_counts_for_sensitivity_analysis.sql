-- CREATE TEMP FUNCTION RAD_THRESHOLD() AS (10);
-- CREATE TEMP FUNCTION SCORE_THRESHOLD() AS (0.01);
-- CREATE TEMP FUNCTION NL_HOUR_THRESHOLD() AS (1);


CREATE TEMP FUNCTION RAD_THRESHOLD() AS ({RAD_THRESHOLD});
CREATE TEMP FUNCTION SCORE_THRESHOLD() AS ({SCORE_THRESHOLD});
CREATE TEMP FUNCTION NL_HOUR_THRESHOLD() AS ({NL_HOUR_THRESHOLD});



# count VIIRS and VIIRS-AIS matched 
# for each region and date


WITH 

#################################
# source tables 

# table that contains all dates and areas of interest
all_dates_area as (
select
    * except(year_month6, year_month4, year_month3, year_month2, year_month1)
    -- CAST(year_month1 AS STRING) as year_month1,
    -- CAST(year_month2 AS STRING) as year_month2,
    -- CAST(year_month3 AS STRING) as year_month3,
    -- CAST(year_month4 AS STRING) as year_month4,
    -- CAST(year_month6 AS STRING) as year_month6
from
`scratch_masaki.all_dates_area_2017_2020`
),



# viirs matching
viirs_matching as (
select
    EXTRACT(YEAR from date) as year,
    LPAD(CAST(EXTRACT(MONTH from date) AS STRING), 2, '0') as month,
    IF(EXTRACT(DAY from date) <= 15, 'F', 'S' ) as half_month,

    CONCAT(EXTRACT(YEAR from date), LPAD(CAST(EXTRACT(MONTH from date) AS STRING), 2, '0'), IF(EXTRACT(DAY from date) <= 15, 'F', 'S' )) as year_half_month,
    *
from
  `scratch_masaki.viirs_matching_squid_area_no_overlap_local_night_2017_2021_v20220512`
where
    Rad_DNB > RAD_THRESHOLD()
    # these two area/date have a lot of false detection
    # and not ((area='nw_indian' and date='2018-07-05') or (area='se_pacific' and date='2017-04-07'))
),





######################################################
# AIS squid vessels that did night loitering in the squid area
ais_squid_vessels_night_loitering as (
SELECT
    *
FROM
    `scratch_masaki.nightloitering_vessel_points_only_squid_local_night_v20220512`
WHERE
    fishing_hours >= NL_HOUR_THRESHOLD()
),




################################
# Extracting viirs that matched with nightloitering AIS squid vessels


# VIIRS detections that matched with night loitering AIS squid vessels
viirs_matched_with_night_loitering as (
select
  a.*
from
  viirs_matching a
  inner join 
  ais_squid_vessels_night_loitering
  using (date, area, ssvid)
where
    score >= SCORE_THRESHOLD()

),



#################################
# Counting vessels

# count of VIIRS total greater than radiance threshold
count_viirs_total as (
SELECT 
    max(year_half_month ) as year_half_month,
    date,
    area,
    count(*) as count_viirs_total
FROM 
    viirs_matching
GROUP BY 
    date,
    area
),

# count of VIIRS that matched with "ANY" AIS vessels
count_viirs_matched as (
SELECT 
    date,
    area,
    count(*) as count_viirs_matched
FROM 
    viirs_matching
WHERE
    score >= SCORE_THRESHOLD()
GROUP BY 
    date,
    area
),




###############################################################
# Count the night loitering AIS squid vessels
count_nightloitering as (
SELECT 
    date,
    area,
    count(*) as count_nl
FROM 
    ais_squid_vessels_night_loitering
GROUP BY 
    date,
    area
),
###############################################################





###############################################################
# count of NIGHT LOITERING squid vessels that matched with VIIRS
count_matched_nightloitering as (
SELECT 
    date,
    area,
    count(*) as count_nl_matched
FROM 
    viirs_matched_with_night_loitering
GROUP BY 
    date,
    area
),

###############################################################



###################################################
# JOIN AIS count and VIIRS count
daily_counts as (

select
    a.*,
    #* except(area, date, year_half_month, year_month1, year_month2, year_month3, year_month4, year_month6)
    * except(area, date, year_half_month)
from

    all_dates_area a

    left join
    count_viirs_total 
    using (date, area)

    left join
    count_viirs_matched
    using (date, area)

    left join
    count_nightloitering 
    using (date, area)

    left join
    count_matched_nightloitering
    using (date, area)
)


select
    area,
    date,
    year_half_month,
    -- year_month1,
    -- year_month2,
    -- year_month3,
    -- year_month4,
    -- year_month6,
    count_viirs_total,
    IF(count_viirs_total is not null and count_viirs_matched is null, 0, count_viirs_matched) as count_viirs_matched,
    IF(count_nl is null, 0, count_nl) as count_nl,
    IF(count_viirs_total is not null and count_nl_matched is null, 0, count_nl_matched) as count_nl_matched,
from 
    daily_counts
order by 
    area, date
