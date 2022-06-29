# Get daily count of 
#   VIIRS detection
#   VIIRS detection matched to ANY AIS vessels
#   VIIRS detection matched to night loitering squid AIS vessels
#   VIIRS detection unmatched 

-- CREATE TEMP FUNCTION RAD_THRESHOLD() AS (10);
-- CREATE TEMP FUNCTION SCORE_THRESHOLD() AS (0.01);
-- CREATE TEMP FUNCTION NL_HOUR_THRESHOLD() AS (1);


CREATE TEMP FUNCTION RAD_THRESHOLD() AS ({RAD_THRESHOLD});
CREATE TEMP FUNCTION SCORE_THRESHOLD() AS ({SCORE_THRESHOLD});
CREATE TEMP FUNCTION NL_HOUR_THRESHOLD() AS ({NL_HOUR_THRESHOLD});





# count VIIRS and VIIRS detection that matched/unmatched to AIS 
# for each region and date

WITH 

#################################
# source tables 


# This table contains all the dates (2017-01-01 ~ 2020-12-31) and areas of interest.
# This table is used to make sure the final result contains all the dates and areas.
all_dates_area as (
select
    * except(year_month6, year_month4, year_month3, year_month2, year_month1),
    CAST(year_month1 AS STRING) as year_month1, # 
    CAST(year_month2 AS STRING) as year_month2, # 2 month period
    CAST(year_month3 AS STRING) as year_month3, # 3 month period == quater
    CAST(year_month4 AS STRING) as year_month4, # 4 month period (1-2-3-4), (5-6-7-8), (9-10-11-12) 
    CAST(year_month6 AS STRING) as year_month6  # 6 month period (1-2-3-4-5-6), (7-8-9-1-0-11-12)
from
    `scratch_masaki.all_dates_area_2017_2020`
),



# viirs matching
viirs_matching as (
select
    EXTRACT(YEAR from date) as year,
    LPAD(CAST(EXTRACT(MONTH from date) AS STRING), 2, '0') as month,
    IF(EXTRACT(DAY from date) <= 15, 'F', 'S' ) as half_month,
    # year_half_month coulmn contains values like below 
    # 202012F (First half of December 2020),
    # 202012S (Second half of December 2020)
    CONCAT(EXTRACT(YEAR from date), LPAD(CAST(EXTRACT(MONTH from date) AS STRING), 2, '0'), IF(EXTRACT(DAY from date) <= 15, 'F', 'S' )) as year_half_month,
    *
from
    `scratch_masaki.viirs_matching_squid_area_no_overlap_local_night_2017_2021_v20220512`
where
    # Extracting VBDs with radiance greater than the RAD_THRESHOLD
    Rad_DNB > RAD_THRESHOLD()
),


######################################################
# daily night loitering hours for each squid vessels in the squid fishing areas
ais_squid_vessels_night_loitering as (
SELECT
    *
FROM
    `scratch_masaki.nightloitering_vessel_points_only_squid_local_night_v20220512`
WHERE
    fishing_hours >= NL_HOUR_THRESHOLD()
),



################################
# Extracting only VIIRS detections that matched to night loitering AIS squid vessels
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



# daily count of total VIIRS detections
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

# daily count of VIIRS detections that matched with "ANY" AIS vessels
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


# daily count of VIIRS detections that matched with "Night loitering Squid" AIS vessels
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


# daily count of "Night loitering Squid" AIS vessels
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





###################################################
# JOIN all the daily counts
daily_counts as (

select
    a.*,
    * except(area, date, year_half_month, year_month1, year_month2, year_month3, year_month4, year_month6)
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
    year_month1,
    year_month2,
    year_month3,
    year_month4,
    year_month6,
    count_viirs_total,
    # We need to distinguish NULL and Zero for the count of VIIRS detection
    # If we observe at least 1 VIIRS deteciton in the area then we can regard NULL value of the number of matched VIIRS as 0, 
    # otherwize NULL value of the number of matched VIIRS should be treated as NULL.
    IF(count_viirs_total is not null and count_viirs_matched is null, 0, count_viirs_matched) as count_viirs_matched,
    IF(count_viirs_total is not null and count_nl_matched is null,        0, count_nl_matched) as count_nl_matched,
    # NULL value of the number of night loitering AIS squid vessels can always regareded as 0.
    IF(count_nl is null, 0, count_nl) as count_nl,

from 
    daily_counts
order by 
    area, date
