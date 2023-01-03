CREATE TEMP FUNCTION today() AS (DATE('{YYYY_MM_DD}'));
CREATE TEMP FUNCTION yesterday() AS (DATE_SUB(today(), INTERVAL 1 DAY));
CREATE TEMP FUNCTION tomorrow() AS (DATE_ADD(today(), INTERVAL 1 DAY));

with 







-- Table we are drawing AIS messages from
ais_position_table as (

select
    date(_partitiontime) as date,
    *
from 
  `pipe_production_v20201001.research_messages`
where
    date(_partitiontime) between yesterDay() and tomorrow()
    and timestamp between '{start_datetime}' and '{end_datetime}'
    and lat between {lat_min} and {lat_max}
    and lon between {lon_min} and {lon_max}

),


--
-- What is a good AIS segment? Find it here, in good_segs
good_segs as ( 
  SELECT
    seg_id
  FROM
    `pipe_production_v20201001.research_segs`
  WHERE
    good_seg
    AND positions > 10
    AND NOT overlapping_and_short
),

--
-- vessel info table, used for vessel class.
vessel_info_table as (
  select 
    ssvid,
    best.best_flag,
    best.best_vessel_class,
    best.best_length_m,
    best.best_tonnage_gt,
    case
      -- These vessel class should be compatible with `map_label()` function and `probability_table` subquery.
      when best.best_vessel_class in
            ("drifting_longlines", "purse_seines", "other_purse_seines", "tuna_purse_seines",
            "cargo_or_tanker", "cargo", "tanker", "tug", "trawlers", "fishing") 
            then best.best_vessel_class
      when on_fishing_list_best then "fishing" 
      else "other"
    end label, 
  from `world-fishing-827.gfw_research.vi_ssvid_v20211201` 
  where
    -- This condition retains records having `best.best_vessel_class is null`.
    -- These should go to label `other`
    not (best.best_vessel_class is not null and best.best_vessel_class = "gear")
        
),




ais_messages as (  
select
  date,
  -- (ssvid, seg_id, timestamp) should be unique key of this subquery.
  ssvid,
  seg_id,
  timestamp,
  lat,
  lon,
  course,
  speed_knots as speed,
  type,
  source,
  receiver_type,
  receiver,
  b.*
from 
  ais_position_table a
  LEFT JOIN
  vessel_info_table b
  using (ssvid)
where
  abs(lat) <= 90 and abs(lon) <= 180                               
  and seg_id in (select seg_id from good_segs)
  and speed_knots < 50
  -- ignore the really fast vessels... most are noise
  -- this could just ignore speeds of 102.3 
                                                  
)


select * from ais_messages


