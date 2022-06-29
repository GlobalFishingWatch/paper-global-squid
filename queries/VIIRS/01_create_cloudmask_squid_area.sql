# extract cloud mask data only within squid fishing areas



CREATE OR REPLACE TABLE scratch_masaki.cloud_mask_10th_degree_grid_squid_area as 

with 
-- cloud mask
cloud_mask as (
SELECT
    date(StartDateTime) as date,
    *,
    
FROM
    `scratch_masaki.cloud_mask_10th_degree_grid`
),


-- Squid Fishing area
aoi_vertical as (

SELECT
    *
FROM 
    `world-fishing-827.fra_collaboration.squid_region_aois_v20211016`
)


select
    b.area,
    a.*
    
from
    cloud_mask a
    inner join
    aoi_vertical b
    ON ST_CONTAINS(b.geometry , ST_GEOGPOINT(a.lon, a.lat))