# Variables to define ellipse for south amerincan filter 
CREATE TEMP FUNCTION X() AS (-50); # center lon
CREATE TEMP FUNCTION Y() AS (-22); # center lat
CREATE TEMP FUNCTION RX() AS (50); # radius lon
CREATE TEMP FUNCTION RY() AS (20); # radius lat 
 
 
 
 WITH 

  # VIIRS detection
  viirs AS (
  SELECT
        -- concat(cast(Date_Mscan as string),concat(cast(Lat_DNB as string),cast(Lon_DNB as string))) as detect_id,
        -- CAST(SUBSTR(File_DNB, 40,5) AS INT64) AS OrbitNumber,
        Date(Date_Mscan) as date,
        Lat_DNB,
        Lon_DNB,
        Date_Mscan,
        Rad_DNB,
        -- Rad_I04,
        QF_Detect,
        SMI,
        -- SI,
        SHI,
        -- LI,
        -- SATZ_GDNBO,
        # Define allipse, TRUE is inside
        IF(POW(((Lon_DNB - X()) / RX()),2) + POW(((Lat_DNB - Y()) / RY()) , 2) < 1, TRUE, FALSE) as is_south_america
  FROM
      `world-fishing-827.pipe_viirs_production_v20220112.raw_vbd_global`
  where
        QF_Detect IN (1,2,3,5,7,10)
        AND DATE(Date_Mscan) BETWEEN "2020-01-01" AND "2020-12-31"
        -- AND Lon_DNB between -180 and 20
        -- AND Lat_DNB between -90 and 30
  )

SELECT
  *
FROM
  viirs
