---------------------------
-- Manually curated and reviewed
-- list of squid vessel MMSI
--
-- Note that this removes vessels
-- that spoof positions from
-- Peru to NZ
-- saved as paper_global_squid.final_squid_vessel_list_v20220525
---------------------------
WITH squid_vessels AS(
SELECT
ssvid,
geartype,
flag,
 CASE
 WHEN ssvid = '412331123' THEN TIMESTAMP('2017-09-01')
    WHEN ssvid = '412000286' THEN TIMESTAMP('2019-05-15')
    WHEN ssvid = '412328735' THEN TIMESTAMP('2018-05-11')
 ELSE first_timestamp
 END AS first_timestamp,
 CASE
    WHEN ssvid = '412000286' THEN TIMESTAMP('2020-11-25')
    WHEN ssvid = '412328735' THEN TIMESTAMP('2020-12-31')
 ELSE last_timestamp
 END AS last_timestamp
FROM
`world-fishing-827.paper_global_squid.npfc_sprfmo_nwpac_nwind_nn_tmt_arg_eastpac_vessels_v20220525`
WHERE ssvid NOT IN (
'412440615','412440613','412549237','412440616',
'412440617','412440614','412549045','412549232',
'412440612','412331057','650217001','412421111',
'412331136','412463481','412463478','412549043',
'417000748','412549238','416888888','412331136',
'412549234','412463479','338183544','412549233',
'412549043','412549229','417000786','412549238',
'412549231','412549212','412329516','412440382',
'412328795', '431100770','440046000','645379000',
-- spoofed from Peru to NZL
'412660090','412336492','412696260','412323990',
'412424456','412328789','412333656','5167106',
'441352000','412322110','440179000','412421087',
'412333657','412322090','412333752','412331076',
'412333654','200024440','412336489','412660070',
'412333751','412322120','412333655','432909000',
'412421151','412421153','412331501','412421155',
'412421157','416000000','412337347','412421158'
)
)

SELECT * FROM squid_vessels
