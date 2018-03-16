CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_Wind
AS
SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    status,
    technology,
    primemover,
    sectorname,
    sector,
    nameplatecapacity_mw,
    summercapacity_mw,
    wintercapacity_mw,
    operatingmonth,
    operatingyear,
    NULL AS retirementmonth,
    NULL AS retirementyear,
    numberofturbines,
    predominantturbinemanufacturer,
    predominantturbinemodelnumber,
    designwindspeed_mph,
    windqualityclass,
    turbinehubheight_feet,
    faaobstaclenumber,
    'operable' AS ops_status
FROM {schemaName}.{tableName}l2_3_2_windoperable

UNION ALL

SELECT
    utilityid,
	utilityname,
	plantcode,
	plantname,
	state,
	county,
	generatorid,
	status,
	technology,
	primemover,
	sectorname,
	sector,
	nameplatecapacity_mw,
	summercapacity_mw,
	wintercapacity_mw,
	NULL AS operatingmonth,
    NULL AS operatingyear,
    operatingmonth AS retirementmonth, --this column should have been retirementmonth
    operatingyear AS retirementyear,  --this column should have been retirementyear
	numberofturbines,
	predominantturbinemanufacturer,
	predominantturbinemodelnumber,
	designwindspeed_mph,
	windqualityclass,
	turbinehubheight_feet,
	faaobstaclenumber,
	'retired_cancelled' AS ops_status
FROM {schemaName}.{tableName}l2_3_2_windretcancelled;



CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_Solar
AS
SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    status,
    technology,
    primemover,
    sectorname,
    sector,
    nameplatecapacity_mw,
    summercapacity_mw,
    wintercapacity_mw,
    operatingmonth,
    operatingyear,
    NULL AS retirementmonth,
    NULL AS retirementyear,
    lensesmirrors,
    singleaxistracking,
    dualaxistracking,
    fixedtilt,
    parabolictrough,
    linearfresnel,
    powertower,
    dishengine,
    othersolartechnology,
    dcnetcapacity_mw,
    crystallinesilicon,
    thinfilm_cdte,
    thinfilm_asi,
    thinfilm_cigs,
    thinfilm_other,
    othermaterials,
    'operable' AS ops_status
FROM {schemaName}.{tableName}l2_3_3_solaroperable

UNION ALL

SELECT
    utilityid,
	utilityname,
	plantcode,
	plantname,
	state,
	county,
	generatorid,
	status,
	technology,
	primemover,
	sectorname,
	sector,
	nameplatecapacity_mw,
	summercapacity_mw,
	wintercapacity_mw,
	NULL AS operatingmonth,
    NULL AS operatingyear,
    operatingmonth AS retirementmonth, --this column should have been retirementmonth
    operatingyear AS retirementyear,  --this column should have been retirementyear
	lensesmirrors,
	singleaxistracking,
	dualaxistracking,
	fixedtilt,
	parabolictrough,
	linearfresnel,
	powertower,
	dishengine,
	othersolartechnology,
	dcnetcapacity_mw,
	crystallinesilicon,
	thinfilm_cdte,
	thinfilm_asi,
	thinfilm_cigs,
	thinfilm_other,
	othermaterials,
	'retired_cancelled' AS ops_status
FROM {schemaName}.{tableName}l2_3_3_solarretcancelled;


CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_multifuel
AS
SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    status,
    technology,
    primemover,
    sectorname,
    sector,
    nameplatecapacity_mw,
    summercapacity_mw,
    wintercapacity_mw,
    energysource1,
    energysource2,
    cofirefuels,
    cofireenergysource1,
    cofireenergysource2,
    cofireenergysource3,
    cofireenergysource4,
    cofireenergysource5,
    cofireenergysource6,
    switchbetweenoilandnaturalgas,
    switchwhenoperating,
    netsummercapacitywithnaturalgas_mw,
    netwintercapacitywithnaturalgas_mw,
    netsummercapacitywithoil_mw,
    netwintercapacitywithoil_mw,
    timetoswitchfromgastooil,
    timetoswitchfromoiltogas,
    factorsthatlimitswitching,
    storagelimits,
    airpermitlimits,
    otherlimits,
    'operable' AS ops_status
FROM {schemaName}.{tableName}l2_3_4_multifueloperable

UNION ALL

SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    status,
    technology,
    primemover,
    sectorname,
    sector,
    nameplatecapacity_mw,
    summercapacity_mw,
    wintercapacity_mw,
    energysource1,
    energysource2,
    cofirefuels,
    cofireenergysource1,
    cofireenergysource2,
    cofireenergysource3,
    cofireenergysource4,
    cofireenergysource5,
    cofireenergysource6,
    switchbetweenoilandnaturalgas,
    NULL AS switchwhenoperating,
    NULL AS netsummercapacitywithnaturalgas_mw,
    NULL AS netwintercapacitywithnaturalgas_mw,
    NULL AS netsummercapacitywithoil_mw,
    NULL AS netwintercapacitywithoil_mw,
    NULL AS timetoswitchfromgastooil,
    NULL AS timetoswitchfromoiltogas,
    NULL AS factorsthatlimitswitching,
    NULL AS storagelimits,
    NULL AS airpermitlimits,
    NULL AS otherlimits,
    'proposed' AS ops_status
FROM {schemaName}.{tableName}l2_3_4_multifuelproposed    

UNION ALL

SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    status,
    technology,
    primemover,
    sectorname,
    sector,
    nameplatecapacity_mw,
    summercapacity_mw,
    wintercapacity_mw,
    energysource1,
    energysource2,
    cofirefuels,
    cofireenergysource1,
    cofireenergysource2,
    cofireenergysource3,
    cofireenergysource4,
    cofireenergysource5,
    cofireenergysource6,
    switchbetweenoilandnaturalgas,
    switchwhenoperating,
    netsummercapacitywithnaturalgas_mw,
    netwintercapacitywithnaturalgas_mw,
    netsummercapacitywithoil_mw,
    netwintercapacitywithoil_mw,
    timetoswitchfromgastooil,
    timetoswitchfromoiltogas,
    factorsthatlimitswitching,
    storagelimits,
    airpermitlimits,
    otherlimits,
    'retired_cancelled' AS ops_status
FROM {schemaName}.{tableName}l2_3_4_multifuelretcancelled;
        
        
CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_generator
AS
SELECT
    utilityid,
    utilityname,
    CAST(plantcode AS VARCHAR(100)), --fix this in the source table
    plantname,
    state,
    county,
    generatorid,
    technology,
    primemover,
    unitcode,
    ownership,
    ductburners,
    canbypassheatrecoverysteamgenerator,
    rto_isolmpnodedesignation,
    rto_isolocationdesignationforreportingwholesalesalesdatatoferc AS rto_isolocationdesignation,
    nameplatecapacity_mw,
    nameplatepowerfactor,
    summercapacity_mw,
    wintercapacity_mw,
    minimumload_mw,
    uprateorderatecompletedduringyear,
    monthuprateorderatecompleted,
    yearuprateorderatecompleted,
    status,
    synchronizedtotransmissiongrid,
    NULL AS effectivemonth,
    NULL AS effectiveyear,
    NULL AS currentmonth,
    NULL AS currentyear,
    operatingmonth,
    operatingyear,
    plannedretirementmonth AS retirementmonth,
    plannedretirementyear AS retirementyear,
    associatedwithcombinedheatandpowersystem,
    sectorname,
    sector,
    toppingorbottoming,
    energysource1,
    energysource2,
    energysource3,
    energysource4,
    energysource5,
    energysource6,
    startupsource1,
    startupsource2,
    startupsource3,
    startupsource4,
    solidfuelgasificationsystem,
    carboncapturetechnology,
    turbines_inverters_orhydrokineticbuoys,
    timefromcoldshutdowntofullload,
    fluidizedbedtechnology,
    pulverizedcoaltechnology,
    stokertechnology,
    othercombustiontechnology,
    subcriticaltechnology,
    supercriticaltechnology,
    ultrasupercriticaltechnology,
    plannednetsummercapacityuprate_mw,
    plannednetwintercapacityuprate_mw,
    plannedupratemonth,
    planneduprateyear,
    plannednetsummercapacityderate_mw,
    plannednetwintercapacityderate_mw,
    plannedderatemonth,
    plannedderateyear,
    plannednewprimemover,
    plannedenergysource1,
    plannednewnameplatecapacity_mw,
    plannedrepowermonth,
    plannedrepoweryear,
    otherplannedmodifications,
    othermodificationsmonth,
    othermodificationsyear,
    cofirefuels,
    switchbetweenoilandnaturalgas,
    NULL AS previouslycanceled,
    'operable' AS ops_status
FROM {schemaName}.{tableName}l2_3_1_generatoroperable

UNION ALL

SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    technology,
    primemover,
    unitcode,
    ownership,
    ductburners,
    canbypassheatrecoverysteamgenerator,
    rto_isolmpnodedesignation,
    rto_isolocationdesignation,
    nameplatecapacity_mw,
    nameplatepowerfactor,
    summercapacity_mw,
    wintercapacity_mw,
    minimumload_mw,
    uprateorderatecompletedduringyear,
    monthuprateorderatecompleted,
    yearuprateorderatecompleted,
    status,
    synchronizedtotransmissiongrid,
    NULL AS effectivemonth,
    NULL AS effectiveyear,
    NULL AS currentmonth,
    NULL AS currentyear,
    operatingmonth,
    operatingyear,
    retirementmonth,
    retirementyear,
    associatedwithcombinedheatandpowersystem,
    sectorname,
    sector,
    toppingorbottoming,
    energysource1,
    energysource2,
    energysource3,
    energysource4,
    energysource5,
    energysource6,
    startupsource1,
    startupsource2,
    startupsource3,
    startupsource4,
    solidfuelgasificationsystem,
    carboncapturetechnology,
    turbines_inverters_orhydrokineticbuoys,
    timefromcoldshutdowntofullload,
    fluidizedbedtechnology,
    pulverizedcoaltechnology,
    stokertechnology,
    othercombustiontechnology,
    subcriticaltechnology,
    supercriticaltechnology,
    ultrasupercriticaltechnology,
    NULL AS plannednetsummercapacityuprate_mw,
    NULL AS plannednetwintercapacityuprate_mw,
    NULL AS plannedupratemonth,
    NULL AS planneduprateyear,
    NULL AS plannednetsummercapacityderate_mw,
    NULL AS plannednetwintercapacityderate_mw,
    NULL AS plannedderatemonth,
    NULL AS plannedderateyear,
    NULL AS plannednewprimemover,
    NULL AS plannedenergysource1,
    NULL AS plannednewnameplatecapacity_mw,
    NULL AS plannedrepowermonth,
    NULL AS plannedrepoweryear,
    NULL AS otherplannedmodifications,
    NULL AS othermodificationsmonth,
    NULL AS othermodificationsyear,
    cofirefuels,
    switchbetweenoilandnaturalgas,
    NULL AS previouslycanceled,
    'retired_cancelled' AS ops_status
FROM {schemaName}.{tableName}l2_3_1_generatorretcancelled

UNION ALL

SELECT
    utilityid,
    utilityname,
    plantcode,
    plantname,
    state,
    county,
    generatorid,
    technology,
    primemover,
    unitcode,
    ownership,
    ductburners,
    canbypassheatrecoverysteamgenerator,
    rto_isolmpnodedesignation,
    rto_isolocationdesignation,
    nameplatecapacity_mw,
    nameplatepowerfactor,
    summercapacity_mw,
    wintercapacity_mw,
    NULL AS minimumload_mw,
    NULL AS uprateorderatecompletedduringyear,
    NULL AS monthuprateorderatecompleted,
    NULL AS yearuprateorderatecompleted,
    status,
    NULL AS synchronizedtotransmissiongrid,
    effectivemonth,
    effectiveyear,
    currentmonth,
    currentyear,
    NULL AS operatingmonth,  --fix this
    NULL AS operatingyear,      --fix this
    NULL AS retirementmonth, --fix this
    NULL AS retirementyear, --fix this
    associatedwithcombinedheatandpowersystem,
    sectorname,
    sector,
    NULL AS toppingorbottoming,
    energysource1,
    energysource2,
    energysource3,
    energysource4,
    energysource5,
    energysource6,
    NULL AS startupsource1,
    NULL AS startupsource2,
    NULL AS startupsource3,
    NULL AS startupsource4,
    solidfuelgasificationsystem,
    carboncapturetechnology,
    turbines_inverters_orhydrokineticbuoys,
    NULL AS timefromcoldshutdowntofullload,
    fluidizedbedtechnology,
    pulverizedcoaltechnology,
    stokertechnology,
    othercombustiontechnology,
    subcriticaltechnology,
    supercriticaltechnology,
    ultrasupercriticaltechnology,
    NULL AS plannednetsummercapacityuprate_mw,
    NULL AS plannednetwintercapacityuprate_mw,
    NULL AS plannedupratemonth,
    NULL AS planneduprateyear,
    NULL AS plannednetsummercapacityderate_mw,
    NULL AS plannednetwintercapacityderate_mw,
    NULL AS plannedderatemonth,
    NULL AS plannedderateyear,
    NULL AS plannednewprimemover,
    NULL AS plannedenergysource1,
    NULL AS plannednewnameplatecapacity_mw,
    NULL AS plannedrepowermonth,
    NULL AS plannedrepoweryear,
    NULL AS otherplannedmodifications,
    NULL AS othermodificationsmonth,
    NULL AS othermodificationsyear,
    cofirefuels,
    switchbetweenoilandnaturalgas,
    previouslycanceled,
    'proposed' AS ops_status
FROM {schemaName}.{tableName}l2_3_1_generatorproposed;



CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_generator_wind
AS
SELECT
    G.utilityid,
    COALESCE(W.utilityname, G.utilityname) AS utilityname,
    G.plantcode,
    COALESCE(W.plantname, G.plantname) AS plantname,
    COALESCE(W.state, G.state) AS state,
    COALESCE(W.county, G.county) AS county,
    G.generatorid,
    COALESCE(W.technology, G.technology) AS technology,
    COALESCE(W.primemover, G.primemover) AS primemover,
    G.unitcode,
    G.ownership,
    G.ductburners,
    G.canbypassheatrecoverysteamgenerator,
    G.rto_isolmpnodedesignation,
    G.rto_isolocationdesignation,
    COALESCE(W.nameplatecapacity_mw, G.nameplatecapacity_mw) AS nameplatecapacity_mw,
    G.nameplatepowerfactor,
    COALESCE(W.summercapacity_mw, G.summercapacity_mw) AS summercapacity_mw,
    COALESCE(W.wintercapacity_mw, G.wintercapacity_mw) AS wintercapacity_mw,
    G.minimumload_mw,
    G.uprateorderatecompletedduringyear,
    G.monthuprateorderatecompleted,
    G.yearuprateorderatecompleted,
    COALESCE(G.status, W.status) AS status,
    G.synchronizedtotransmissiongrid,
    G.effectivemonth,
    G.effectiveyear,
    G.currentmonth,
    G.currentyear,
    COALESCE(W.operatingmonth, G.operatingmonth) AS operatingmonth,
    COALESCE(W.operatingyear, G.operatingyear) AS operatingyear,
    COALESCE(W.retirementmonth, G.retirementmonth) AS retirementmonth,
    COALESCE(W.retirementyear, G.retirementyear) AS retirementyear,
    G.associatedwithcombinedheatandpowersystem,
    COALESCE(W.sectorname, G.sectorname) AS sectorname,
    COALESCE(W.sector, G.sector) AS sector,
    G.toppingorbottoming,
    G.energysource1,
    G.energysource2,
    G.energysource3,
    G.energysource4,
    G.energysource5,
    G.energysource6,
    G.startupsource1,
    G.startupsource2,
    G.startupsource3,
    G.startupsource4,
    G.solidfuelgasificationsystem,
    G.carboncapturetechnology,
    G.turbines_inverters_orhydrokineticbuoys,
    G.timefromcoldshutdowntofullload,
    G.fluidizedbedtechnology,
    G.pulverizedcoaltechnology,
    G.stokertechnology,
    G.othercombustiontechnology,
    G.subcriticaltechnology,
    G.supercriticaltechnology,
    G.ultrasupercriticaltechnology,
    G.plannednetsummercapacityuprate_mw,
    G.plannednetwintercapacityuprate_mw,
    G.plannedupratemonth,
    G.planneduprateyear,
    G.plannednetsummercapacityderate_mw,
    G.plannednetwintercapacityderate_mw,
    G.plannedderatemonth,
    G.plannedderateyear,
    G.plannednewprimemover,
    G.plannedenergysource1,
    G.plannednewnameplatecapacity_mw,
    G.plannedrepowermonth,
    G.plannedrepoweryear,
    G.otherplannedmodifications,
    G.othermodificationsmonth,
    G.othermodificationsyear,
    G.cofirefuels,
    G.switchbetweenoilandnaturalgas,
    G.previouslycanceled,
    COALESCE(W.ops_status, G.ops_status) AS ops_status,
    W.numberofturbines,
    W.predominantturbinemanufacturer,
    W.predominantturbinemodelnumber,
    W.designwindspeed_mph,
    W.windqualityclass,
    W.turbinehubheight_feet,
    W.faaobstaclenumber
FROM {schemaName}.{tableName}vw_generator G
LEFT JOIN {schemaName}.{tableName}vw_wind W
    ON G.utilityid = W.utilityid
    AND G.plantcode = W.plantcode
    AND G.generatorid = W.generatorid;
    

CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_generator_wind_solar
AS
SELECT
    GW.utilityid,
    COALESCE(S.utilityname, GW.utilityname) AS utilityname,
    GW.plantcode,
    COALESCE(S.plantname, GW.plantname) AS plantname,
    COALESCE(S.state, GW.state) AS state,
    COALESCE(S.county, GW.county) AS county,
    GW.generatorid,
    COALESCE(S.technology, GW.technology) AS technology,
    COALESCE(S.primemover, GW.primemover) AS primemover,
    GW.unitcode,
    GW.ownership,
    GW.ductburners,
    GW.canbypassheatrecoverysteamgenerator,
    GW.rto_isolmpnodedesignation,
    GW.rto_isolocationdesignation,
    COALESCE(S.nameplatecapacity_mw, GW.nameplatecapacity_mw) AS nameplatecapacity_mw,
    GW.nameplatepowerfactor,
    COALESCE(S.summercapacity_mw, GW.summercapacity_mw) AS summercapacity_mw,
    COALESCE(S.wintercapacity_mw, GW.wintercapacity_mw) AS wintercapacity_mw,
    GW.minimumload_mw,
    GW.uprateorderatecompletedduringyear,
    GW.monthuprateorderatecompleted,
    GW.yearuprateorderatecompleted,
    COALESCE(S.status, GW.status) AS status,
    GW.synchronizedtotransmissiongrid,
    GW.effectivemonth,
    GW.effectiveyear,
    GW.currentmonth,
    GW.currentyear,
    COALESCE(S.operatingmonth, GW.operatingmonth) AS operatingmonth,
    COALESCE(S.operatingyear, GW.operatingyear) AS operatingyear,
    COALESCE(S.retirementmonth, GW.retirementmonth) AS retirementmonth,
    COALESCE(S.retirementyear, GW.retirementyear) AS retirementyear,
    GW.associatedwithcombinedheatandpowersystem,
    COALESCE(S.sectorname, GW.sectorname) AS sectorname,
    COALESCE(S.sector, GW.sector) AS sector,
    GW.toppingorbottoming,
    GW.energysource1,
    GW.energysource2,
    GW.energysource3,
    GW.energysource4,
    GW.energysource5,
    GW.energysource6,
    GW.startupsource1,
    GW.startupsource2,
    GW.startupsource3,
    GW.startupsource4,
    GW.solidfuelgasificationsystem,
    GW.carboncapturetechnology,
    GW.turbines_inverters_orhydrokineticbuoys,
    GW.timefromcoldshutdowntofullload,
    GW.fluidizedbedtechnology,
    GW.pulverizedcoaltechnology,
    GW.stokertechnology,
    GW.othercombustiontechnology,
    GW.subcriticaltechnology,
    GW.supercriticaltechnology,
    GW.ultrasupercriticaltechnology,
    GW.plannednetsummercapacityuprate_mw,
    GW.plannednetwintercapacityuprate_mw,
    GW.plannedupratemonth,
    GW.planneduprateyear,
    GW.plannednetsummercapacityderate_mw,
    GW.plannednetwintercapacityderate_mw,
    GW.plannedderatemonth,
    GW.plannedderateyear,
    GW.plannednewprimemover,
    GW.plannedenergysource1,
    GW.plannednewnameplatecapacity_mw,
    GW.plannedrepowermonth,
    GW.plannedrepoweryear,
    GW.otherplannedmodifications,
    GW.othermodificationsmonth,
    GW.othermodificationsyear,
    GW.cofirefuels,
    GW.switchbetweenoilandnaturalgas,
    GW.previouslycanceled,
    GW.ops_status,
    GW.numberofturbines,
    GW.predominantturbinemanufacturer,
    GW.predominantturbinemodelnumber,
    GW.designwindspeed_mph,
    GW.windqualityclass,
    GW.turbinehubheight_feet,
    GW.faaobstaclenumber,    
    S.lensesmirrors,
    S.singleaxistracking,
    S.dualaxistracking,
    S.fixedtilt,
    S.parabolictrough,
    S.linearfresnel,
    S.powertower,
    S.dishengine,
    S.othersolartechnology,
    S.dcnetcapacity_mw,
    S.crystallinesilicon,
    S.thinfilm_cdte,
    S.thinfilm_asi,
    S.thinfilm_cigs,
    S.thinfilm_other,
    S.othermaterials
FROM {schemaName}.{tableName}VW_generator_wind GW
LEFT JOIN {schemaName}.{tableName}VW_solar S
    ON GW.utilityid = S.utilityid
    AND GW.plantcode = S.plantcode
    AND GW.generatorid = S.generatorid;
    
        
CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_generator_wind_solar_multifuel
AS
SELECT
    GWS.utilityid,
    COALESCE(MF.utilityname, GWS.utilityname) AS utilityname,
    COALESCE(MF.plantname, GWS.plantname) AS plantname,
    COALESCE(MF.state, GWS.state) AS state,
    COALESCE(MF.county, GWS.county) AS county,
    GWS.plantcode,
    GWS.generatorid,
    COALESCE(MF.technology, GWS.technology) AS technology,
    COALESCE(MF.primemover, GWS.primemover) AS primemover,
    GWS.unitcode,
    GWS.ownership,
    GWS.ductburners,
    GWS.canbypassheatrecoverysteamgenerator,
    GWS.rto_isolmpnodedesignation,
    GWS.rto_isolocationdesignation,
    COALESCE(MF.nameplatecapacity_mw, GWS.nameplatecapacity_mw) AS nameplatecapacity_mw,
    GWS.nameplatepowerfactor,
    COALESCE(MF.summercapacity_mw, GWS.summercapacity_mw) AS summercapacity_mw,
    COALESCE(MF.wintercapacity_mw, GWS.wintercapacity_mw) AS wintercapacity_mw,
    GWS.minimumload_mw,
    GWS.uprateorderatecompletedduringyear,
    GWS.monthuprateorderatecompleted,
    GWS.yearuprateorderatecompleted,
    COALESCE(MF.status, GWS.status) AS status,
    GWS.synchronizedtotransmissiongrid,
    GWS.effectivemonth,
    GWS.effectiveyear,
    GWS.currentmonth,
    GWS.currentyear,
    GWS.operatingmonth,
    GWS.operatingyear,
    GWS.retirementmonth,
    GWS.retirementyear,
    GWS.associatedwithcombinedheatandpowersystem,
    COALESCE(MF.sectorname, GWS.sectorname) AS sectorname,
    COALESCE(MF.sector, GWS.sector) AS sector,
    GWS.toppingorbottoming,
    COALESCE(MF.energysource1, GWS.energysource1) AS energysource1,
    COALESCE(MF.energysource2, GWS.energysource2) AS energysource2,
    GWS.energysource3,
    GWS.energysource4,
    GWS.energysource5,
    GWS.energysource6,
    GWS.startupsource1,
    GWS.startupsource2,
    GWS.startupsource3,
    GWS.startupsource4,
    GWS.solidfuelgasificationsystem,
    GWS.carboncapturetechnology,
    GWS.turbines_inverters_orhydrokineticbuoys,
    GWS.timefromcoldshutdowntofullload,
    GWS.fluidizedbedtechnology,
    GWS.pulverizedcoaltechnology,
    GWS.stokertechnology,
    GWS.othercombustiontechnology,
    GWS.subcriticaltechnology,
    GWS.supercriticaltechnology,
    GWS.ultrasupercriticaltechnology,
    GWS.plannednetsummercapacityuprate_mw,
    GWS.plannednetwintercapacityuprate_mw,
    GWS.plannedupratemonth,
    GWS.planneduprateyear,
    GWS.plannednetsummercapacityderate_mw,
    GWS.plannednetwintercapacityderate_mw,
    GWS.plannedderatemonth,
    GWS.plannedderateyear,
    GWS.plannednewprimemover,
    GWS.plannedenergysource1,
    GWS.plannednewnameplatecapacity_mw,
    GWS.plannedrepowermonth,
    GWS.plannedrepoweryear,
    GWS.otherplannedmodifications,
    GWS.othermodificationsmonth,
    GWS.othermodificationsyear,
    COALESCE(MF.cofirefuels, GWS.cofirefuels) AS cofirefuels,
    COALESCE(MF.switchbetweenoilandnaturalgas, GWS.switchbetweenoilandnaturalgas) AS switchbetweenoilandnaturalgas,
    GWS.previouslycanceled,
    GWS.ops_status,
    GWS.numberofturbines,
    GWS.predominantturbinemanufacturer,
    GWS.predominantturbinemodelnumber,
    GWS.designwindspeed_mph,
    GWS.windqualityclass,
    GWS.turbinehubheight_feet,
    GWS.faaobstaclenumber,
    GWS.lensesmirrors,
    GWS.singleaxistracking,
    GWS.dualaxistracking,
    GWS.fixedtilt,
    GWS.parabolictrough,
    GWS.linearfresnel,
    GWS.powertower,
    GWS.dishengine,
    GWS.othersolartechnology,
    GWS.dcnetcapacity_mw,
    GWS.crystallinesilicon,
    GWS.thinfilm_cdte,
    GWS.thinfilm_asi,
    GWS.thinfilm_cigs,
    GWS.thinfilm_other,
    GWS.othermaterials,
    MF.cofireenergysource1,
    MF.cofireenergysource2,
    MF.cofireenergysource3,
    MF.cofireenergysource4,
    MF.cofireenergysource5,
    MF.cofireenergysource6,
    MF.switchwhenoperating,
    MF.netsummercapacitywithnaturalgas_mw,
    MF.netwintercapacitywithnaturalgas_mw,
    MF.netsummercapacitywithoil_mw,
    MF.netwintercapacitywithoil_mw,
    MF.timetoswitchfromgastooil,
    MF.timetoswitchfromoiltogas,
    MF.factorsthatlimitswitching,
    MF.storagelimits,
    MF.airpermitlimits,
    MF.otherlimits
FROM {schemaName}.{tableName}VW_generator_wind_solar GWS
LEFT JOIN {schemaName}.{tableName}VW_multifuel MF
    ON  GWS.utilityid = MF.utilityid
    AND GWS.plantcode = MF.plantcode
    AND GWS.generatorid = MF.generatorid;
    
        
CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_utility_plant
AS
SELECT
    U.utilityid,
    COALESCE(P.utilityname, U.utilityname) AS utilityname,
    COALESCE(P.streetaddress, U.streetaddress) AS streetaddress,
    COALESCE(P.city, U.city) AS City,
    COALESCE(P.state, U.state) AS state,
    COALESCE(CAST(P.zip AS VARCHAR(100)), U.zip) AS zip,
    U.ownerofplants,
    U.operatorofplants,
    U.assetmanagerofplants,
    U.otherrelationshipswithplants,
    U.entitytype,
    P.plantcode,
    P.plantname,
    P.county,
    P.latitude,
    P.longitude,
    P.nercregion,
    P.balancingauthoritycode,
    P.balancingauthorityname,
    P.nameofwatersource,
    P.primarypurpose_naicscode,
    P.regulatorystatus,
    P.sector,
    P.sectorname,
    P.netmetering_forfacilitieswithsolarorwindgeneration,
    P.ferccogenerationstatus,
    P.ferccogenerationdocketnumber,
    P.fercsmallpowerproducerstatus,
    P.fercsmallpowerproducerdocketnumber,
    P.fercexemptwholesalegeneratorstatus,
    P.fercexemptwholesalegeneratordocketnumber,
    P.ashimpoundment,
    P.ashimpoundmentlined,
    P.ashimpoundmentstatus,
    P.transmissionordistributionsystemowner,
    P.transmissionordistributionsystemownerid,
    P.transmissionordistributionsystemownerstate,
    P.gridvoltage_kv,
    P.gridvoltage2_kv,
    P.gridvoltage3_kv,
    P.naturalgaspipelinename
FROM {schemaName}.{tableName}1_utility U
LEFT JOIN {schemaName}.{tableName}2_plant P 
    ON  U.utilityid = P.utilityid;
    
    
CREATE OR REPLACE VIEW {schemaName}.{tableName}VW_boiler_cooling_info_equip
AS
SELECT
    BC.utilityid,
    BC.utilityname,
    BC.plantcode,
    BC.plantname,
    BC.boilerid,
    BC.coolingid,
    BC.steamplanttype,
    DP.state,
    DP.boilerstatus,
    DP.typeofboiler,
    COALESCE(DP.inservicemonth, EC.inservicemonth) AS inservicemonth,
    COALESCE(DP.inserviceyear, EC.inserviceyear) AS inserviceyear,
    DP.retirementmonth,
    DP.retirementyear,
    DP.firingtype1,
    DP.firingtype2,
    DP.firingtype3,
    DP.maxsteamflow_thousandpoundsperhour,
    DP.firingrateusingcoal_01tonsperhour,
    DP.firingrateusingpetroleum_01barrelsperhour,
    DP.firingrateusinggas_01mcfperhour,
    DP.firingrateusingotherfuels,
    DP.wasteheatinput_millionbtuperhour,
    DP.primaryfuel1,
    DP.primaryfuel2,
    DP.primaryfuel3,
    DP.primaryfuel4,
    DP.turndownratio,
    DP.efficiency100percentload,
    DP.efficiency50percentload,
    DP.airflow100percentload_cubicfeetperminute,
    DP.wetdrybottom,
    DP.flyashreinjection,
    EC.coolingstatus,
    EC.coolingtype1,
    EC.coolingtype2,
    EC.coolingtype3,
    EC.coolingtype4,
    EC.percentdrycooling,
    EC.coolingwatersource,
    EC.coolingwaterdischarge,
    EC.watersourcecode,
    EC.watertypecode,
    EC.intakerateat100percent_gallonsperminute,
    EC.chlorineinservicemonth,
    EC.chlorineinserviceyear,
    EC.pondinservicemonth,
    EC.pondinserviceyear,
    EC.pondsurfacearea_acres,
    EC.pondvolume_acrefeet,
    EC.towerinservicemonth,
    EC.towerinserviceyear,
    EC.towertype1,
    EC.towertype2,
    EC.towertype3,
    EC.towertype4,
    EC.towerwaterrate_gallonsperminute,
    EC.powerrequirement_mw,
    EC.costtotal_thousanddollars,
    EC.costponds_thousanddollars,
    EC.costtowers_thousanddollars,
    EC.costchlorineequipment_thousanddollars,
    EC.intakedistanceshore_feet,
    EC.outletdistanceshore_feet,
    EC.intakedistancesurface_feet,
    EC.outletdistancesurface_feet
FROM {schemaName}.{tableName}enviroassocboilercooling BC
LEFT JOIN {schemaName}.{tableName}enviroequipboilerinfodesignparameters DP
    ON  DP.boilerid =  BC.boilerid
    AND DP.utilityid = BC.utilityid
    AND DP.plantcode = BC.plantcode
LEFT JOIN {schemaName}.{tableName}enviroequipcooling EC
    ON  EC.coolingid = BC.coolingid
    AND EC.utilityid = BC.utilityid
    AND EC.plantcode = BC.plantcode;
    