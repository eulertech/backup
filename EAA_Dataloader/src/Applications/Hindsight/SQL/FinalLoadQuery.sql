--If the ETL is run more than once on any given day, delete previous rows for that day 
DELETE FROM hindsight_prod.series_data_history WHERE version=CAST(GETDATE() AS DATE);

--Finally insert into the history table after passing all the checks
INSERT INTO hindsight_prod.series_data_history(series_id, date, datavalue, version, status)
SELECT series_id, date, datavalue, version, status 
FROM hindsight_etl.history_series_data_preload;


--If the ETL is run more than once on any given day, delete previous rows for that day 
DELETE FROM hindsight_prod.series_attributes_history WHERE version=CAST(GETDATE() AS DATE);


--These are the attributes that have changed
INSERT INTO hindsight_prod.series_attributes_history
(
    series_id,
    series_key,
    mnemonic_source,
    mnemonic,
    dri_mnemonic,
    wefa_mnemonic,
    frequency,
    seriestype,
    startdate,
    enddate,
    shortlabel,
    longlabel,
    conversion,
    distribution,
    decimals,
    status,
    concept,
    geo,
    unit,
    scale,
    industry,
    keyindicator,
    giirecommended,
    realnominal,
    seasonaladjustment,
    source,
    timestamp,
    wefa_version,
    bank,
    version
)    
SELECT
    s.series_id,
    s.series_key,
    s.mnemonic_source,
    s.mnemonic,
    s.dri_mnemonic,
    s.wefa_mnemonic,
    s.frequency,
    s.seriestype,
    s.startdate,
    s.enddate,
    s.shortlabel,
    s.longlabel,
    s."conversion",
    s.distribution,
    s.decimals,
    s.status,
    s.concept,
    s.geo,
    s.unit,
    s.scale,
    s.industry,
    s.keyindicator,
    s.giirecommended,
    s.realnominal,
    s.seasonaladjustment,
    s.source,
    s."timestamp",
    s.wefa_version,
    s.bank,
    CAST(GETDATE() AS DATE) AS version
FROM hindsight_etl.stg_series_attributes AS S

EXCEPT

SELECT
    h.series_id,
	h.series_key,
	h.mnemonic_source,
	h.mnemonic,
	h.dri_mnemonic,
	h.wefa_mnemonic,
	h.frequency,
	h.seriestype,
	h.startdate,
	h.enddate,
	h.shortlabel,
	h.longlabel,
	h."conversion",
	h.distribution,
	h.decimals,
	h.status,
	h.concept,
	h.geo,
	h.unit,
	h.scale,
	h.industry,
	h.keyindicator,
	h.giirecommended,
	h.realnominal,
	h.seasonaladjustment,
	h.source,
	h."timestamp",
	h.wefa_version,
	h.bank,
    CAST(GETDATE() AS DATE) AS version
FROM hindsight_prod.series_attributes_history AS H
INNER JOIN 
(
    SELECT series_id, MAX(version) AS version
    FROM hindsight_prod.series_attributes_history
    GROUP BY series_id
) AS M
    ON H.series_id=M.series_id
    AND H.version=M.version;



--Finally load the table hindsight_prod.series_data
TRUNCATE TABLE hindsight_prod.series_data;
INSERT INTO hindsight_prod.series_data(series_id, date, datavalue, version)
SELECT series_id, CAST(date_cleaned AS DATE) AS date, value, CAST(GETDATE() AS DATE) AS version 
FROM hindsight_etl.stg_series_data;


--Finally load the table hindsight_prod.series_attributes
TRUNCATE TABLE hindsight_prod.series_attributes;
INSERT INTO hindsight_prod.series_attributes(series_id,series_key,mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,bank,source,timestamp,wefa_version)
SELECT series_id,series_key,mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,bank,source,"timestamp" AS timestamp,wefa_version
FROM hindsight_etl.stg_series_attributes;

