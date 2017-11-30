--Capture etl error log
INSERT INTO hindsight_etl.etl_errors(userid,slice,tbl,starttime,session,query,filename,line_number,colname,type,col_length,position,raw_line,raw_field_value,err_code,err_reason)
SELECT userid,slice,tbl,starttime,session,query,filename,line_number,colname,type,col_length,position,raw_line,raw_field_value,err_code,err_reason
FROM stl_load_errors 
WHERE CAST(starttime AS DATE)=CAST(GETDATE() AS DATE) 
    AND starttime > (select max(starttime) FROM hindsight_etl.etl_errors)
ORDER BY starttime DESC;


--remove the duplicate rows (problem in the source)
--we already reported this problem to Joan. (this is temporary solution)
TRUNCATE TABLE hindsight_etl.stg_series_data;
INSERT INTO hindsight_etl.stg_series_data(series_id,date,datavalue)
SELECT series_id, date, datavalue
FROM
(
    SELECT series_id, date, datavalue, ROW_NUMBER() OVER(PARTITION BY series_id, date ORDER BY datavalue) AS rownum
    FROM hindsight_temp.etl_series_data --landing table #1
) AS O
WHERE O.rownum=1;


TRUNCATE TABLE hindsight_etl.stg_series_attributes;
INSERT INTO hindsight_etl.stg_series_attributes(series_id,series_key,mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,bank,source,timestamp,wefa_version)
SELECT DISTINCT series_id,CAST(series_key AS INT),mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,bank,source,"timestamp" AS timestamp,wefa_version 
FROM hindsight_temp.etl_series_attributes; --landing table #2



--If the ETL is run more than once on any given day, delete previous rows for that day
DELETE FROM hindsight_etl.row_count_history WHERE date=CAST(GETDATE() AS DATE);

--Capture the rowcount for investigations later if anything goes wrong
INSERT INTO hindsight_etl.row_count_history(tablename, rowcount, date, exec_order)
SELECT 'hindsight_temp.etl_series_attributes' as TableName, count(*) AS RowCount, CAST(GETDATE() AS DATE), 1 from hindsight_temp.etl_series_attributes
UNION ALL
SELECT 'hindsight_temp.etl_series_data' as TableName, count(*) AS RowCount, CAST(GETDATE() AS DATE), 2 from hindsight_temp.etl_series_data
UNION ALL
SELECT 'hindsight_etl.stg_series_data' as TableName, count(*) AS RowCount, CAST(GETDATE() AS DATE), 3 from hindsight_etl.stg_series_data
UNION ALL
SELECT 'hindsight_etl.stg_series_attributes' as TableName, count(*) AS RowCount, CAST(GETDATE() AS DATE), 4 from hindsight_etl.stg_series_attributes;

--cleaning process
--replace NA with empty space
UPDATE hindsight_etl.stg_series_data SET value_cleaned=datavalue;
UPDATE hindsight_etl.stg_series_data SET value_cleaned='' WHERE TRIM(datavalue)='NA' OR TRIM(datavalue)='' OR datavalue IS NULL;
UPDATE hindsight_etl.stg_series_data SET value=CAST(NULL AS FLOAT);--redshift considers space as 0. We do not want that to happen
UPDATE hindsight_etl.stg_series_data SET value=CAST(value_cleaned AS FLOAT) WHERE value_cleaned <> '';
UPDATE hindsight_etl.stg_series_data SET date_cleaned=CAST(date AS DATE);


--build stg_series_data
TRUNCATE TABLE hindsight_etl.stg_series_data_cleaned;
INSERT INTO hindsight_etl.stg_series_data_cleaned(series_id, date, datavalue, version)
SELECT 
    series_id, 
    date_cleaned AS date, 
    value AS datavalue, 
    CAST(GETDATE() AS DATE) AS version
FROM hindsight_etl.stg_series_data;

--Replace empty string with NULL
UPDATE hindsight_etl.stg_series_data_cleaned SET datavalue=CAST(NULL AS FLOAT) WHERE datavalue='';

--build stg_series_attributes
TRUNCATE TABLE hindsight_etl.stg_series_attributes_cleaned;
INSERT INTO hindsight_etl.stg_series_attributes_cleaned(series_id,series_key,mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,bank,source,timestamp,wefa_version)
SELECT series_id,series_key,mnemonic_source,mnemonic,dri_mnemonic,wefa_mnemonic,frequency,seriestype,startdate,enddate,shortlabel,longlabel,conversion,distribution,decimals,status,concept,geo,unit,scale,industry,keyindicator,giirecommended,realnominal,seasonaladjustment,bank,source,"timestamp" AS timestamp,wefa_version
FROM hindsight_etl.stg_series_attributes;

TRUNCATE TABLE hindsight_etl.history_series_data_preload;
INSERT INTO hindsight_etl.history_series_data_preload(series_id, date, datavalue, version, status)
SELECT series_id, date, datavalue, version, status FROM hindsight_etl.modified_series;

INSERT INTO hindsight_etl.history_series_data_preload(series_id, date, datavalue, version, status)
SELECT series_id, date, CAST(NULL AS FLOAT) AS datavalue, version, status FROM hindsight_etl.deleted_series;

INSERT INTO hindsight_etl.history_series_data_preload(series_id, date, datavalue, version, status)
SELECT series_id, date, datavalue, version, status FROM hindsight_etl.inserted_series;

--If the ETL is run more than once on any given day, delete previous rows for that day
DELETE FROM hindsight_etl.row_count_by_seriesid WHERE version=CAST(GETDATE() AS DATE);

---Start the checking process

INSERT INTO hindsight_etl.row_count_by_seriesid(series_id, rowcount, version)
SELECT series_id, count(*) AS rowcount, CAST(GETDATE() AS DATE) AS version
FROM hindsight_etl.stg_series_data_cleaned 
GROUP BY series_id;


TRUNCATE TABLE hindsight_etl.mismatch_from_prior;
INSERT INTO hindsight_etl.mismatch_from_prior(series_id, prior_rowcount, prior_version, current_rowcount, current_version, change, ratio)
SELECT 
    P.series_id, 
    P.rowcount AS V1_rowcount, 
    P.version  AS V1_version,
    L.rowcount AS V2_rowcount, 
    L.version  AS V2_version,
    ABS(L.rowcount-P.rowcount) AS change, 
    ABS(L.rowcount-P.rowcount)/CAST(LEAST(L.rowcount, P.rowcount) AS FLOAT) AS change_ratio
FROM hindsight_etl.row_count_by_seriesid AS L
INNER JOIN hindsight_etl.VW_prior_rowcount_by_seriesid AS P
    ON L.series_id=P.series_id
WHERE L.version=(SELECT MAX(version) FROM hindsight_etl.row_count_by_seriesid)
    AND L.rowcount <> P.rowcount;
    
