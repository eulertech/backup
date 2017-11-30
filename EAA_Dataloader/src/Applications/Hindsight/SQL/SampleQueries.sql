/*
 * Query Pull historical data
 */
SELECT O.series_id, O.date, O.datavalue, A.frequency
FROM
(        
    SELECT series_id, date, datavalue, version, status,   ROW_NUMBER() OVER(PARTITION BY series_id,  date ORDER BY version DESC) AS rownum
    FROM hindsight_prod.series_data_history
    WHERE version <= '2016-12-31'
) AS O    
LEFT JOIN hindsight_prod.series_attributes AS A
    ON O.series_id=A.series_id;   
WHERE O.rownum=1 AND O.status <> 'D';
--this condition is required to filter out everything other than the latest and the ones that got deleted
--eg. to return data as of 2016-12-31. 
--the join with the series_attributes is to enable filter by some attribute (eg frequency)



/*
 * Query Pull historical attributes
 */
SELECT 
    s.series_id, s.version, a.series_key, a.mnemonic_source, a.mnemonic, 
    a.dri_mnemonic, a.wefa_mnemonic, a.frequency, a.seriestype, a.startdate, 
    a.enddate, a.shortlabel, a.longlabel, a."conversion", a.distribution, 
    a.decimals, a.status, a.concept, a.geo, a.unit, a.scale, a.industry, 
    a.keyindicator, a.giirecommended, a.realnominal, a.seasonaladjustment, 
    a.source, a."timestamp", a.wefa_version, a.bank
FROM 
( 
    SELECT o.series_id, o.version
    FROM 
    ( 
        SELECT series_id, date, datavalue, version, status, pg_catalog.row_number() OVER(PARTITION BY series_id, date ORDER BY version DESC) AS rownum
        FROM hindsight_prod.series_data_history
    ) AS O
    WHERE O.rownum = 1 AND O.status <> 'D'
) AS S
LEFT JOIN hindsight_prod.series_attributes_history AS A 
    ON s.series_id = a.series_id 
    AND s.version = a.version;

    
    
    

/*
 * Pull the most recent snapshot of data (as and when the ETL ran)
 */
SELECT 
       D.series_id, 
       D.date, 
       D.datavalue,
       A.frequency
FROM hindsight_prod.series_data AS D
LEFT JOIN hindsight_prod.series_attributes AS A
      ON D.series_id=A.series_id;

      
/*
 * Pull the most recent snapshot of attributes (as and when the ETL ran) 
 */      
SELECT 
	series_id, version, series_key, mnemonic_source, 
	mnemonic, dri_mnemonic, wefa_mnemonic, frequency, 
	seriestype, startdate, enddate, shortlabel, longlabel, 
	"conversion", distribution, decimals, status, concept, 
	geo, unit, scale, industry, keyindicator, giirecommended, 
	realnominal, seasonaladjustment, source, "timestamp", 
	wefa_version, bank
FROM hindsight_prod.series_attributes;
      