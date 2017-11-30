CREATE TABLE hindsight_etl.etl_errors
(
   userid           integer,
   slice            integer,
   tbl              integer,
   starttime        timestamp,
   session          integer,
   query            integer,
   filename         char(256),
   line_number      bigint,
   colname          char(127),
   type             char(10),
   col_length       char(10),
   position         integer,
   raw_line         char(1024),
   raw_field_value  char(1024),
   err_code         integer,
   err_reason       char(100)
);



CREATE TABLE hindsight_etl.row_count_history
(
   tablename   varchar(100),
   rowcount    integer,
   date        date,
   exec_order  integer
);


CREATE TABLE hindsight_etl.row_count_by_seriesid
(
   series_id    INT,
   rowcount     INT,
   version      date
);


CREATE TABLE hindsight_etl.history_series_data_preload
(
   series_id  integer,
   date       date,
   datavalue  float8,
   version    date,
   status     char(1)
);

CREATE TABLE hindsight_etl.stg_series_data_cleaned
(
   series_id  integer,
   date       date,
   datavalue  float8,
   version    date
);

CREATE TABLE hindsight_temp.etl_series_attributes
(
   series_id           INTEGER NOT NULL,
   series_key          INTEGER,
   mnemonic_source     VARCHAR(15) ENCODE LZO,
   mnemonic            VARCHAR(50) ENCODE LZO,
   dri_mnemonic        VARCHAR(50) ENCODE LZO,
   wefa_mnemonic       VARCHAR(50) ENCODE LZO,
   frequency           VARCHAR(20) ENCODE LZO,
   seriestype          VARCHAR(15) ENCODE LZO,
   startdate           VARCHAR(20) ENCODE LZO,
   enddate             VARCHAR(20) ENCODE LZO,
   shortlabel          VARCHAR(408) ENCODE LZO,
   longlabel           VARCHAR(1024) ENCODE LZO,
   conversion          VARCHAR(15) ENCODE LZO,
   distribution        VARCHAR(15) ENCODE LZO,
   decimals            VARCHAR(5) ENCODE LZO,
   status              VARCHAR(15) ENCODE LZO,
   concept             VARCHAR(15) ENCODE LZO,
   geo                 VARCHAR(15) ENCODE LZO,
   unit                VARCHAR(15) ENCODE LZO,
   scale               VARCHAR(50) ENCODE LZO,
   industry            VARCHAR(15) ENCODE LZO,
   keyindicator        VARCHAR(15) ENCODE LZO,
   giirecommended      VARCHAR(15) ENCODE LZO,
   realnominal         VARCHAR(10) ENCODE LZO,
   seasonaladjustment  VARCHAR(15) ENCODE LZO,
   bank                VARCHAR(2048) ENCODE LZO,
   source              VARCHAR(1024) ENCODE LZO,
   timestamp           VARCHAR(30) ENCODE LZO,
   wefa_version        VARCHAR(50) ENCODE LZO
);


CREATE TABLE hindsight_etl.stg_series_attributes_cleaned
(
   series_id           INTEGER NOT NULL,
   series_key          INTEGER,
   mnemonic_source     VARCHAR(15) ENCODE LZO,
   mnemonic            VARCHAR(50) ENCODE LZO,
   dri_mnemonic        VARCHAR(50) ENCODE LZO,
   wefa_mnemonic       VARCHAR(50) ENCODE LZO,
   frequency           VARCHAR(20) ENCODE LZO,
   seriestype          VARCHAR(15) ENCODE LZO,
   startdate           VARCHAR(20) ENCODE LZO,
   enddate             VARCHAR(20) ENCODE LZO,
   shortlabel          VARCHAR(408) ENCODE LZO,
   longlabel           VARCHAR(1024) ENCODE LZO,
   conversion          VARCHAR(15) ENCODE LZO,
   distribution        VARCHAR(15) ENCODE LZO,
   decimals            VARCHAR(5) ENCODE LZO,
   status              VARCHAR(15) ENCODE LZO,
   concept             VARCHAR(15) ENCODE LZO,
   geo                 VARCHAR(15) ENCODE LZO,
   unit                VARCHAR(15) ENCODE LZO,
   scale               VARCHAR(50) ENCODE LZO,
   industry            VARCHAR(15) ENCODE LZO,
   keyindicator        VARCHAR(15) ENCODE LZO,
   giirecommended      VARCHAR(15) ENCODE LZO,
   realnominal         VARCHAR(10) ENCODE LZO,
   seasonaladjustment  VARCHAR(15) ENCODE LZO,
   source              VARCHAR(1024) ENCODE LZO,
   timestamp           VARCHAR(30) ENCODE LZO,
   wefa_version        VARCHAR(50) ENCODE LZO,
   bank                VARCHAR(2048) ENCODE LZO
); 

CREATE TABLE hindsight_etl.stg_series_attributes
(
   series_id           INTEGER NOT NULL,
   series_key          INTEGER,
   mnemonic_source     VARCHAR(15) ENCODE LZO,
   mnemonic            VARCHAR(50) ENCODE LZO,
   dri_mnemonic        VARCHAR(50) ENCODE LZO,
   wefa_mnemonic       VARCHAR(50) ENCODE LZO,
   frequency           VARCHAR(20) ENCODE LZO,
   seriestype          VARCHAR(15) ENCODE LZO,
   startdate           VARCHAR(20) ENCODE LZO,
   enddate             VARCHAR(20) ENCODE LZO,
   shortlabel          VARCHAR(408) ENCODE LZO,
   longlabel           VARCHAR(1024) ENCODE LZO,
   conversion          VARCHAR(15) ENCODE LZO,
   distribution        VARCHAR(15) ENCODE LZO,
   decimals            VARCHAR(5) ENCODE LZO,
   status              VARCHAR(15) ENCODE LZO,
   concept             VARCHAR(15) ENCODE LZO,
   geo                 VARCHAR(15) ENCODE LZO,
   unit                VARCHAR(15) ENCODE LZO,
   scale               VARCHAR(50) ENCODE LZO,
   industry            VARCHAR(15) ENCODE LZO,
   keyindicator        VARCHAR(15) ENCODE LZO,
   giirecommended      VARCHAR(15) ENCODE LZO,
   realnominal         VARCHAR(10) ENCODE LZO,
   seasonaladjustment  VARCHAR(15) ENCODE LZO,
   source              VARCHAR(1024) ENCODE LZO,
   timestamp           VARCHAR(30) ENCODE LZO,
   wefa_version        VARCHAR(50) ENCODE LZO,
   bank                VARCHAR(2048) ENCODE LZO
);


CREATE TABLE hindsight_prod.series_attributes
(
   series_id           INTEGER NOT NULL,
   series_key          INTEGER,
   mnemonic_source     VARCHAR(15) ENCODE LZO,
   mnemonic            VARCHAR(50) ENCODE LZO,
   dri_mnemonic        VARCHAR(50) ENCODE LZO,
   wefa_mnemonic       VARCHAR(50) ENCODE LZO,
   frequency           VARCHAR(20) ENCODE LZO,
   seriestype          VARCHAR(15) ENCODE LZO,
   startdate           VARCHAR(20) ENCODE LZO,
   enddate             VARCHAR(20) ENCODE LZO,
   shortlabel          VARCHAR(408) ENCODE LZO,
   longlabel           VARCHAR(1024) ENCODE LZO,
   conversion          VARCHAR(15) ENCODE LZO,
   distribution        VARCHAR(15) ENCODE LZO,
   decimals            VARCHAR(5) ENCODE LZO,
   status              VARCHAR(15) ENCODE LZO,
   concept             VARCHAR(15) ENCODE LZO,
   geo                 VARCHAR(15) ENCODE LZO,
   unit                VARCHAR(15) ENCODE LZO,
   scale               VARCHAR(50) ENCODE LZO,
   industry            VARCHAR(15) ENCODE LZO,
   keyindicator        VARCHAR(15) ENCODE LZO,
   giirecommended      VARCHAR(15) ENCODE LZO,
   realnominal         VARCHAR(10) ENCODE LZO,
   seasonaladjustment  VARCHAR(15) ENCODE LZO,
   source              VARCHAR(1024) ENCODE LZO,
   timestamp           VARCHAR(30) ENCODE LZO,
   wefa_version        VARCHAR(50) ENCODE LZO,
   bank                VARCHAR(2048) ENCODE LZO
);


CREATE TABLE hindsight_prod.series_attributes_history
(
   series_id           INTEGER NOT NULL,
   series_key          INTEGER,
   mnemonic_source     VARCHAR(15) ENCODE LZO,
   mnemonic            VARCHAR(50) ENCODE LZO,
   dri_mnemonic        VARCHAR(50) ENCODE LZO,
   wefa_mnemonic       VARCHAR(50) ENCODE LZO,
   frequency           VARCHAR(20) ENCODE LZO,
   seriestype          VARCHAR(15) ENCODE LZO,
   startdate           VARCHAR(20) ENCODE LZO,
   enddate             VARCHAR(20) ENCODE LZO,
   shortlabel          VARCHAR(408) ENCODE LZO,
   longlabel           VARCHAR(1024) ENCODE LZO,
   conversion          VARCHAR(15) ENCODE LZO,
   distribution        VARCHAR(15) ENCODE LZO,
   decimals            VARCHAR(5) ENCODE LZO,
   status              VARCHAR(15) ENCODE LZO,
   concept             VARCHAR(15) ENCODE LZO,
   geo                 VARCHAR(15) ENCODE LZO,
   unit                VARCHAR(15) ENCODE LZO,
   scale               VARCHAR(50) ENCODE LZO,
   industry            VARCHAR(15) ENCODE LZO,
   keyindicator        VARCHAR(15) ENCODE LZO,
   giirecommended      VARCHAR(15) ENCODE LZO,
   realnominal         VARCHAR(10) ENCODE LZO,
   seasonaladjustment  VARCHAR(15) ENCODE LZO,
   source              VARCHAR(1024) ENCODE LZO,
   timestamp           VARCHAR(30) ENCODE LZO,
   wefa_version        VARCHAR(50) ENCODE LZO,
   bank                VARCHAR(2048) ENCODE LZO,
   version			   DATE
);

CREATE OR REPLACE VIEW hindsight_etl.vw_get_latest_series_data_from_history
AS
SELECT series_id, date, datavalue 
FROM
(
    SELECT
        series_id, date, datavalue, version, status,
        ROW_NUMBER() OVER(PARTITION BY series_id, date ORDER BY version desc) AS rownum
    FROM hindsight_prod.series_data_history
) AS O
WHERE O.rownum=1
    AND O.status <> 'D';
    

CREATE OR REPLACE VIEW hindsight_etl.vw_get_latest_series_attributes_from_history
AS
SELECT
    H.series_id,
    H.series_key,
    H.mnemonic_source,
    H.mnemonic,
    H.dri_mnemonic,
    H.wefa_mnemonic,
    H.frequency,
    H.seriestype,
    H.startdate,
    H.enddate,
    H.shortlabel,
    H.longlabel,
    H."conversion" AS conversion,
    H.distribution,
    H.decimals,
    H.status,
    H.concept,
    H.geo,
    H.unit,
    H.scale,
    H.industry,
    H.keyindicator,
    H.giirecommended,
    H.realnominal,
    H.seasonaladjustment,
    H.source,
    H."timestamp" AS timestamp,
    H.wefa_version,
    H.bank,
    H.version       
FROM hindsight_prod.series_attributes_history AS H
INNER JOIN 
(
    SELECT series_id, MAX(version) AS version
    FROM hindsight_prod.series_attributes_history
    GROUP BY series_id
) AS M
    ON H.series_id=M.series_id
    AND H.version=M.version;

    
--The modified ones
--The series_id-date combo is present in both the latest & history, but the datavalues don't match    
--Avoid datavalue to datavalue comparison. This column has nulls in it. Comparison would throw logic errors
CREATE OR REPLACE VIEW hindsight_etl.modified_series
AS
SELECT L.series_id, L.date , L.datavalue, CAST(GETDATE() AS DATE) AS version, 'M' AS status
FROM
(
    SELECT series_id, date 
    FROM
    (
        SELECT series_id, date, datavalue FROM hindsight_etl.stg_series_data_cleaned
        EXCEPT 
        SELECT series_id, date, datavalue FROM hindsight_etl.vw_get_latest_series_data_from_history
    ) AS O1 --includes status M and I

    INTERSECT

    SELECT series_id, date 
    FROM
    (
        SELECT series_id, date FROM hindsight_etl.stg_series_data_cleaned
        INTERSECT
        SELECT series_id, date FROM hindsight_etl.vw_get_latest_series_data_from_history
    ) AS O2 --includes status M and NO_CHANGE
) AS O3
INNER JOIN hindsight_etl.stg_series_data_cleaned AS L
    ON O3.series_id=L.series_id
    AND O3.date=L.date;
    
    
    
--The series_id-date combos which are in H-L except the ones whose datavalue was modified
CREATE OR REPLACE VIEW hindsight_etl.deleted_series
AS
SELECT O.series_id, O.date, CAST(GETDATE() AS DATE) AS version, 'D' AS status
FROM
(
    SELECT series_id, date FROM hindsight_etl.vw_get_latest_series_data_from_history
    EXCEPT
    SELECT series_id, date FROM hindsight_etl.stg_series_data_cleaned
) AS O;

    
--The inserted ones
--The series_id-date combos which are in L-H except the ones whose datavalue was modified
CREATE OR REPLACE VIEW hindsight_etl.inserted_series
AS
SELECT L.series_id, L.date, L.datavalue, CAST(GETDATE() AS DATE) AS version, 'I' AS status
FROM
(
    SELECT series_id, date FROM hindsight_etl.stg_series_data_cleaned
    EXCEPT 
    SELECT series_id, date FROM hindsight_etl.vw_get_latest_series_data_from_history
) AS O
INNER JOIN hindsight_etl.stg_series_data_cleaned AS L
    ON O.series_id=L.series_id
    AND O.date=L.date;



CREATE TABLE hindsight_etl.mismatch_from_prior
(
    series_id           INT,
    prior_rowcount      INT,
    prior_version       DATE,
    current_rowcount    INT,
    current_version     DATE,
    ratio               FLOAT
);


CREATE OR REPLACE VIEW hindsight_etl.VW_prior_rowcount_by_seriesid
AS
SELECT R.series_id, R.version, R.rowcount
FROM hindsight_etl.row_count_by_seriesid AS R
INNER JOIN
(
    SELECT series_id, MAX(version) AS version
    FROM hindsight_etl.row_count_by_seriesid
    WHERE version < (SELECT MAX(version) FROM hindsight_etl.row_count_by_seriesid)
    GROUP BY series_id
) AS RM
    ON R.series_id=RM.series_id
    AND R.version=RM.version;
      


CREATE OR REPLACE VIEW hindsight_etl.VW_real_rowcount_mismatch_across_version
AS
SELECT M.series_id
    ,S.frequency
    ,M.prior_version
    ,M.prior_rowcount
    ,TO_DATE(H.startdate, 'mm/dd/YYYY') AS old_startdate
    ,TO_DATE(H.enddate, 'mm/dd/YYYY') AS old_enddate    
    ,CASE UPPER(S.frequency)
        WHEN 'ANNL' THEN DATEDIFF(yr,  TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'DAY'  THEN DATEDIFF(d,   TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'MONT' THEN DATEDIFF(mon, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'QUAR' THEN DATEDIFF(qtr, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'WFR'  THEN DATEDIFF(day, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'WSU'  THEN DATEDIFF(day, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'WTU'  THEN DATEDIFF(day, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'WMO'  THEN DATEDIFF(day, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'SANN' THEN CAST((CAST(DATEDIFF(d, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) + 1 AS FLOAT)/365)*2 AS INT) + 1
        ELSE DATEDIFF(d, TO_DATE(H.startdate, 'mm/dd/YYYY'), TO_DATE(H.enddate, 'mm/dd/YYYY')) + 1
    END AS old_date_diff    
    ,M.current_version
    ,M.current_rowcount
    ,TO_DATE(S.startdate, 'mm/dd/YYYY') AS latest_startdate
    ,TO_DATE(S.enddate, 'mm/dd/YYYY') AS latest_enddate
    ,CASE UPPER(S.frequency)
        WHEN 'ANNL' THEN DATEDIFF(yr,  TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'DAY'  THEN DATEDIFF(d,   TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'MONT' THEN DATEDIFF(mon, TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'QUAR' THEN DATEDIFF(qtr, TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
        WHEN 'WFR'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'WSU'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'WTU'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'WMO'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'),  TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
        WHEN 'SANN' THEN CAST((CAST(DATEDIFF(d, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1 AS FLOAT)/365)*2 AS INT) + 1
        ELSE DATEDIFF(d, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
    END AS latest_date_diff
    ,M.change
    ,M.ratio
FROM hindsight_etl.mismatch_from_prior AS M
INNER JOIN hindsight_etl.stg_series_attributes_cleaned AS S
    ON M.series_id=S.series_id
LEFT JOIN hindsight_etl.vw_get_latest_series_attributes_from_history AS H
    ON M.series_id=H.series_id
WHERE current_rowcount != CASE UPPER(S.frequency)
					        WHEN 'ANNL' THEN DATEDIFF(yr,  TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
					        WHEN 'DAY'  THEN DATEDIFF(d,   TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
					        WHEN 'MONT' THEN DATEDIFF(mon, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
					        WHEN 'QUAR' THEN DATEDIFF(qtr, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
					        WHEN 'WFR'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
					        WHEN 'WSU'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
					        WHEN 'WTU'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
					        WHEN 'WMO'  THEN DATEDIFF(day, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) / 7 + 1
					        WHEN 'SANN' THEN CAST((CAST(DATEDIFF(d, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1 AS FLOAT)/365)*2 AS INT) + 1
                            ELSE DATEDIFF(d, TO_DATE(S.startdate, 'mm/dd/YYYY'), TO_DATE(S.enddate, 'mm/dd/YYYY')) + 1
                          END;

                          