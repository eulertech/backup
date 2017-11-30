INSERT INTO {schemaName}.{tableName}dst_start_end(year, dst_start, dst_end)
SELECT 2010, CAST('2010-03-14 02:00:00' AS DATETIME), CAST('2010-11-07 02:00:00' AS DATETIME)
UNION ALL
SELECT 2011, CAST('2011-03-13 02:00:00' AS DATETIME), CAST('2011-11-06 02:00:00' AS DATETIME)
UNION ALL
SELECT 2012, CAST('2012-03-11 02:00:00' AS DATETIME), CAST('2012-11-04 02:00:00' AS DATETIME)
UNION ALL
SELECT 2013, CAST('2013-03-10 02:00:00' AS DATETIME), CAST('2013-11-03 02:00:00' AS DATETIME)
UNION ALL
SELECT 2014, CAST('2014-03-09 02:00:00' AS DATETIME), CAST('2014-11-02 02:00:00' AS DATETIME)
UNION ALL
SELECT 2015, CAST('2015-03-08 02:00:00' AS DATETIME), CAST('2015-11-01 02:00:00' AS DATETIME)
UNION ALL
SELECT 2016, CAST('2016-03-13 02:00:00' AS DATETIME), CAST('2016-11-06 02:00:00' AS DATETIME)
UNION ALL
SELECT 2017, CAST('2017-03-12 02:00:00' AS DATETIME), CAST('2017-11-05 02:00:00' AS DATETIME)
UNION ALL
SELECT 2018, CAST('2018-03-11 02:00:00' AS DATETIME), CAST('2018-11-04 02:00:00' AS DATETIME)
UNION ALL
SELECT 2019, CAST('2019-03-10 02:00:00' AS DATETIME), CAST('2019-11-03 02:00:00' AS DATETIME);


--capturing the data in ercot_dam into a temp table to do the adjustment
INSERT INTO {schemaName}.{tableName}dst_adjustment(DeliveryDate, HourEnding, BusName, LMP, DSTFlag, DeliveryDate_ts)
SELECT DeliveryDate, HourEnding, BusName, LMP, DSTFlag,CAST(RIGHT(D.deliverydate, 4) || '-' || LEFT(D.deliverydate, 2) || '-' || SUBSTRING(D.deliverydate, 4,2) || ' ' || CAST(CAST(LEFT(D.hourending,2) AS INT)-1 AS CHAR(2)) || ':00:00' AS DATETIME) AS DeliveryDate_ts
FROM {schemaName}.{tableName}DAM AS D;

--dst adjustment
UPDATE {schemaName}.{tableName}dst_adjustment
SET adjusted_DeliveryDate_ts=case when deliverydate_ts > dst_start and deliverydate_ts < dst_end and dstFlag='N' THEN dateadd(hour,-1,deliverydate_ts) else deliverydate_ts end
from {schemaName}.{tableName}dst_start_end
where CAST(RIGHT({schemaName}.{tableName}dst_adjustment.deliverydate, 4) AS INT)={schemaName}.{tableName}dst_start_end.year;



DROP TABLE IF EXISTS {schemaName}.{tableName}l2DAM CASCADE;
CREATE TABLE {schemaName}.{tableName}l2DAM
(
    DeliveryDate_ts         DATETIME,
    ISO						VARCHAR(20) ENCODE LZO,
    BusName                 VARCHAR(100) ENCODE LZO,
    LMP                     FLOAT
);

INSERT INTO {schemaName}.{tableName}l2DAM(
    DeliveryDate_ts,
    ISO,
    BusName,
    LMP)
SELECT
    adjusted_DeliveryDate_ts AS DeliveryDate_ts, --adjusted LMP datetime
    'ERCOT' AS ISO,
    BusName,
    CAST(LMP AS FLOAT)         
FROM {schemaName}.{tableName}dst_adjustment;


--INSERT INTO {schemaName}.{tableName}l2DAM(
--    DeliveryDate_ts,
--    ISO,
--    BusName,
--    LMP)
--SELECT
--    CAST(RIGHT(deliverydate, 4) || '-' || LEFT(deliverydate, 2) || '-' || SUBSTRING(deliverydate, 4,2) || ' ' || CAST(CAST(LEFT(hourending,2) AS INT)-1 AS CHAR(2)) || ':00:00' AS DATETIME) AS DeliveryDate_ts,
--    'ERCOT' AS ISO,
--    BusName,
--    CAST(LMP AS FLOAT)         
--FROM {schemaName}.{tableName}DAM
--WHERE DSTFlag='N';


--Get a list of hours from 0-23
DROP TABLE IF EXISTS {schemaName}.{tableName}hours CASCADE;
CREATE TABLE {schemaName}.{tableName}hours
(
    timepart CHAR(9)
);

INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 00:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 01:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 02:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 03:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 04:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 05:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 06:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 07:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 08:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 09:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 10:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 11:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 12:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 13:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 14:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 15:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 16:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 17:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 18:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 19:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 20:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 21:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 22:00:00');
INSERT INTO {schemaName}.{tableName}hours(timepart) VALUES (' 23:00:00');    

--Get the distinct list of BusNames & manufactured timestamps for all dates available for respective BusNames
DROP TABLE IF EXISTS {schemaName}.{tableName}distinct_busname_deliverydate CASCADE;
CREATE TABLE {schemaName}.{tableName}distinct_busname_deliverydate
(
    BusName		VARCHAR(100) ENCODE LZO,
	ts 			DATETIME
);


INSERT INTO {schemaName}.{tableName}distinct_busname_deliverydate 
(
	busname,
	ts
)
SELECT
	O.busname,
	CAST(O.datepart || H.timepart AS DATETIME) AS ts 
FROM
(
    SELECT 
    	busname, 
    	LEFT(CAST(deliverydate_ts AS VARCHAR(25)),10) AS datepart
    FROM {schemaName}.{tableName}l2DAM
    GROUP BY busname, LEFT(CAST(deliverydate_ts AS VARCHAR(25)),10)
) AS O
CROSS JOIN {schemaName}.{tableName}hours AS H;


--Fix the missing rows issue in the timeseries
DROP TABLE IF EXISTS {schemaName}.{tableName}l2DAM_ts CASCADE;
CREATE TABLE {schemaName}.{tableName}l2DAM_ts
(
    BusName		VARCHAR(100) ENCODE LZO,
	ts 			DATETIME,
    ISO         VARCHAR(20) ENCODE LZO,
    LMP         FLOAT
);

--Fix the missing rows issue in the timeseries by inserting null values for the missing timestamps
INSERT INTO {schemaName}.{tableName}l2DAM_ts
(
    BusName,
    ts,
    ISO,
    LMP
)
SELECT
	V.busname,
	V.ts,
	L.ISO,
	L.LMP
FROM {schemaName}.{tableName}distinct_busname_deliverydate AS V
LEFT JOIN {schemaName}.{tableName}l2DAM AS L
	ON V.busname = L.busname
	AND V.ts = L.deliverydate_ts;


	
--Create the timeseries attributes table
DROP TABLE IF EXISTS {schemaName}.{tableName}l2DAM_ts_attributes CASCADE;
CREATE TABLE {schemaName}.{tableName}l2DAM_ts_attributes
(
    BusId       INT,
    BusName		VARCHAR(100) ENCODE LZO,	
    ISO         VARCHAR(20) ENCODE LZO
);


--Populate the timeseries attributes table
INSERT INTO {schemaName}.{tableName}l2DAM_ts_attributes
(
    BusId,
    BusName,
    ISO
)
SELECT
    ROW_NUMBER() OVER(ORDER BY BusName) AS BusId,
    BusName,
    'ERCOT' AS ISO
FROM
(
	SELECT DISTINCT
	    BusName
	FROM {schemaName}.{tableName}l2DAM
) AS O;

--Create the timeseries data table
DROP TABLE IF EXISTS {schemaName}.{tableName}l2DAM_ts_data CASCADE;
CREATE TABLE {schemaName}.{tableName}l2DAM_ts_data
(
    BusId       INT,
	ts 			DATETIME,
    LMP         FLOAT
);


--Populate the timeseries data table
INSERT INTO {schemaName}.{tableName}l2DAM_ts_data
(
    BusId,
	ts,
    LMP
)
SELECT
    A.BusId,
	T.ts,
    T.LMP
FROM {schemaName}.{tableName}l2DAM_ts AS T
INNER JOIN {schemaName}.{tableName}l2DAM_ts_attributes AS A
    ON t.BusName = A.BusName;

    
INSERT INTO {schemaName}.{tableName}l2DAM_ts_Incremental
(
    BusName,
    ts,
    ISO,
    LMP,
    LoadDate
)
SELECT DISTINCT
    BusName,
	ts,
    ISO,
    LMP,
    CAST(GETDATE() AS DATE) AS LoadDate
FROM {schemaName}.{tableName}l2DAM_ts

EXCEPT

SELECT
    BusName,
    ts,
    ISO,
    LMP,
    CAST(GETDATE() AS DATE) AS LoadDate
FROM {schemaName}.{tableName}l2DAM_ts_Incremental;
