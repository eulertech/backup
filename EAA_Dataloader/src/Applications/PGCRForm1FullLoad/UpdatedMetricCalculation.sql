--Create an aggregated & consolidated table with all the metrics under review
--Pull all values from the base tables
DROP TABLE IF EXISTS pgcr_dev.plant_metrics;
SELECT 
    state
    ,year
    ,facility_id_orispl
    ,facility_name
    ,unit_id
    ,unit_type
    ,fuel_type_primary
    ,fuel_type_secondary
    ,MAX(gross_load_mw) AS max_gross_load_mw
    ,SUM(gross_load_mw) AS sum_gross_load_mw
INTO pgcr_dev.plant_metrics    
FROM pgcr_prod.airmarketsfull_emission
GROUP BY 
    state
    ,year
    ,facility_id_orispl
    ,facility_name
    ,unit_id
    ,unit_type
    ,fuel_type_primary
    ,fuel_type_secondary;
    
    
-- select top 10 * from pgcr_dev.plant_metrics;

--Add nameplatecapacity_mw from 860    
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN nameplatecapacity_mw FLOAT;    
   
UPDATE pgcr_dev.plant_metrics
SET nameplatecapacity_mw=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.nameplatecapacity_mw
FROM    pgcr_prod.eia860_vw_generator_wind_solar_multifuel
WHERE   pgcr_dev.plant_metrics.state               = pgcr_prod.eia860_vw_generator_wind_solar_multifuel.state
    AND pgcr_dev.plant_metrics.facility_id_orispl  = pgcr_prod.eia860_vw_generator_wind_solar_multifuel.plantcode
    AND pgcr_dev.plant_metrics.unit_id             = pgcr_prod.eia860_vw_generator_wind_solar_multifuel.generatorid;    
    

-- select top 10 * from pgcr_dev.plant_metrics where nameplatecapacity_mw is not null;

--compute the 2 versions of estimated nameplatecapacity_mw
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN est_nameplatecapacity_mw FLOAT;    
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN est_nameplatecapacity_mw_ver2 FLOAT;    

UPDATE pgcr_dev.plant_metrics 
SET est_nameplatecapacity_mw=CASE 
                            WHEN (nameplatecapacity_mw IS NULL OR nameplatecapacity_mw=0) THEN max_gross_load_mw
                            WHEN nameplatecapacity_mw > max_gross_load_mw THEN nameplatecapacity_mw
                            WHEN nameplatecapacity_mw < max_gross_load_mw THEN max_gross_load_mw
                            END;

UPDATE pgcr_dev.plant_metrics SET est_nameplatecapacity_mw_ver2= max_gross_load_mw;

-- select top 10 * from pgcr_dev.plant_metrics where nameplatecapacity_mw is not null;

--Calculating the full load heat rate version 1
DROP TABLE IF EXISTS pgcr_dev.FLHR_1_ver1 CASCADE;
SELECT 
    A.state
   ,A.year
   ,A.facility_id_orispl
   ,A.unit_id
   ,P.nameplatecapacity_mw
   ,A.gross_load_mw
   ,CASE WHEN (A.gross_load_mw IS NOT NULL AND A.gross_load_mw <> 0) THEN A.heat_input_mmbtu/A.gross_load_mw ELSE 0 END AS hr_gl
   ,0::FLOAT AS gross_load_mw_denom
   ,0::FLOAT AS wtavg_heatrate
INTO pgcr_dev.FLHR_1_ver1
FROM pgcr_prod.airmarketsfull_emission AS A
INNER JOIN pgcr_dev.plant_metrics AS P
    ON  A.state              = P.state
    AND A.year               = P.year
    AND A.facility_id_orispl = P.facility_id_orispl
    AND A.unit_id            = P.unit_id
WHERE   A.gross_load_mw >= P.est_nameplatecapacity_mw*0.9;   

-- select top 20 * from pgcr_dev.FLHR_1_ver1;

DROP VIEW IF EXISTS pgcr_dev.VW_sum_gross_load90_ver1 CASCADE;
CREATE VIEW pgcr_dev.VW_sum_gross_load90_ver1
AS
SELECT 
    state
   ,year
   ,facility_id_orispl
   ,unit_id
   ,SUM(gross_load_mw) AS sum_gross_load_mw90v1
FROM pgcr_dev.FLHR_1_ver1
GROUP BY
    state
    ,year
    ,facility_id_orispl
    ,unit_id;


-- select top 30 * from pgcr_dev.VW_sum_gross_load90_ver1;

UPDATE pgcr_dev.FLHR_1_ver1 SET gross_load_mw_denom = pgcr_dev.VW_sum_gross_load90_ver1.sum_gross_load_mw90v1
FROM pgcr_dev.VW_sum_gross_load90_ver1
WHERE pgcr_dev.FLHR_1_ver1.year                 = pgcr_dev.VW_sum_gross_load90_ver1.year
AND   pgcr_dev.FLHR_1_ver1.state                = pgcr_dev.VW_sum_gross_load90_ver1.state
AND   pgcr_dev.FLHR_1_ver1.facility_id_orispl   = pgcr_dev.VW_sum_gross_load90_ver1.facility_id_orispl
AND   pgcr_dev.FLHR_1_ver1.unit_id              = pgcr_dev.VW_sum_gross_load90_ver1.unit_id;

UPDATE pgcr_dev.FLHR_1_ver1 set wtavg_heatrate=hr_gl/gross_load_mw_denom WHERE gross_load_mw_denom<>0;
                 
DROP VIEW if exists pgcr_dev.FLHR_ver1 CASCADE;
CREATE VIEW pgcr_dev.FLHR_ver1
AS
    SELECT 
        state
        ,year
        ,facility_id_orispl
        ,unit_id
        ,SUM(wtavg_heatrate) AS FLHR_v1
    FROM pgcr_dev.FLHR_1_ver1
    GROUP BY 
        state
        ,year
        ,facility_id_orispl
        ,unit_id;


-- select top 20 * from pgcr_dev.FLHR_ver1;        

ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN FLHR_v1 FLOAT;
    
UPDATE pgcr_dev.plant_metrics 
SET FLHR_v1=pgcr_dev.FLHR_ver1.FLHR_v1
FROM pgcr_dev.FLHR_ver1
WHERE pgcr_dev.plant_metrics.year               = pgcr_dev.FLHR_ver1.year
AND   pgcr_dev.plant_metrics.state              = pgcr_dev.FLHR_ver1.state
AND   pgcr_dev.plant_metrics.facility_id_orispl = pgcr_dev.FLHR_ver1.facility_id_orispl
AND   pgcr_dev.plant_metrics.unit_id            = pgcr_dev.FLHR_ver1.unit_id;

--select distinct FLHR_v1 from pgcr_dev.plant_metrics;

--select * from pgcr_dev.plant_metrics where FLHR_V1 in (select top 5 FLHR_v1 from pgcr_dev.plant_metrics where FLHR_v1 is not null order by FLHR_V1 desc;
--select * from pgcr_prod.airmarketsfull_emission where state='OK' and year=2017 and facility_id_orispl=4940 and unit_id=1501;

/*
-----------------Calculation of weighted average full load heat rate (weighted by gross load) Version 2 starts here----------------
weighted average by output level/load
(gross_load_mw >= 0.9 * est_nameplatecapacity_mw_ver2
*/
--Calculating the full load heat rate version 2
DROP TABLE IF EXISTS pgcr_dev.FLHR_1_ver2 CASCADE;
SELECT 
     A.state
    ,A.year
    ,A.facility_id_orispl
    ,A.unit_id
    ,A.gross_load_mw
    ,CASE WHEN (A.gross_load_mw IS NOT NULL AND A.gross_load_mw <> 0) THEN A.heat_input_mmbtu/A.gross_load_mw ELSE 0 END AS hr_gl
    ,0::FLOAT AS gross_load_mw_denom
    ,0::FLOAT AS wtavg_heatrate
INTO pgcr_dev.FLHR_1_ver2
FROM pgcr_prod.airmarketsfull_emission AS A
INNER JOIN pgcr_dev.plant_metrics AS P
    ON  A.state              = P.state
    AND A.year               = P.year
    AND A.facility_id_orispl = P.facility_id_orispl
    AND A.unit_id            = P.unit_id
WHERE   A.gross_load_mw >= P.est_nameplatecapacity_mw_ver2*0.9;


DROP VIEW IF EXISTS pgcr_dev.VW_sum_gross_load90_ver2 CASCADE;
CREATE VIEW pgcr_dev.VW_sum_gross_load90_ver2
AS
SELECT 
    state
   ,year
   ,facility_id_orispl
   ,unit_id
   ,SUM(gross_load_mw) AS sum_gross_load_mw90v2
FROM pgcr_dev.FLHR_1_ver2
GROUP BY
    state
    ,year
    ,facility_id_orispl
    ,unit_id;

    
UPDATE pgcr_dev.FLHR_1_ver2 SET gross_load_mw_denom = pgcr_dev.VW_sum_gross_load90_ver2.sum_gross_load_mw90v2   
FROM pgcr_dev.VW_sum_gross_load90_ver2
WHERE pgcr_dev.FLHR_1_ver2.year                 = pgcr_dev.VW_sum_gross_load90_ver2.year
AND   pgcr_dev.FLHR_1_ver2.state                = pgcr_dev.VW_sum_gross_load90_ver2.state
AND   pgcr_dev.FLHR_1_ver2.facility_id_orispl   = pgcr_dev.VW_sum_gross_load90_ver2.facility_id_orispl
AND   pgcr_dev.FLHR_1_ver2.unit_id              = pgcr_dev.VW_sum_gross_load90_ver2.unit_id;

UPDATE pgcr_dev.FLHR_1_ver2 set wtavg_heatrate=hr_gl/gross_load_mw_denom WHERE gross_load_mw_denom<>0;



DROP VIEW if exists pgcr_dev.FLHR_ver2 CASCADE;
CREATE VIEW pgcr_dev.FLHR_ver2
AS
SELECT 
        state
        ,year
        ,facility_id_orispl
        ,unit_id
        ,SUM(wtavg_heatrate) AS FLHR_v2
    FROM pgcr_dev.FLHR_1_ver2
    GROUP BY 
        state
        ,year
        ,facility_id_orispl
        ,unit_id;

-- select top 20 * from pgcr_dev.FLHR_ver2;
        
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN FLHR_v2 FLOAT;
        
UPDATE pgcr_dev.plant_metrics 
SET FLHR_v2=pgcr_dev.FLHR_ver2.FLHR_v2
FROM pgcr_dev.FLHR_ver2
WHERE pgcr_dev.plant_metrics.year               = pgcr_dev.FLHR_ver2.year
AND   pgcr_dev.plant_metrics.state              = pgcr_dev.FLHR_ver2.state
AND   pgcr_dev.plant_metrics.facility_id_orispl = pgcr_dev.FLHR_ver2.facility_id_orispl
AND   pgcr_dev.plant_metrics.unit_id            = pgcr_dev.FLHR_ver2.unit_id;


-- select distinct FLHR_v2 from pgcr_dev.plant_metrics;

/*
-----------------Calculation of weighted average full load heat rate (weighted by gross load) Version 2 ends here----------------
(gross_load_mw >= 0.9 * est_nameplatecapacity_mw
*/



--Calculate avg heat rate Version 1
DROP TABLE IF EXISTS pgcr_dev.AHR_1_ver1 CASCADE;
SELECT 
    A.state
   ,A.year
   ,A.facility_id_orispl
   ,A.unit_id
   ,A.gross_load_mw
   ,CASE WHEN (A.gross_load_mw IS NOT NULL AND A.gross_load_mw <> 0) THEN A.heat_input_mmbtu/A.gross_load_mw ELSE 0 END AS hr_gl
   ,0::FLOAT AS gross_load_mw_denom
   ,0::FLOAT AS wtavg_heatrate
INTO pgcr_dev.AHR_1_ver1
FROM pgcr_prod.airmarketsfull_emission AS A
INNER JOIN pgcr_dev.plant_metrics AS P
    ON  A.state              = P.state
    AND A.year               = P.year
    AND A.facility_id_orispl = P.facility_id_orispl
    AND A.unit_id            = P.unit_id
WHERE   gross_load_mw >= est_nameplatecapacity_mw*0.3;   


DROP VIEW IF EXISTS pgcr_dev.VW_sum_gross_load30_ver1 CASCADE;
CREATE VIEW pgcr_dev.VW_sum_gross_load30_ver1
AS
SELECT 
    state
   ,year
   ,facility_id_orispl
   ,unit_id
   ,SUM(gross_load_mw) AS sum_gross_load_mw30v1
FROM pgcr_dev.AHR_1_ver1
GROUP BY
    state
    ,year
    ,facility_id_orispl
    ,unit_id;

    
    
UPDATE pgcr_dev.AHR_1_ver1 SET gross_load_mw_denom = pgcr_dev.VW_sum_gross_load30_ver1.sum_gross_load_mw30v1   
FROM pgcr_dev.VW_sum_gross_load30_ver1
WHERE pgcr_dev.AHR_1_ver1.year                  = pgcr_dev.VW_sum_gross_load30_ver1.year
AND   pgcr_dev.AHR_1_ver1.state                 = pgcr_dev.VW_sum_gross_load30_ver1.state
AND   pgcr_dev.AHR_1_ver1.facility_id_orispl    = pgcr_dev.VW_sum_gross_load30_ver1.facility_id_orispl
AND   pgcr_dev.AHR_1_ver1.unit_id               = pgcr_dev.VW_sum_gross_load30_ver1.unit_id;

UPDATE pgcr_dev.AHR_1_ver1 set wtavg_heatrate=hr_gl/gross_load_mw_denom WHERE gross_load_mw_denom<>0;
                 
DROP VIEW if exists pgcr_dev.AHR_ver1 CASCADE;
CREATE VIEW pgcr_dev.AHR_ver1
AS
    SELECT 
        state
        ,year
        ,facility_id_orispl
        ,unit_id
        ,SUM(wtavg_heatrate) AS AHR_v1
    FROM pgcr_dev.AHR_1_ver1
    GROUP BY 
        state
        ,year
        ,facility_id_orispl
        ,unit_id;
        
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN AHR_v1 FLOAT;
    
UPDATE pgcr_dev.plant_metrics 
SET AHR_v1=pgcr_dev.AHR_ver1.AHR_v1
FROM pgcr_dev.AHR_ver1
WHERE pgcr_dev.plant_metrics.year               = pgcr_dev.AHR_ver1.year
AND   pgcr_dev.plant_metrics.state              = pgcr_dev.AHR_ver1.state
AND   pgcr_dev.plant_metrics.facility_id_orispl = pgcr_dev.AHR_ver1.facility_id_orispl
AND   pgcr_dev.plant_metrics.unit_id            = pgcr_dev.AHR_ver1.unit_id;


-- select top 20 * from pgcr_dev.plant_metrics  where ahr_v1 is not null;

--Calculate avg heat rate Version 2
DROP TABLE IF EXISTS pgcr_dev.AHR_1_ver2 CASCADE;
SELECT 
     A.state
    ,A.year
    ,A.facility_id_orispl
    ,A.unit_id
    ,A.gross_load_mw
    ,CASE WHEN (A.gross_load_mw IS NOT NULL AND A.gross_load_mw <> 0) THEN A.heat_input_mmbtu/A.gross_load_mw ELSE 0 END AS hr_gl
    ,0::FLOAT AS gross_load_mw_denom
    ,0::FLOAT AS wtavg_heatrate
INTO pgcr_dev.AHR_1_ver2
FROM pgcr_prod.airmarketsfull_emission AS A
INNER JOIN pgcr_dev.plant_metrics AS P
    ON  A.state              = P.state
    AND A.year               = P.year
    AND A.facility_id_orispl = P.facility_id_orispl
    AND A.unit_id            = P.unit_id
WHERE   gross_load_mw >= est_nameplatecapacity_mw_ver2*0.3;


DROP VIEW IF EXISTS pgcr_dev.VW_sum_gross_load30_ver2 CASCADE;
CREATE VIEW pgcr_dev.VW_sum_gross_load30_ver2
AS
SELECT 
    state
   ,year
   ,facility_id_orispl
   ,unit_id
   ,SUM(gross_load_mw) AS sum_gross_load_mw30v2
FROM pgcr_dev.AHR_1_ver1
GROUP BY
    state
    ,year
    ,facility_id_orispl
    ,unit_id;

    
    
UPDATE pgcr_dev.AHR_1_ver2 SET gross_load_mw_denom = pgcr_dev.VW_sum_gross_load30_ver2.sum_gross_load_mw30v2   
FROM pgcr_dev.VW_sum_gross_load30_ver2
WHERE pgcr_dev.AHR_1_ver2.year                  = pgcr_dev.VW_sum_gross_load30_ver2.year
AND   pgcr_dev.AHR_1_ver2.state                 = pgcr_dev.VW_sum_gross_load30_ver2.state
AND   pgcr_dev.AHR_1_ver2.facility_id_orispl    = pgcr_dev.VW_sum_gross_load30_ver2.facility_id_orispl
AND   pgcr_dev.AHR_1_ver2.unit_id               = pgcr_dev.VW_sum_gross_load30_ver2.unit_id;

UPDATE pgcr_dev.AHR_1_ver2 set wtavg_heatrate=hr_gl/gross_load_mw_denom WHERE gross_load_mw_denom<>0;

       
DROP VIEW if exists pgcr_dev.AHR_ver2 CASCADE;
CREATE VIEW pgcr_dev.AHR_ver2
AS
    SELECT 
        state
        ,year
        ,facility_id_orispl
        ,unit_id
        ,SUM(wtavg_heatrate) AS AHR_v2
    FROM pgcr_dev.AHR_1_ver2
    GROUP BY 
        state
        ,year
        ,facility_id_orispl
        ,unit_id;
         
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN AHR_v2 FLOAT;
    
UPDATE pgcr_dev.plant_metrics 
SET AHR_v2=pgcr_dev.AHR_ver2.AHR_v2
FROM pgcr_dev.AHR_ver2
WHERE pgcr_dev.plant_metrics.year               = pgcr_dev.AHR_ver2.year
AND   pgcr_dev.plant_metrics.state              = pgcr_dev.AHR_ver2.state
AND   pgcr_dev.plant_metrics.facility_id_orispl = pgcr_dev.AHR_ver2.facility_id_orispl
AND   pgcr_dev.plant_metrics.unit_id            = pgcr_dev.AHR_ver2.unit_id;         

-- select top 20 * from pgcr_dev.plant_metrics where ahr_v2 is not null;
-- select top 20 * from pgcr_dev.plant_metrics where ahr_v2 is not null and ahr_v2 > ahr_v1 * 1.01 ;


--ramp up/down
DROP TABLE IF EXISTS pgcr_dev.ramp_updown_calc CASCADE;
SELECT 
    (DATE::varchar(10) + ' ' +hour::varchar(10) + ':00:00')::datetime AS ts
    ,state
   ,year
   ,facility_id_orispl
   ,unit_id
   ,gross_load_mw
   ,lag(gross_load_mw,1) OVER (PARTITION BY state, year, facility_id_orispl, unit_id ORDER BY (DATE::varchar(10) + ' ' +hour::varchar(10) + ':00:00')::datetime) as gross_load_lag1
   ,row_number() over(partition by state, year, facility_id_orispl, unit_id order by date, hour) AS rownum
   ,DATEDIFF(hour,(LAG(date,1) OVER (PARTITION BY state, year, facility_id_orispl, unit_id ORDER BY DATE,hour)::varchar(10) + ' ' + LAG(hour,1) OVER (PARTITION BY state, year, facility_id_orispl, unit_id ORDER BY DATE,hour)::varchar(10) + ':00:00')::datetime,(DATE::varchar(10) + ' ' +hour::varchar(10) + ':00:00')::datetime) AS hour_diff
   ,0::FLOAT ramp_up
INTO pgcr_dev.ramp_updown_calc
FROM pgcr_prod.airmarketsfull_emission;


UPDATE pgcr_dev.ramp_updown_calc SET hour_diff=1 WHERE rownum=1;

UPDATE pgcr_dev.ramp_updown_calc
   SET ramp_up = (gross_load_mw - gross_load_lag1)/60 --as per the req, this has to be per minute. Hence the division by 60
WHERE gross_load_lag1 IS NOT NULL 
    AND gross_load_lag1<>0
    AND gross_load_mw IS NOT NULL
    AND gross_load_mw<>0
    AND hour_diff=1;    

    
DROP TABLE IF EXISTS pgcr_dev.ramp_updown CASCADE;
SELECT 
    state
    ,year
    ,facility_id_orispl
    ,unit_id
    ,case when MAX(ramp_up) > 0 then MAX(ramp_up) else NULL END rampup
    ,case when MIN(ramp_up) < 0 then -1 * MIn(ramp_up) else NULL END rampdown
INTO pgcr_dev.ramp_updown
FROM pgcr_dev.ramp_updown_calc
GROUP BY 
    state
    ,year
    ,facility_id_orispl
    ,unit_id;
        

ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN ramp_up FLOAT;
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN ramp_down FLOAT;
    
UPDATE pgcr_dev.plant_metrics 
SET ramp_up     = pgcr_dev.ramp_updown.rampup,  
    ramp_down   = pgcr_dev.ramp_updown.rampdown
FROM pgcr_dev.ramp_updown
WHERE pgcr_dev.plant_metrics.year               = pgcr_dev.ramp_updown.year
AND   pgcr_dev.plant_metrics.state              = pgcr_dev.ramp_updown.state
AND   pgcr_dev.plant_metrics.facility_id_orispl = pgcr_dev.ramp_updown.facility_id_orispl
AND   pgcr_dev.plant_metrics.unit_id            = pgcr_dev.ramp_updown.unit_id;         

-- select top 20 * from pgcr_dev.plant_metrics where ramp_up is not null and ramp_down is not null;

DROP TABLE IF EXISTS pgcr_dev.capacity_factor_by_year2;
SELECT 
        year
       ,state
       ,facility_id_orispl
       ,unit_id
       ,CASE 
        WHEN year=2017 THEN sum_gross_load_mw/(3*30*24) --max date in the DB is 2017-03-30. Change this after the call 3 *30 * 24
        ELSE sum_gross_load_mw/8760 
        END AS avg_gross_load_mw_ytd
       ,est_nameplatecapacity_mw
       ,est_nameplatecapacity_mw_ver2
       ,0::FLOAT AS capacity_factor
       ,0::FLOAT AS capacity_factor_ver2
INTO pgcr_dev.capacity_factor_by_year2
FROM pgcr_dev.plant_metrics;
--WHERE gross_load_mw >= 0.3 * est_nameplatecapacity_mw  --this is the condition specified by Barclay to filter out the lower loads


UPDATE pgcr_dev.capacity_factor_by_year2  SET capacity_factor=avg_gross_load_mw_ytd/(est_nameplatecapacity_mw) WHERE est_nameplatecapacity_mw<>0;
UPDATE pgcr_dev.capacity_factor_by_year2 SET capacity_factor_ver2=avg_gross_load_mw_ytd/(est_nameplatecapacity_mw_ver2) WHERE est_nameplatecapacity_mw_ver2<>0;


ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN capacity_factor FLOAT;
ALTER TABLE pgcr_dev.plant_metrics ADD COLUMN capacity_factor_ver2 FLOAT;
    
UPDATE pgcr_dev.plant_metrics 
SET capacity_factor         = pgcr_dev.capacity_factor_by_year2.capacity_factor,  
    capacity_factor_ver2    = pgcr_dev.capacity_factor_by_year2.capacity_factor_ver2
FROM pgcr_dev.capacity_factor_by_year2
WHERE pgcr_dev.plant_metrics.year               = pgcr_dev.capacity_factor_by_year2.year
AND   pgcr_dev.plant_metrics.state              = pgcr_dev.capacity_factor_by_year2.state
AND   pgcr_dev.plant_metrics.facility_id_orispl = pgcr_dev.capacity_factor_by_year2.facility_id_orispl
AND   pgcr_dev.plant_metrics.unit_id            = pgcr_dev.capacity_factor_by_year2.unit_id;   

   
-- select * from pgcr_dev.plant_metrics where capacity_factor_ver2>1;
--select * from pgcr_dev.plant_metrics where capacity_factor>1;
--select * from pgcr_dev.plant_metrics where flhr_v1 > ahr_v1;
--select * from pgcr_dev.plant_metrics where flhr_v2 > ahr_v2;
--select * from pgcr_dev.FLHR_ver2 where flhr_v2<>0;

-- select * from pgcr_dev.plant_metrics;
