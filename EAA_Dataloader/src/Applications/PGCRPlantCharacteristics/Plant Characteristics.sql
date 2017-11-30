/*
Heat rate
Find max ramp up time
Find max ramp down time
Find plant/unit startup
*/
--data prep from airmarkets
DROP TABLE IF EXISTS pgcr_dev.plant_characteristics_analysis CASCADE;
SELECT
        (DATE::varchar(10) + ' ' +hour::varchar(10) + ':00:00')::datetime as timestamp,
        state,
        facility_name,
        facility_id_orispl,
        unit_id,
        operating_time,
        0::FLOAT AS minimumload_mw,
        0::FLOAT AS summercapacity_mw,
        0::FLOAT AS wintercapacity_mw,
        0::FLOAT AS nameplatecapacity_mw,
        0::BOOLEAN AS is_greater_than_minimumload,
        0::FLOAT AS thirty_percent_of_nameplatecapacity_mw,    
        row_number() over(partition by state,facility_name, unit_id order by date, hour) AS rownum, 
        DATEDIFF(hour,(LAG(date,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour)::varchar(10) + ' ' + LAG(hour,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour)::varchar(10) + ':00:00')::datetime,(DATE::varchar(10) + ' ' +hour::varchar(10) + ':00:00')::datetime) AS hour_diff,
        0::int AS timeseries_redflag,
        CASE WHEN gross_load_mw IS NULL OR gross_load_mw=0 THEN 0 ELSE heat_input_mmbtu / gross_load_mw   END AS heat_rate,
        CASE WHEN row_number() over(partition by state,facility_name, unit_id order by date, hour) <> 1 AND LAG(gross_load_mw,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour) IS NULL AND gross_load_mw IS NOT NULL THEN 1 ELSE 0 END AS is_plant_start,
        heat_input_mmbtu,
        LAG(heat_input_mmbtu,1) OVER (PARTITION BY facility_name,unit_id ORDER BY DATE,hour) AS heat_input_mmbtu_lag,
        heat_input_mmbtu - LAG(heat_input_mmbtu,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour) AS heat_input_mmbtu_delta,
        CASE
        WHEN LAG(heat_input_mmbtu,1) OVER (PARTITION BY facility_name,unit_id ORDER BY DATE,hour)=0 THEN 0
        ELSE (heat_input_mmbtu - LAG(heat_input_mmbtu,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour))/LAG(heat_input_mmbtu,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour)
        END AS heat_input_mmbtu_change_ratio,
        gross_load_mw,
        LAG(gross_load_mw,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour) AS gross_load_mw_lag,
        gross_load_mw - LAG(gross_load_mw,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour) AS gross_load_mw_delta,
        CASE
        WHEN LAG(gross_load_mw,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour)=0 THEN 0    
        ELSE (gross_load_mw - LAG(gross_load_mw,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour))/LAG(gross_load_mw,1) OVER (PARTITION BY state,facility_name,unit_id ORDER BY DATE,hour)
        END  AS gross_load_mw_change_ratio,
        CASE WHEN operating_time=0 OR operating_time IS NULL THEN 0 ELSE heat_input_mmbtu/operating_time END AS heat_efficiency,
        CASE WHEN operating_time=0 OR operating_time IS NULL THEN 0 ELSE gross_load_mw/operating_time END AS power_efficiency,
        steam_load_5000lb_hr,
        associated_stacks,
        year,
        date,
        hour,
        programs,
        so2_pounds,
        avg_nox_rate_lb_mmbtu,
        nox_pounds,
        co2_short_tons,
        epa_region,
        nerc_region,
        county,
        source_category,
        owner,
        operator,
        representative_primary,
        representative_secondary,
        so2_phase,
        nox_phase,
        operating_status,
        unit_type,
        fuel_type_primary,
        fuel_type_secondary,
        so2_controls,
        nox_controls,
        pm_controls,
        hg_controls,
        facility_latitude,
        facility_longitude
INTO pgcr_dev.plant_characteristics_analysis        
FROM pgcr_prod.airmarketsfull_emission
ORDER BY facility_name,unit_id
         ,DATE
         ,hour;
    
--select * from pgcr_dev.plant_characteristics_analysis where facility_name='Barry' and year=2017 and unit_id=1 and gross_load_mw is not null

/*
if the row is a run begining, set hour_diff=1
This is to account for the start time for a new run of the generator
*/
UPDATE pgcr_dev.plant_characteristics_analysis
SET hour_diff=1
WHERE rownum=1;

/*
Set the timeseries_redflag if there's a gap in airmarkets timeseries
*/
UPDATE pgcr_dev.plant_characteristics_analysis
SET timeseries_redflag=1
WHERE hour_diff <> 1
    AND rownum <> 1;
    

/*
Add the balancingauthorityname from table pgcr_prod.eia860_2_plant
This is the column Barclay refers to as market
*/
DROP VIEW IF EXISTS pgcr_dev.boiler_details;
CREATE VIEW pgcr_dev.boiler_details
AS
SELECT b.utilityid
       ,b.utilityname
       ,b.plantcode
       ,b.plantname
       ,b.boilerid
       ,b.coolingid
       ,b.steamplanttype
       ,p.balancingauthoritycode
       ,p.state
FROM pgcr_prod.eia860_2_plant AS P
  LEFT JOIN pgcr_prod.eia860_enviroassocboilercooling AS b
         ON b.utilityid = p.utilityid
        AND b.utilityname = p.utilityname
        AND b.plantcode = p.plantcode
        AND b.plantname = p.plantname;
        
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN balancingauthoritycode varchar(10) ENCODE LZO;
UPDATE pgcr_dev.plant_characteristics_analysis
   SET balancingauthoritycode = pgcr_dev.boiler_details.balancingauthoritycode
FROM pgcr_dev.boiler_details
WHERE pgcr_dev.plant_characteristics_analysis.state = pgcr_dev.boiler_details.state
AND   pgcr_dev.plant_characteristics_analysis.facility_name = pgcr_dev.boiler_details.plantname
AND   pgcr_dev.plant_characteristics_analysis.unit_id = pgcr_dev.boiler_details.boilerid;

/*
Add the following columns to be shown in the Plant Characteristics Dashboard
1. technology
2. primemover
3. unitcode
4. timefromcoldshutdowntofullload
Fetch the following columns from the 860 view:
    1. minimumload_mw
    2. summercapacity_mw
    3. wintercapacity_mw
    4. nameplatecapacity_mw*/
--TODO CU: cross check if these columns are the right match (join key check)

ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN technology VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN primemover VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN unitcode VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN timefromcoldshutdowntofullload VARCHAR(100);

UPDATE pgcr_dev.plant_characteristics_analysis
SET technology=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.technology,
    primemover=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.primemover,
    unitcode=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.unitcode,
    timefromcoldshutdowntofullload=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.timefromcoldshutdowntofullload,
    minimumload_mw=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.minimumload_mw,
    summercapacity_mw=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.summercapacity_mw,
    wintercapacity_mw=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.wintercapacity_mw,
    nameplatecapacity_mw=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.nameplatecapacity_mw
FROM pgcr_prod.eia860_vw_generator_wind_solar_multifuel
WHERE pgcr_dev.plant_characteristics_analysis.state = pgcr_prod.eia860_vw_generator_wind_solar_multifuel.state
    AND pgcr_dev.plant_characteristics_analysis.facility_id_orispl = pgcr_prod.eia860_vw_generator_wind_solar_multifuel.plantcode
    AND pgcr_dev.plant_characteristics_analysis.unit_id=pgcr_prod.eia860_vw_generator_wind_solar_multifuel.generatorid;

/*
Estimate the nameplatecapacity for generators for which nameplatecapacity_mw is null
Add a column to capture the estimated nameplatecapacity, where the original column nameplatecapacity_mw is null
*/
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN est_nameplatecapacity_mw FLOAT;
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN est_nameplatecapacity_mw2 FLOAT;
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN max_gross_load_mw FLOAT;

DROP VIEW IF EXISTS pgcr_dev.vw_max_gross_load;
CREATE VIEW pgcr_dev.vw_max_gross_load
AS
SELECT state, facility_name, unit_id, MAX(gross_load_mw) AS max_gross_load_mw
FROM pgcr_dev.plant_characteristics_analysis
GROUP BY state, facility_name, unit_id;

UPDATE pgcr_dev.plant_characteristics_analysis
SET max_gross_load_mw=pgcr_dev.vw_max_gross_load.max_gross_load_mw
FROM pgcr_dev.vw_max_gross_load
WHERE pgcr_dev.plant_characteristics_analysis.state=pgcr_dev.vw_max_gross_load.state
    AND trim(pgcr_dev.plant_characteristics_analysis.facility_name)=trim(pgcr_dev.vw_max_gross_load.facility_name)
    AND pgcr_dev.plant_characteristics_analysis.unit_id=pgcr_dev.vw_max_gross_load.unit_id;

UPDATE pgcr_dev.plant_characteristics_analysis SET est_nameplatecapacity_mw=nameplatecapacity_mw;
UPDATE pgcr_dev.plant_characteristics_analysis
SET est_nameplatecapacity_mw = max_gross_load_mw
WHERE (nameplatecapacity_mw=0 OR nameplatecapacity_mw IS NULL);


--UPDATE pgcr_dev.plant_characteristics_analysis SET est_nameplatecapacity_mw2=NULL;
UPDATE pgcr_dev.plant_characteristics_analysis
SET est_nameplatecapacity_mw2=GREATEST(max_gross_load_mw, est_nameplatecapacity_mw);

--Set is_greater_than_minimumload=1/True where gross_load_mw is greater than or equal to minimumload_mw
UPDATE pgcr_dev.plant_characteristics_analysis SET is_greater_than_minimumload = 1 WHERE gross_load_mw >= minimumload_mw;


--Set thirty_percent_of_nameplatecapacity_mw to 30% of the nameplatecapacity_mw
UPDATE pgcr_dev.plant_characteristics_analysis SET thirty_percent_of_nameplatecapacity_mw = est_nameplatecapacity_mw*0.3;
--CU Ran till here
ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN thirty_percent_of_nameplatecapacity_mw2 FLOAT;
UPDATE pgcr_dev.plant_characteristics_analysis SET thirty_percent_of_nameplatecapacity_mw2 = est_nameplatecapacity_mw2*0.3;


------------------------------------------------Data Blip Analysis--------------------------------------------------
DROP VIEW pgcr_dev.vw_plant_characeristics_blip_analysis CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characeristics_blip_analysis AS
SELECT a.*, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS dt_prev,datediff(hr, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt), dt) blip
FROM
(select primemover, state, facility_name, unit_id, TIMESTAMP as dt, gross_load_mw from pgcr_dev.plant_characteristics_analysis
where gross_load_mw>0) a;

DROP VIEW pgcr_dev.vw_plant_characeristics_primemover_blip_histogram;
CREATE VIEW pgcr_dev.vw_plant_characeristics_primemover_blip_histogram AS
select primemover, blip-1 as blip, count(*) as countie from pgcr_dev.vw_plant_characeristics_blip_analysis
group by primemover, blip
having blip>1;

------------------------------------------------Data Blip Treatment--------------------------------------------------
DROP VIEW pgcr_dev.vw_plant_characteristics_lead_lag_load CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characteristics_lead_lag_load AS
select *, lag(gross_load_mw,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_lag1
, lag(gross_load_mw,2) OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_lag2
, lead(gross_load_mw,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_lead1
, lead(gross_load_mw,2) OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_lead2
from pgcr_dev.plant_characteristics_analysis;

DROP TABLE pgcr_dev.plant_characteristics_no_load_blips CASCADE;
CREATE TABLE pgcr_dev.plant_characteristics_no_load_blips AS
SELECT *, LAG(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_mw_noblips_lag
FROM
(SELECT *
      , case when gross_load_mw is null AND gross_load_lag1 is not null AND gross_load_lead1 is not null then (gross_load_lag1+gross_load_lead1)/2
             when gross_load_mw is null AND gross_load_lag1 is not null AND gross_load_lead1 is null AND gross_load_lead2 IS NOT NULL then (gross_load_lag1+gross_load_lead2)/2
             when gross_load_mw is null AND gross_load_lag1 is null AND gross_load_lead1 is NOT null AND gross_load_lag2 IS NOT NULL then (gross_load_lag2+gross_load_lead1)/2
             else gross_load_mw
        end gross_load_mw_noblips--treated for blips upto 2 hours
from pgcr_dev.vw_plant_characteristics_lead_lag_load
);
/*
-----------------Calculation to get Minimum runtime starts here-------------
*/
DROP VIEW IF EXISTS pgcr_dev.vw_get_start_stop_1 CASCADE;
CREATE VIEW pgcr_dev.vw_get_start_stop_1
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,TIMESTAMP as dt
       ,minimumload_mw
       ,thirty_percent_of_nameplatecapacity_mw
       ,thirty_percent_of_nameplatecapacity_mw2
       ,hour_diff
       ,gross_load_mw_noblips as gross_load_mw
       ,LAG(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lag_gross_load_mw
       ,LEAD(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lead_gross_load_mw
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) as unit_time_rank
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt desc) as unit_time_rank_desc
FROM pgcr_dev.plant_characteristics_no_load_blips;

--select * from pgcr_dev.vw_get_start_stop_1 where facility_name='AMEA Sylacauga Plant' and unit_id=1 order by unit_time_rank;

DROP VIEW IF EXISTS pgcr_dev.vw_get_start_stop_2;
CREATE VIEW pgcr_dev.vw_get_start_stop_2
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,dt
       ,minimumload_mw
       ,thirty_percent_of_nameplatecapacity_mw
       ,thirty_percent_of_nameplatecapacity_mw2
       ,hour_diff
       ,gross_load_mw
       ,lag_gross_load_mw
       ,lead_gross_load_mw
       ,unit_time_rank
       ,unit_time_rank_desc
       ,CASE WHEN gross_load_mw >0 AND lag_gross_load_mw IS NULL AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start
       ,CASE WHEN gross_load_mw >0 AND lead_gross_load_mw IS NULL AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop
       ,CASE WHEN gross_load_mw >thirty_percent_of_nameplatecapacity_mw AND (lag_gross_load_mw IS NULL OR lag_gross_load_mw < thirty_percent_of_nameplatecapacity_mw) AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start1
       ,CASE WHEN gross_load_mw >thirty_percent_of_nameplatecapacity_mw AND (lead_gross_load_mw IS NULL OR lead_gross_load_mw < thirty_percent_of_nameplatecapacity_mw) AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop1
       ,CASE WHEN gross_load_mw >thirty_percent_of_nameplatecapacity_mw2 AND (lag_gross_load_mw IS NULL OR lag_gross_load_mw < thirty_percent_of_nameplatecapacity_mw2) AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start2
       ,CASE WHEN gross_load_mw >thirty_percent_of_nameplatecapacity_mw2 AND (lead_gross_load_mw IS NULL OR lead_gross_load_mw < thirty_percent_of_nameplatecapacity_mw2) AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop2
FROM pgcr_dev.vw_get_start_stop_1
ORDER BY state, facility_name, unit_id, dt;

--select * from pgcr_dev.vw_get_start_stop_2 where facility_name='AMEA Sylacauga Plant' and unit_id=1 order by unit_time_rank;


DROP TABLE IF EXISTS pgcr_dev.get_start_stop_2 CASCADE;
SELECT A.state, A.facility_name, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_start_stop_2
FROM pgcr_dev.vw_get_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start=1
    AND B.is_stop=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;

ALTER TABLE pgcr_dev.get_start_stop_2 ADD COLUMN run_delta INT;
UPDATE pgcr_dev.get_start_stop_2 SET run_delta=DATEDIFF(hour,start_dt,stop_dt)+1;
    
DROP TABLE IF EXISTS pgcr_dev.get_start_stop_21 CASCADE;
SELECT A.state, A.facility_name, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_start_stop_21
FROM pgcr_dev.vw_get_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start1=1
    AND B.is_stop1=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;

ALTER TABLE pgcr_dev.get_start_stop_21 ADD COLUMN run_delta1 INT;
UPDATE pgcr_dev.get_start_stop_21 SET run_delta1=DATEDIFF(hour,start_dt,stop_dt)+1;

DROP TABLE IF EXISTS pgcr_dev.get_start_stop_22 CASCADE;
SELECT A.state, A.facility_name, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_start_stop_22
FROM pgcr_dev.vw_get_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start2=1
    AND B.is_stop2=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;

ALTER TABLE pgcr_dev.get_start_stop_22 ADD COLUMN run_delta2 INT;
UPDATE pgcr_dev.get_start_stop_22 SET run_delta2=DATEDIFF(hour,start_dt,stop_dt)+1;


--This table will not contain the units which were constantly running for the entire period of analysis or were constantly shut down.
DROP VIEW IF EXISTS pgcr_dev.vw_facilities_min_max_runtimes CASCADE;
CREATE VIEW pgcr_dev.vw_facilities_min_max_runtimes
AS
select state, facility_name, unit_id, unit_type, start_dt, stop_dt, run_delta as min_runtime
from
(
    SELECT *
           ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY run_delta) AS min_rownum
    FROM pgcr_dev.get_start_stop_2
    --WHERE run_delta > 5    --not good for combustion cycles and turbines??
) AS O
WHERE O.min_rownum=1
ORDER BY state
             ,facility_name
             ,unit_id
             ,start_dt;

DROP VIEW IF EXISTS pgcr_dev.vw_facilities_min_max_runtimes1 CASCADE;
CREATE VIEW pgcr_dev.vw_facilities_min_max_runtimes1
AS
select state, facility_name, unit_id, unit_type, start_dt, stop_dt, run_delta1 as min_runtime1
from
(
    SELECT *
           ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY run_delta1) AS min_rownum
    FROM pgcr_dev.get_start_stop_21
    --WHERE run_delta > 5    --not good for combustion cycles and turbines??
) AS O
WHERE O.min_rownum=1
ORDER BY state
             ,facility_name
             ,unit_id
             ,start_dt;

DROP VIEW IF EXISTS pgcr_dev.vw_facilities_min_max_runtimes2 CASCADE;
CREATE VIEW pgcr_dev.vw_facilities_min_max_runtimes2
AS
select state, facility_name, unit_id, unit_type, start_dt, stop_dt, run_delta2 as min_runtime2
from
(
    SELECT *
           ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY run_delta2) AS min_rownum
    FROM pgcr_dev.get_start_stop_22
    --WHERE run_delta > 5    --not good for combustion cycles and turbines??
) AS O
WHERE O.min_rownum=1
ORDER BY state
             ,facility_name
             ,unit_id
             ,start_dt;

/*
-----------------Calculation to get Minimum runtime ends here-------------
*/


/*
-----------------Calculation to get No. of starts by year begins here-------------
*/

DROP TABLE pgcr_dev.plant_characteristics_no_of_starts;
CREATE TABLE pgcr_dev.plant_characteristics_no_of_starts AS
SELECT state, facility_name, unit_id, unit_type, date_part(year, dt) as year, sum(is_start) as no_of_starts from pgcr_dev.vw_get_start_stop_2
group by state, facility_name, unit_id, unit_type, date_part(year, dt)
order by state, facility_name, unit_id;

--select * from pgcr_dev.plant_characteristics_no_of_starts;

/*
-----------------Calculation to get No. of starts by year ends here-------------
*/

/*
-----------------Calculation to get Minimum off time starts here-------------
*/
DROP VIEW IF EXISTS pgcr_dev.vw_get_offtime_start_stop_1 CASCADE;
CREATE VIEW pgcr_dev.vw_get_offtime_start_stop_1
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,TIMESTAMP as dt
       ,minimumload_mw
       ,hour_diff
       ,gross_load_mw_noblips as gross_load_mw
       ,LAG(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lag_gross_load_mw
       ,LEAD(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lead_gross_load_mw
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) as unit_time_rank
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt desc) as unit_time_rank_desc
FROM pgcr_dev.plant_characteristics_no_load_blips;

DROP VIEW IF EXISTS pgcr_dev.vw_get_offtime_start_stop_2;
CREATE VIEW pgcr_dev.vw_get_offtime_start_stop_2
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,dt
       ,minimumload_mw
       ,hour_diff
       ,gross_load_mw
       ,lag_gross_load_mw
       ,lead_gross_load_mw
       ,unit_time_rank
       ,unit_time_rank_desc
       ,CASE WHEN gross_load_mw is null AND lag_gross_load_mw <>0 AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start
       ,CASE WHEN gross_load_mw is null AND lead_gross_load_mw <>0 AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop
FROM pgcr_dev.vw_get_offtime_start_stop_1
ORDER BY state, facility_name, unit_id, dt;


DROP TABLE IF EXISTS pgcr_dev.get_offtime_start_stop_2 CASCADE;
SELECT A.state, A.facility_name, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_offtime_start_stop_2
FROM pgcr_dev.vw_get_offtime_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_offtime_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start=1
    AND B.is_stop=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;
    

ALTER TABLE pgcr_dev.get_offtime_start_stop_2 ADD COLUMN stop_delta INT;
UPDATE pgcr_dev.get_offtime_start_stop_2 SET stop_delta=DATEDIFF(hour,start_dt,stop_dt)+1;



DROP VIEW IF EXISTS pgcr_dev.vw_facilities_offtime_min_max_runtimes;
CREATE VIEW pgcr_dev.vw_facilities_offtime_min_max_runtimes
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,start_dt
       ,stop_dt
       ,stop_delta as min_off_time
FROM (SELECT *
             ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY stop_delta) AS rownum
      FROM pgcr_dev.get_offtime_start_stop_2
      --WHERE stop_delta > 0
      ) AS O
WHERE O.rownum = 1
ORDER BY state
         ,facility_name
         ,unit_id
         ,stop_delta ASC;
         
/*
-----------------Calculation to get Minimum off time ends here-------------
*/


/*
-----------------Calculation to get Minimum start time starts here-------------
*/
DROP VIEW IF EXISTS pgcr_dev.vw_get_starttime_start_stop_1 CASCADE;
CREATE VIEW pgcr_dev.vw_get_starttime_start_stop_1
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,TIMESTAMP as dt
       ,minimumload_mw
       ,thirty_percent_of_nameplatecapacity_mw
       ,thirty_percent_of_nameplatecapacity_mw2
       ,hour_diff
       ,gross_load_mw_noblips as gross_load_mw
       ,LAG(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lag_gross_load_mw
       ,LEAD(gross_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lead_gross_load_mw
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) as unit_time_rank
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt desc) as unit_time_rank_desc
FROM pgcr_dev.plant_characteristics_no_load_blips;

DROP VIEW IF EXISTS pgcr_dev.vw_get_starttime_start_stop_2;
CREATE VIEW pgcr_dev.vw_get_starttime_start_stop_2
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,dt
       ,minimumload_mw
       ,hour_diff
       ,gross_load_mw
       ,lag_gross_load_mw
       ,lead_gross_load_mw
       ,unit_time_rank
       ,unit_time_rank_desc
       ,CASE WHEN gross_load_mw is null AND lead_gross_load_mw <>0 THEN 1 ELSE 0 END is_start
       ,CASE WHEN lag_gross_load_mw < thirty_percent_of_nameplatecapacity_mw AND gross_load_mw > thirty_percent_of_nameplatecapacity_mw THEN 1 ELSE 0 END is_stop
FROM pgcr_dev.vw_get_starttime_start_stop_1
ORDER BY state, facility_name, unit_id, dt;


DROP TABLE IF EXISTS pgcr_dev.get_starttime_start_stop_2 CASCADE;
SELECT A.state, A.facility_name, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_starttime_start_stop_2
FROM pgcr_dev.vw_get_starttime_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_starttime_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start=1
    AND B.is_stop=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;
    

ALTER TABLE pgcr_dev.get_starttime_start_stop_2 ADD COLUMN start_time INT;
UPDATE pgcr_dev.get_starttime_start_stop_2 SET start_time=DATEDIFF(hour,start_dt,stop_dt);



DROP VIEW IF EXISTS pgcr_dev.vw_facilities_offtime_min_max_runtimes;
CREATE VIEW pgcr_dev.vw_facilities_min_max_starttimes
AS
SELECT state
       ,facility_name
       ,unit_id
       ,unit_type
       ,start_dt
       ,stop_dt
       ,start_time as min_start_time
FROM (SELECT *
             ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY start_time) AS rownum
      FROM pgcr_dev.get_starttime_start_stop_2
      ) AS O
WHERE O.rownum = 1
ORDER BY state
         ,facility_name
         ,unit_id
         ,start_time ASC;

--select * from pgcr_dev.vw_facilities_min_max_starttimes;
         
/*
-----------------Calculation to get Minimum start time ends here-------------
*/


/*
-----------------Calculation of heat rate above 30% gross load starts here---------------------------
(gross_load_mw >= 0.3 * est_nameplatecapacity_mw
slice this by year
*/
DROP TABLE IF EXISTS pgcr_dev.wtavg_heatrate_1 CASCADE;
SELECT state, year
       ,facility_name
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(gross_load_mw) AS gross_load_mw_denom_1
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num_1
       , case when SUM(gross_load_mw) <>0 then SUM(heat_input_mmbtu)/SUM(gross_load_mw)
              else NULL
         end heat_rate_above_30perc1
       ,count(gross_load_mw) as hours_30_1
INTO pgcr_dev.wtavg_heatrate_1
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   gross_load_mw >= est_nameplatecapacity_mw*0.3
GROUP BY state, year
         ,facility_name
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;

DROP TABLE IF EXISTS pgcr_dev.wtavg_heatrate_2 CASCADE;
SELECT state, year
       ,facility_name
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(gross_load_mw) AS gross_load_mw_denom_2
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num_2
       , case when SUM(gross_load_mw) <>0 then SUM(heat_input_mmbtu)/SUM(gross_load_mw)
              else NULL
         end heat_rate_above_30perc2
       ,count(gross_load_mw) as hours_30_2
INTO pgcr_dev.wtavg_heatrate_2
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   gross_load_mw >= est_nameplatecapacity_mw2*0.3
GROUP BY state, year
         ,facility_name
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;

--select * from pgcr_dev.wtavg_heatrate_2;

DROP VIEW IF EXISTS pgcr_dev.vw_wt_avg_heatrate CASCADE;
CREATE VIEW pgcr_dev.vw_wt_avg_heatrate
AS
SELECT a.*,b.gross_load_mw_denom_2,b.heat_input_mmbtu_num_2, b.heat_rate_above_30perc2, b.hours_30_2
FROM pgcr_dev.wtavg_heatrate_1 a
JOIN pgcr_dev.wtavg_heatrate_2 b
ON a.state=b.state
AND a.year=b.year
AND a.unit_id=b.unit_id
AND a.unit_type=b.unit_type
AND a.fuel_type_primary=b.fuel_type_primary
AND a.fuel_type_secondary=b.fuel_type_secondary;

--select * from pgcr_dev.vw_wt_avg_heatrate where heat_rate_above_30perc1-heat_rate_above_30perc2 >1;

/*
-----------------Calculation of heat rate above 30% gross load ends here---------------------------
(gross_load_mw >= 0.3 * est_nameplatecapacity_mw
*/


/*
-----------------Calculation of full load heat rate starts here---------------------------
(gross_load_mw >= 0.3 * est_nameplatecapacity_mw
slice this by year
*/
DROP TABLE IF EXISTS pgcr_dev.full_load_heatrate_1 CASCADE;
SELECT state, year
       ,facility_name
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(gross_load_mw) AS gross_load_mw_denom_1
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num_1
       , case when SUM(gross_load_mw) <>0 then SUM(heat_input_mmbtu)/SUM(gross_load_mw)
              else NULL
         end full_load_heatrate1
       ,count(gross_load_mw) as hours_90_1
INTO pgcr_dev.full_load_heatrate_1
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   gross_load_mw >= est_nameplatecapacity_mw*0.9
GROUP BY state, year
         ,facility_name
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;

DROP TABLE IF EXISTS pgcr_dev.full_load_heatrate_2 CASCADE;
SELECT state, year
       ,facility_name
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(gross_load_mw) AS gross_load_mw_denom_2
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num_2
       , case when SUM(gross_load_mw) <>0 then SUM(heat_input_mmbtu)/SUM(gross_load_mw)
              else NULL
         end full_load_heatrate2
       ,count(gross_load_mw) as hours_90_2
INTO pgcr_dev.full_load_heatrate_2
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   gross_load_mw >= est_nameplatecapacity_mw2*0.9
GROUP BY state, year
         ,facility_name
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;
         
--select * from pgcr_dev.wtavg_heatrate_2;

DROP VIEW IF EXISTS pgcr_dev.full_load_heatrate CASCADE;
CREATE VIEW pgcr_dev.full_load_heatrate
AS
SELECT a.*,b.gross_load_mw_denom_2,b.heat_input_mmbtu_num_2, b.full_load_heatrate2, b.hours_90_2
FROM pgcr_dev.full_load_heatrate_1 a
JOIN pgcr_dev.full_load_heatrate_2 b
ON a.state=b.state
AND a.year=b.year
AND a.unit_id=b.unit_id
AND a.unit_type=b.unit_type
AND a.fuel_type_primary=b.fuel_type_primary
AND a.fuel_type_secondary=b.fuel_type_secondary;

--select * from pgcr_dev.full_load_heatrate where full_load_heatrate1-full_load_heatrate2 >1;

/*
-----------------Calculation of full load heat rate ends here---------------------------
(gross_load_mw >= 0.9 * est_nameplatecapacity_mw
*/


DROP VIEW IF EXISTS pgcr_dev.heat_rate;
CREATE VIEW pgcr_dev.heat_rate
AS
SELECT h.state, h.year
       ,h.facility_name
       ,h.unit_id
       ,h.unit_type
       ,h.fuel_type_primary
       ,h.fuel_type_secondary
       ,h.heat_rate_above_30perc1
       ,h.hours_30_1
       ,h.heat_rate_above_30perc2
       ,h.hours_30_2
       ,f.full_load_heatrate1
       ,f.hours_90_1
       ,f.full_load_heatrate2
       ,f.hours_90_2
FROM pgcr_dev.vw_wt_avg_heatrate  H
LEFT JOIN pgcr_dev.full_load_heatrate F
 ON h.state=f.state
 and h.facility_name       = f.facility_name
    AND h.unit_id             = f.unit_id
and    h.unit_type           = f.unit_type
and    h.year           = f.year
and    h.fuel_type_primary   = f.fuel_type_primary
   and h.fuel_type_secondary = f.fuel_type_secondary;

--select * from pgcr_dev.heat_rate where full_load_heatrate2>heat_rate_above_30perc2;


/*
Calculate the capacity factor
This information will be used for calculating capacity_factor
    Capacity factor by year (EPA, CEMS....maybe by year), % - -  MWh/(nameplate MW * 8760)
        Numerator => sum(annual gross_load)
        If nameplate is not available, take the max MWh in the dataset
*/
DROP TABLE IF EXISTS pgcr_dev.capacity_factor_by_year;
SELECT cast(DATE_PART(year,DATE::DATE) as int) AS year
       ,state
       ,facility_name
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(gross_load_mw)/COUNT(*) AS avg_gross_load_mw
       ,MAX(est_nameplatecapacity_mw) AS est_nameplatecapacity_mw
       ,MAX(est_nameplatecapacity_mw2) AS est_nameplatecapacity_mw2
INTO pgcr_dev.capacity_factor_by_year
FROM pgcr_dev.plant_characteristics_no_load_blips
GROUP BY DATE_PART(year,DATE::DATE)
         ,state
         ,facility_name
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;


ALTER TABLE pgcr_dev.capacity_factor_by_year ADD COLUMN capacity_factor1 FLOAT;
UPDATE pgcr_dev.capacity_factor_by_year 
SET capacity_factor1=avg_gross_load_mw/(est_nameplatecapacity_mw) 
WHERE est_nameplatecapacity_mw<>0;

ALTER TABLE pgcr_dev.capacity_factor_by_year ADD COLUMN capacity_factor2 FLOAT;
UPDATE pgcr_dev.capacity_factor_by_year 
SET capacity_factor2=avg_gross_load_mw/(est_nameplatecapacity_mw2) 
WHERE est_nameplatecapacity_mw2<>0;

--Select * FROM pgcr_dev.plant_characteristics_no_load_blips where facility_name = 'Louisiana 1' and unit_id='1A' and year=2017;

--select * from pgcr_dev.capacity_factor_by_year where facility_name = 'Louisiana 1' and unit_id='1A';

/*
-------------------capacity factor calculation ends here--------------------------
*/

/*
Estimate the ramp_up rate of the generator
*/

--select * from pgcr_Dev.plant_characteristics_no_load_blips;

ALTER TABLE pgcr_dev.plant_characteristics_no_load_blips ADD COLUMN ramp_up FLOAT;
UPDATE pgcr_dev.plant_characteristics_no_load_blips SET ramp_up=0;

UPDATE pgcr_dev.plant_characteristics_no_load_blips
   SET ramp_up = (gross_load_mw_noblips - gross_load_mw_noblips_lag)/60 --as per the req, this has to be per minute. Hence the division by 60
WHERE gross_load_mw_noblips_lag IS NOT NULL 
    AND gross_load_mw_noblips_lag<>0
    AND gross_load_mw_noblips IS NOT NULL
    AND gross_load_mw_noblips<>0
    AND hour_diff=1;

DROP TABLE IF EXISTS pgcr_dev.plant_basic_details CASCADE;
SELECT year
       ,state
       ,facility_name
       ,unit_id
       ,unit_type
       ,nerc_region
       ,fuel_type_primary
       ,fuel_type_secondary
       ,case when MAX(ramp_up) > 0 then MAX(ramp_up)
             else NULL
        END rampup
        ,case when MIN(ramp_up) < 0 then -1 * MIN(ramp_up)
             else NULL
        END rampdown
       ,MIN(est_nameplatecapacity_mw) AS est_nameplatecapacity_mw
       ,MIN(est_nameplatecapacity_mw2) AS est_nameplatecapacity_mw2
       ,MIN(summercapacity_mw) AS summercapacity_mw
       ,MIN(wintercapacity_mw) AS wintercapacity_mw
       ,SUM(so2_pounds) AS so2_pounds
       ,SUM(nox_pounds) AS nox_pounds
       ,AVG(avg_nox_rate_lb_mmbtu) AS avg_nox_rate_lb_mmbtu
       ,SUM(co2_short_tons) AS co2_short_tons
       ,facility_latitude
       ,facility_longitude
       ,MAX(OWNER) AS OWNER
INTO pgcr_dev.plant_basic_details       
FROM pgcr_dev.plant_characteristics_no_load_blips
GROUP BY year,state
         ,facility_name
         ,unit_id
         ,unit_type
         ,nerc_region
         ,fuel_type_primary
         ,fuel_type_secondary
         ,facility_latitude
         ,facility_longitude;

--select * from pgcr_dev.plant_basic_details;

