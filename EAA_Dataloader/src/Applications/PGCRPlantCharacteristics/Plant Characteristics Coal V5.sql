/*
Heat rate
Find max ramp up time
Find max ramp down time
Find plant/unit startup
*/
--data prep from airmarkets

DROP TABLE IF EXISTS pgcr_dev.plant_characteristics_data CASCADE;
SELECT * 
INTO pgcr_dev.plant_characteristics_data
FROM (SELECT *, count(*) over (partition by facility_id_orispl, unit_id, year) as unit_year_rowcount_initial
              , CASE WHEN operating_time=0 OR operating_time IS NULL OR gross_load_mw=0 OR gross_load_mw IS NULL THEN 0 
                     ELSE heat_input_mmbtu/(gross_load_mw * operating_time) 
                END AS heat_efficiency
      FROM pgcr_prod.airmarketsfull_emission
     ) 
WHERE trim(lower(fuel_type_primary))='coal' AND heat_efficiency >=5 AND heat_efficiency <=25;

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

DROP VIEW IF EXISTS pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel;
CREATE VIEW pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel AS
SELECT a.*, b.am_facility_id_orispl, b.am_unit_id, b.flag_manually_mapped_by_aa as flag_manually_mapped, c.net_gen_multiplier as net_gen_multiplier_plant
, avg(c.net_gen_multiplier) over (partition by primemover, energysource1) as net_gen_multiplier_generic
, CASE WHEN unit_family_size >= 1 then c.net_gen_multiplier
       ELSE avg(c.net_gen_multiplier) over (partition by primemover, energysource1)
  END net_gen_multiplier
FROM
(SELECT *, case when count(*) over (partition by plantcode, primemover, energysource1)=1 then 1
                when nameplatecapacity_mw is null or nameplatecapacity_mw = 0 then NULL
                when avg(nameplatecapacity_mw) over (partition by plantcode, primemover, energysource1) / nameplatecapacity_mw between 0.9 and 1.1
                      AND count(*) over (partition by plantcode, primemover, energysource1, operatingyear) = count(*) over (partition by plantcode, primemover, energysource1)
                then count(*) over (partition by plantcode, primemover, energysource1)
           end unit_family_size
  from pgcr_prod.eia860_vw_generator_wind_solar_multifuel
) a
JOIN
pgcr_dev.ihsmarkitdata_coal_mapping_am_860_bkp b
ON trim(lower(a.plantcode))=trim(lower(b.eia860_plantcode))
AND trim(lower(a.generatorid))=trim(lower(b.eia860_generatorid))
LEFT OUTER JOIN
    (SELECT facility_code, prime_mover, fuel_2002
    , CASE WHEN sum(gross_generation) <>0 THEN sum(generation) / sum(gross_generation)
           ELSE NULL
      END net_gen_multiplier
    FROM pgcr_dev.eianetgen_grossandnetgen --This table provides the ratio of netgen to gross gen. It can late be multiplied to gross load to convert it to net load
    GROUP BY facility_code, prime_mover, fuel_2002
    ) c
on a.plantcode=c.facility_code
and a.primemover=c.prime_mover
and a.energysource1=c.fuel_2002;

--select top 100 * from pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel;
--select top 100 * from pgcr_dev.eianetgen_grossandnetgen;
--SELECT * from pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel;


ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN technology VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN primemover VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN unitcode VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN timefromcoldshutdowntofullload VARCHAR(100);
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN net_gen_multiplier float;
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN net_load_mw float;
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN net_load_mw_lag float;
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN flag_manually_mapped int;
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN minimumload_mw int;
ALTER TABLE pgcr_dev.plant_characteristics_data ADD COLUMN nameplatecapacity_mw int;

UPDATE pgcr_dev.plant_characteristics_data
SET technology=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.technology,
    primemover=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.primemover,
    unitcode=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.unitcode,
    timefromcoldshutdowntofullload=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.timefromcoldshutdowntofullload,
    minimumload_mw=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.minimumload_mw,
    nameplatecapacity_mw=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.nameplatecapacity_mw,
    flag_manually_mapped=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.flag_manually_mapped,
    net_gen_multiplier=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.net_gen_multiplier
FROM pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel
WHERE pgcr_dev.plant_characteristics_data.facility_id_orispl = pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.am_facility_id_orispl
    AND pgcr_dev.plant_characteristics_data.unit_id=pgcr_dev.eia860_vw_coal_generator_wind_solar_multifuel.am_unit_id;

DELETE FROM pgcr_dev.plant_characteristics_data
WHERE gross_load_mw <= (.03*nameplatecapacity_mw);


DROP TABLE IF EXISTS pgcr_dev.plant_characteristics_analysis CASCADE;
SELECT
        (DATE::varchar(10) + ' ' +hour::varchar(10) + ':00:00')::datetime as timestamp,
        state,
        facility_name,
        facility_id_orispl,
        unit_id,
        operating_time,
        minimumload_mw,
        nameplatecapacity_mw,
        unit_year_rowcount_initial,
        count(*) over (partition by facility_id_orispl, unit_id, year) as unit_year_rowcount,
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
        steam_load_5000lb_hr,
        associated_stacks,
        year,
        date,
        hour,
        technology,
        primemover,
        unitcode,
        timefromcoldshutdowntofullload,
        net_gen_multiplier,
        net_load_mw,
        net_load_mw_lag,
        flag_manually_mapped,
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
FROM pgcr_dev.plant_characteristics_data
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


UPDATE pgcr_dev.plant_characteristics_analysis SET net_load_mw = gross_load_mw * COALESCE(net_gen_multiplier,1);
UPDATE pgcr_dev.plant_characteristics_analysis SET net_load_mw_lag = gross_load_mw_lag * COALESCE(net_gen_multiplier,1);


--Set is_greater_than_minimumload=1/True where net_load_mw is greater than or equal to minimumload_mw
UPDATE pgcr_dev.plant_characteristics_analysis SET is_greater_than_minimumload = 1 WHERE net_load_mw >= minimumload_mw;


--Set thirty_percent_of_nameplatecapacity_mw to 30% of the nameplatecapacity_mw
UPDATE pgcr_dev.plant_characteristics_analysis SET thirty_percent_of_nameplatecapacity_mw = nameplatecapacity_mw*0.3;

/*
-----------------Estimate the ramp_up rate of the generator------------------------
*/

ALTER TABLE pgcr_dev.plant_characteristics_analysis ADD COLUMN ramp_up FLOAT;
UPDATE pgcr_dev.plant_characteristics_analysis SET ramp_up=0;

UPDATE pgcr_dev.plant_characteristics_analysis
   SET ramp_up = (net_load_mw - net_load_mw_lag)/60 --as per the req, this has to be per minute. Hence the division by 60
WHERE net_load_mw_lag IS NOT NULL 
    AND net_load_mw_lag>1
    AND net_load_mw IS NOT NULL
    AND net_load_mw>1
    AND hour_diff=1;

DROP TABLE IF EXISTS pgcr_dev.plant_basic_details CASCADE;
SELECT year
       ,state
       ,facility_name
       ,facility_id_orispl
       ,MAX(nameplatecapacity_mw) as nameplatecapacity_mw
       ,flag_manually_mapped
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
       ,SUM(so2_pounds) AS so2_pounds
       ,SUM(nox_pounds) AS nox_pounds
       ,AVG(avg_nox_rate_lb_mmbtu) AS avg_nox_rate_lb_mmbtu
       ,SUM(co2_short_tons) AS co2_short_tons
       ,facility_latitude
       ,facility_longitude
       ,MAX(OWNER) AS OWNER
INTO pgcr_dev.plant_basic_details       
FROM pgcr_dev.plant_characteristics_analysis
GROUP BY year,state
         ,facility_name
         ,facility_id_orispl
         ,flag_manually_mapped
         ,unit_id
         ,unit_type
         ,nerc_region
         ,fuel_type_primary
         ,fuel_type_secondary
         ,facility_latitude
         ,facility_longitude;

/*
---------------------End of ramp_up rate of the generator------------------------
*/


------------------------------------------------Data Blip Analysis--------------------------------------------------
DROP VIEW IF EXISTS pgcr_dev.vw_plant_characeristics_blip_analysis CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characeristics_blip_analysis AS
SELECT a.*, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS dt_prev,datediff(hr, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt), dt) blip
FROM
(select primemover, state, facility_name, unit_id, TIMESTAMP as dt, net_load_mw from pgcr_dev.plant_characteristics_analysis
where net_load_mw>0) a;

--Blip Counts
DROP VIEW IF EXISTS pgcr_dev.vw_plant_characeristics_blip_count CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characeristics_blip_count AS
SELECT blip_category, count(*) as countie FROM
(SELECT case when blip=2 then '1 Hour Shutdowns'
             when blip=3 then '2 Hour Shutdowns'
             when blip=4 then '3 Hour Shutdowns'
             when blip=5 then '4 Hour Shutdowns'
             else '>4 Hour shutdowns'
        end blip_category
from pgcr_dev.vw_plant_characeristics_blip_analysis where blip>1)
GROUP BY blip_category;


DROP VIEW IF EXISTS pgcr_dev.vw_plant_characeristics_primemover_blip_histogram;
CREATE VIEW pgcr_dev.vw_plant_characeristics_primemover_blip_histogram AS
select primemover, blip-1 as blip, count(*) as countie from pgcr_dev.vw_plant_characeristics_blip_analysis
group by primemover, blip
having blip>1;

--select * from pgcr_dev.vw_plant_characeristics_primemover_blip_histogram;

------------------------------------------------Data Blip Treatment--------------------------------------------------
--select * from pgcr_dev.plant_characteristics_analysis;

DROP VIEW IF EXISTS pgcr_dev.vw_plant_characteristics_lead_lag_load CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characteristics_lead_lag_load AS
select *
       ,MAX(ramp_up) over (partition by facility_id_orispl, unit_id) as rampup
       ,-1 * (MIN(ramp_up) over (partition by facility_id_orispl, unit_id)) as rampdown
, lag(net_load_mw,1) IGNORE NULLS OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_nonull_lag1
, lead(net_load_mw,1) IGNORE NULLS OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as gross_load_nonull_lead1
from pgcr_dev.plant_characteristics_analysis;

DROP VIEW IF EXISTS pgcr_dev.vw_plant_characeristics_blip_length CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characeristics_blip_length AS
SELECT * FROM
(SELECT a.*, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS dt_prev,datediff(hr, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt), dt) blip
FROM
(select primemover, state, facility_name, unit_id, TIMESTAMP as dt, net_load_mw from pgcr_dev.vw_plant_characteristics_lead_lag_load
where net_load_mw is not null) a
) where blip > 1;

--select * from pgcr_dev.vw_plant_characteristics_lead_lag_load;
--select * from pgcr_dev.vw_plant_characeristics_blip_length;

DROP VIEW IF EXISTS pgcr_dev.vw_plant_characteristics_lead_lag_load_blip;
CREATE VIEW pgcr_dev.vw_plant_characteristics_lead_lag_load_blip AS
SELECT a.*, b.blip as blip_length
FROM pgcr_dev.vw_plant_characteristics_lead_lag_load a
LEFT OUTER JOIN pgcr_dev.vw_plant_characeristics_blip_length b
ON a.state=b.state
AND a.facility_name = b.facility_name
AND a.unit_id = b.unit_id
AND a.timestamp > b.dt_prev
AND a.timestamp < b.dt;

--select * from pgcr_dev.vw_plant_characteristics_lead_lag_load_blip;

DROP TABLE IF EXISTS pgcr_dev.plant_characteristics_no_load_blips CASCADE;
CREATE TABLE pgcr_dev.plant_characteristics_no_load_blips AS
SELECT *, LAG(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY TIMESTAMP) as net_load_mw_noblips_lag
FROM
(SELECT *
      , CASE WHEN (blip_length-1) <  ((gross_load_nonull_lead1/(rampup*60)) + (gross_load_nonull_lag1/(rampdown*60))) THEN (gross_load_nonull_lead1 + gross_load_nonull_lag1)/2
             ELSE net_load_mw
        END net_load_mw_noblips --checking if the blip time was enough for the ramp down and ramp up. 150 is derived from the approximate ramp up rate
from pgcr_dev.vw_plant_characteristics_lead_lag_load_blip
);

------------------------------------------------Data Blip Analysis Post Treatment--------------------------------------------------
DROP VIEW IF EXISTS pgcr_dev.vw_plant_characeristics_blip_analysis_noblips CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characeristics_blip_analysis_noblips AS
SELECT a.*, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS dt_prev,datediff(hr, LAG(dt,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt), dt) blip
FROM
(select primemover, state, facility_name, unit_id, TIMESTAMP as dt, net_load_mw from pgcr_dev.plant_characteristics_no_load_blips
where net_load_mw_noblips>0) a;

--Blip Counts
DROP VIEW IF EXISTS pgcr_dev.vw_plant_characeristics_blip_count_treated CASCADE;
CREATE VIEW pgcr_dev.vw_plant_characeristics_blip_count_treated AS
SELECT blip_category, count(*) as countie FROM
(SELECT case when blip=2 then '1 Hour Shutdowns'
             when blip=3 then '2 Hour Shutdowns'
             when blip=4 then '3 Hour Shutdowns'
             when blip=5 then '4 Hour Shutdowns'
             else '>4 Hour shutdowns'
        end blip_category
  from pgcr_dev.vw_plant_characeristics_blip_analysis_noblips where blip>1)
GROUP BY blip_category;
---------------------------------------------------------------------------------------------------------------------

/*
-----------------Calculation to get Minimum runtime starts here-------------
*/
DROP VIEW IF EXISTS pgcr_dev.vw_get_start_stop_1 CASCADE;
CREATE VIEW pgcr_dev.vw_get_start_stop_1
AS
SELECT state
       ,facility_name
       ,facility_id_orispl
       ,flag_manually_mapped
       ,unit_id
       ,unit_type
       ,TIMESTAMP as dt
       ,minimumload_mw
       ,thirty_percent_of_nameplatecapacity_mw
--       ,thirty_percent_of_nameplatecapacity_mw2
       ,hour_diff
       ,net_load_mw_noblips as net_load_mw
       ,LAG(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lag_net_load_mw
       ,LEAD(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lead_net_load_mw
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) as unit_time_rank
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt desc) as unit_time_rank_desc
FROM pgcr_dev.plant_characteristics_no_load_blips;

--select * from pgcr_dev.vw_get_start_stop_1 where facility_name='AMEA Sylacauga Plant' and unit_id=1 order by unit_time_rank;

DROP VIEW IF EXISTS pgcr_dev.vw_get_start_stop_2;
CREATE VIEW pgcr_dev.vw_get_start_stop_2
AS
SELECT state
       ,facility_name
       ,facility_id_orispl
       ,flag_manually_mapped
       ,unit_id
       ,unit_type
       ,dt
       ,minimumload_mw
       ,thirty_percent_of_nameplatecapacity_mw
       ,hour_diff
       ,net_load_mw
       ,lag_net_load_mw
       ,lead_net_load_mw
       ,unit_time_rank
       ,unit_time_rank_desc
       ,CASE WHEN net_load_mw >0 AND lag_net_load_mw IS NULL AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start
       ,CASE WHEN net_load_mw >0 AND lead_net_load_mw IS NULL AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop
       ,CASE WHEN net_load_mw >thirty_percent_of_nameplatecapacity_mw AND (lag_net_load_mw IS NULL OR lag_net_load_mw < thirty_percent_of_nameplatecapacity_mw) AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start1
       ,CASE WHEN net_load_mw >thirty_percent_of_nameplatecapacity_mw AND (lead_net_load_mw IS NULL OR lead_net_load_mw < thirty_percent_of_nameplatecapacity_mw) AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop1
FROM pgcr_dev.vw_get_start_stop_1
ORDER BY state, facility_name, unit_id, dt;

--select * from pgcr_dev.vw_get_start_stop_2 where facility_name='AMEA Sylacauga Plant' and unit_id=1 order by unit_time_rank;

DROP TABLE IF EXISTS pgcr_dev.get_start_stop_2 CASCADE;
SELECT A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_start_stop_2
FROM pgcr_dev.vw_get_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start=1
    AND B.is_stop=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;

ALTER TABLE pgcr_dev.get_start_stop_2 ADD COLUMN run_delta INT;
UPDATE pgcr_dev.get_start_stop_2 SET run_delta=DATEDIFF(hour,start_dt,stop_dt)+1;
    
DROP TABLE IF EXISTS pgcr_dev.get_start_stop_21 CASCADE;
SELECT A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_start_stop_21
FROM pgcr_dev.vw_get_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start1=1
    AND B.is_stop1=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;

ALTER TABLE pgcr_dev.get_start_stop_21 ADD COLUMN run_delta1 INT;
UPDATE pgcr_dev.get_start_stop_21 SET run_delta1=DATEDIFF(hour,start_dt,stop_dt)+1;


--This table will not contain the units which were constantly running for the entire period of analysis or were constantly shut down.
DROP VIEW IF EXISTS pgcr_dev.vw_facilities_min_max_runtimes CASCADE;
CREATE VIEW pgcr_dev.vw_facilities_min_max_runtimes
AS
select state, facility_name, facility_id_orispl, flag_manually_mapped, unit_id, unit_type, start_dt, stop_dt, run_delta as min_runtime
from
(
    SELECT *
           ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY run_delta) AS min_rownum
    FROM pgcr_dev.get_start_stop_2
) AS O
WHERE O.min_rownum=1
ORDER BY state
             ,facility_name
             ,unit_id
             ,start_dt;

DROP VIEW IF EXISTS pgcr_dev.vw_facilities_min_max_runtimes1 CASCADE;
CREATE VIEW pgcr_dev.vw_facilities_min_max_runtimes1
AS
select state, facility_name, facility_id_orispl, flag_manually_mapped, unit_id, unit_type, start_dt, stop_dt, run_delta1 as min_runtime1
from
(
    SELECT *
           ,ROW_NUMBER() OVER (PARTITION BY state,facility_name,unit_id ORDER BY run_delta1) AS min_rownum
    FROM pgcr_dev.get_start_stop_21
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

DROP TABLE IF EXISTS pgcr_dev.plant_characteristics_no_of_starts;
CREATE TABLE pgcr_dev.plant_characteristics_no_of_starts AS
SELECT state, facility_name, facility_id_orispl, flag_manually_mapped, unit_id, unit_type, date_part(year, dt) as year, sum(is_start) as no_of_starts from pgcr_dev.vw_get_start_stop_2
group by state, facility_name, facility_id_orispl, flag_manually_mapped, unit_id, unit_type, date_part(year, dt)
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
       , facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,TIMESTAMP as dt
       ,minimumload_mw
       ,hour_diff
       ,net_load_mw_noblips as net_load_mw
       ,LAG(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lag_net_load_mw
       ,LEAD(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lead_net_load_mw
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) as unit_time_rank
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt desc) as unit_time_rank_desc
FROM pgcr_dev.plant_characteristics_no_load_blips;

DROP VIEW IF EXISTS pgcr_dev.vw_get_offtime_start_stop_2;
CREATE VIEW pgcr_dev.vw_get_offtime_start_stop_2
AS
SELECT state
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,dt
       ,minimumload_mw
       ,hour_diff
       ,net_load_mw
       ,lag_net_load_mw
       ,lead_net_load_mw
       ,unit_time_rank
       ,unit_time_rank_desc
       ,CASE WHEN net_load_mw is null AND lag_net_load_mw <>0 AND unit_time_rank > 1 THEN 1 ELSE 0 END is_start
       ,CASE WHEN net_load_mw is null AND lead_net_load_mw <>0 AND unit_time_rank_desc > 1 THEN 1 ELSE 0 END is_stop
FROM pgcr_dev.vw_get_offtime_start_stop_1
ORDER BY state, facility_name, unit_id, dt;


DROP TABLE IF EXISTS pgcr_dev.get_offtime_start_stop_2 CASCADE;
SELECT A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_offtime_start_stop_2
FROM pgcr_dev.vw_get_offtime_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_offtime_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start=1
    AND B.is_stop=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;
    

ALTER TABLE pgcr_dev.get_offtime_start_stop_2 ADD COLUMN stop_delta INT;
UPDATE pgcr_dev.get_offtime_start_stop_2 SET stop_delta=DATEDIFF(hour,start_dt,stop_dt)+1;



DROP VIEW IF EXISTS pgcr_dev.vw_facilities_offtime_min_max_runtimes;
CREATE VIEW pgcr_dev.vw_facilities_offtime_min_max_runtimes
AS
SELECT state
       ,facility_name, facility_id_orispl, flag_manually_mapped
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
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,TIMESTAMP as dt
       ,minimumload_mw
       ,thirty_percent_of_nameplatecapacity_mw
--       ,thirty_percent_of_nameplatecapacity_mw2
       ,hour_diff
       ,net_load_mw_noblips as net_load_mw
       ,LAG(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lag_net_load_mw
       ,LEAD(net_load_mw_noblips,1) OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) AS lead_net_load_mw
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt) as unit_time_rank
       ,RANK() OVER (PARTITION BY state, facility_name, unit_id ORDER BY dt desc) as unit_time_rank_desc
FROM pgcr_dev.plant_characteristics_no_load_blips;

DROP VIEW IF EXISTS pgcr_dev.vw_get_starttime_start_stop_2;
CREATE VIEW pgcr_dev.vw_get_starttime_start_stop_2
AS
SELECT state
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,dt
       ,minimumload_mw
       ,hour_diff
       ,net_load_mw
       ,lag_net_load_mw
       ,lead_net_load_mw
       ,unit_time_rank
       ,unit_time_rank_desc
       ,CASE WHEN net_load_mw is null AND lead_net_load_mw <>0 THEN 1 ELSE 0 END is_start
       ,CASE WHEN lag_net_load_mw < thirty_percent_of_nameplatecapacity_mw AND net_load_mw > thirty_percent_of_nameplatecapacity_mw THEN 1 ELSE 0 END is_stop
FROM pgcr_dev.vw_get_starttime_start_stop_1
ORDER BY state, facility_name, unit_id, dt;


DROP TABLE IF EXISTS pgcr_dev.get_starttime_start_stop_2 CASCADE;
SELECT A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt AS start_dt, MIN(B.dt) AS stop_dt
into pgcr_dev.get_starttime_start_stop_2
FROM pgcr_dev.vw_get_starttime_start_stop_2 A
FULL OUTER JOIN pgcr_dev.vw_get_starttime_start_stop_2 B
    ON A.state=B.state
    AND A.facility_name=B.facility_name
    AND A.unit_id=B.unit_id
WHERE A.is_start=1
    AND B.is_stop=1
    AND B.dt > A.dt
GROUP BY A.state, A.facility_name, A.facility_id_orispl, A.flag_manually_mapped, A.unit_id, A.unit_type, A.dt
ORDER BY start_dt;
    

ALTER TABLE pgcr_dev.get_starttime_start_stop_2 ADD COLUMN start_time INT;
UPDATE pgcr_dev.get_starttime_start_stop_2 SET start_time=DATEDIFF(hour,start_dt,stop_dt);



DROP VIEW IF EXISTS pgcr_dev.vw_facilities_min_max_starttimes;
CREATE VIEW pgcr_dev.vw_facilities_min_max_starttimes
AS
SELECT state
       ,facility_name, facility_id_orispl, flag_manually_mapped
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

--select * from pgcr_dev.vw_facilities_min_max_starttimes1;

/*
-----------------Calculation to get Minimum start time ends here-------------
*/

/*
-----------------Calculation of heat rate above 30% gross load starts here---------------------------
(net_load_mw >= 0.3 * est_nameplatecapacity_mw
slice this by year
*/

DROP TABLE IF EXISTS pgcr_dev.wtavg_heatrate_1 CASCADE;
SELECT state, year
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,MAX(net_gen_multiplier) as net_gen_multiplier
       ,SUM(net_load_mw * operating_time) AS net_load_mw_denom_30
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num_30
       , case when SUM(net_load_mw * operating_time) <>0 then SUM(heat_input_mmbtu)/SUM(net_load_mw * operating_time)
              else NULL
         end heat_rate_above_30perc
       ,count(net_load_mw) as hours_30
INTO pgcr_dev.wtavg_heatrate_1
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   net_load_mw >= nameplatecapacity_mw*0.3
GROUP BY state, year
         ,facility_name, facility_id_orispl, flag_manually_mapped
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;

--select * from pgcr_dev.plant_characteristics_no_load_blips where net_load_mw >= est_nameplatecapacity_mw*0.3
--select * from pgcr_dev.wtavg_heatrate_1;

DROP TABLE IF EXISTS pgcr_dev.wtavg_heatrate_2 CASCADE;
SELECT state, year
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,nameplatecapacity_mw
       ,MAX(net_gen_multiplier) as net_gen_multiplier
       ,SUM(net_load_mw * operating_time) AS net_load_mw_denom_0
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num_0
       , case when SUM(net_load_mw * operating_time) <>0 then SUM(heat_input_mmbtu)/SUM(net_load_mw * operating_time)
              else NULL
         end heat_rate_above_0
       ,count(net_load_mw) as hours_0
INTO pgcr_dev.wtavg_heatrate_2
FROM pgcr_dev.plant_characteristics_no_load_blips
GROUP BY state, year
         ,facility_name, facility_id_orispl, flag_manually_mapped
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary
         ,nameplatecapacity_mw;

--select * from pgcr_dev.wtavg_heatrate_2;

DROP VIEW IF EXISTS pgcr_dev.vw_wt_avg_heatrate CASCADE;
CREATE VIEW pgcr_dev.vw_wt_avg_heatrate
AS
SELECT b.*, a.net_load_mw_denom_30,a.heat_input_mmbtu_num_30, a.heat_rate_above_30perc, a.hours_30
FROM pgcr_dev.wtavg_heatrate_1 a
RIGHT OUTER JOIN pgcr_dev.wtavg_heatrate_2 b
ON a.facility_name=b.facility_name
AND a.year=b.year
AND a.unit_id=b.unit_id;

--select * from pgcr_dev.vw_wt_avg_heatrate where heat_rate_above_30perc1-heat_rate_above_30perc2 >1;

/*
-----------------Calculation of heat rate above 30% gross load ends here---------------------------
(net_load_mw >= 0.3 * est_nameplatecapacity_mw
*/


/*
-----------------Calculation of full load heat rate starts here---------------------------
(net_load_mw >= 0.3 * est_nameplatecapacity_mw
slice this by year
*/
DROP TABLE IF EXISTS pgcr_dev.full_load_heatrate80 CASCADE;
SELECT state, year
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(net_load_mw * operating_time) AS net_load_mw_denom
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num
       , case when SUM(net_load_mw * operating_time) <>0 then SUM(heat_input_mmbtu)/SUM(net_load_mw * operating_time)
              else NULL
         end full_load_heatrate80
       ,count(net_load_mw) as hours_80
INTO pgcr_dev.full_load_heatrate80
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   net_load_mw >= nameplatecapacity_mw*0.8
GROUP BY state, year
         ,facility_name, facility_id_orispl, flag_manually_mapped
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;

--select * from pgcr_dev.full_load_heatrate_1;

DROP TABLE IF EXISTS pgcr_dev.full_load_heatrate85 CASCADE;
SELECT state, year
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,SUM(net_load_mw * operating_time) AS net_load_mw_denom
       ,SUM(heat_input_mmbtu) AS heat_input_mmbtu_num
       , case when SUM(net_load_mw * operating_time) <>0 then SUM(heat_input_mmbtu)/SUM(net_load_mw * operating_time)
              else NULL
         end full_load_heatrate85
       ,count(net_load_mw) as hours_85
INTO pgcr_dev.full_load_heatrate85
FROM pgcr_dev.plant_characteristics_no_load_blips
WHERE   net_load_mw >= nameplatecapacity_mw*0.85
GROUP BY state, year
         ,facility_name, facility_id_orispl, flag_manually_mapped
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;
         
--select * from pgcr_dev.wtavg_heatrate_2;

DROP VIEW IF EXISTS pgcr_dev.full_load_heatrate CASCADE;
CREATE VIEW pgcr_dev.full_load_heatrate
AS
SELECT a.*, b.full_load_heatrate85, b.hours_85
FROM pgcr_dev.full_load_heatrate80 a
LEFT OUTER JOIN pgcr_dev.full_load_heatrate85 b
ON a.facility_name=b.facility_name
AND a.year=b.year
AND a.unit_id=b.unit_id;

--select * from pgcr_dev.full_load_heatrate where full_load_heatrate1-full_load_heatrate2 >1;

/*
-----------------Calculation of full load heat rate ends here---------------------------
(net_load_mw >= 0.9 * est_nameplatecapacity_mw
*/


DROP VIEW IF EXISTS pgcr_dev.heat_rate;
CREATE VIEW pgcr_dev.heat_rate
AS
SELECT h.state, h.year
       ,h.facility_name, h.facility_id_orispl, h.flag_manually_mapped
       ,h.unit_id
       ,h.unit_type
       ,h.fuel_type_primary
       ,h.fuel_type_secondary
       ,h.net_gen_multiplier
       ,h.nameplatecapacity_mw
       ,h.heat_rate_above_30perc
       ,h.hours_30
       ,h.heat_rate_above_0
       ,h.hours_0
       ,f.full_load_heatrate80
       ,f.hours_80
       ,f.full_load_heatrate85
       ,f.hours_85
FROM pgcr_dev.vw_wt_avg_heatrate  H
LEFT OUTER JOIN pgcr_dev.full_load_heatrate F
 ON h.facility_name       = f.facility_name
    AND h.unit_id             = f.unit_id
and    h.year           = f.year;

--select * from pgcr_dev.heat_rate where full_load_heatrate85 > heat_rate_above_0;


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
       ,facility_name, facility_id_orispl, flag_manually_mapped
       ,unit_id
       ,unit_type
       ,fuel_type_primary
       ,fuel_type_secondary
       ,unit_year_rowcount_initial - unit_year_rowcount as hours_omitted
       ,MAX(nameplatecapacity_mw) AS nameplatecapacity_mw
       ,SUM(net_load_mw * operating_time) AS net_load_mw
       ,SUM(gross_load_mw * operating_time) AS gross_load_mw
       ,case when DATE_PART(year,DATE::DATE)<2017 then 8760 else max(latest_month) * 8760 / 12 end hours
INTO pgcr_dev.capacity_factor_by_year
FROM (SELECT *, DATE_PART(mon, cast(max(DATE) over (partition by NULL) as DATE)) as latest_month FROM pgcr_dev.plant_characteristics_no_load_blips)
GROUP BY DATE_PART(year,DATE::DATE)
         ,state
         ,facility_name, facility_id_orispl, flag_manually_mapped, unit_year_rowcount_initial, unit_year_rowcount
         ,unit_id
         ,unit_type
         ,fuel_type_primary
         ,fuel_type_secondary;

ALTER TABLE pgcr_dev.capacity_factor_by_year ADD COLUMN capacity_factor FLOAT;
UPDATE pgcr_dev.capacity_factor_by_year 
SET capacity_factor = net_load_mw / (nameplatecapacity_mw * (hours - hours_omitted))
WHERE nameplatecapacity_mw<>0;

ALTER TABLE pgcr_dev.capacity_factor_by_year ADD COLUMN gross_capacity_factor FLOAT;
UPDATE pgcr_dev.capacity_factor_by_year 
SET gross_capacity_factor = gross_load_mw / (nameplatecapacity_mw * (hours - hours_omitted))
WHERE nameplatecapacity_mw<>0;



--Select * FROM pgcr_dev.plant_characteristics_no_load_blips where facility_id_orispl = 1082 and unit_id='4' and year=2017;

--select * from pgcr_dev.capacity_factor_by_year where facility_id_orispl = 1082 and unit_id='4' and year=2017;

/*
-------------------capacity factor calculation ends here--------------------------
*/



DROP TABLE IF EXISTS pgcr_dev.heat_rate_cap_factor;
select COALESCE(h.state,c.state) as state, COALESCE(h.year,c.year) as year
       ,COALESCE(h.facility_name,c.facility_name) as facility_name, COALESCE(h.facility_id_orispl,c.facility_id_orispl) as facility_id_orispl
       , COALESCE(h.flag_manually_mapped,c.flag_manually_mapped) as flag_manually_mapped, COALESCE(h.unit_id, c.unit_id) as unit_id
       , COALESCE(h.unit_type, c.unit_type) as unit_type, COALESCE(h.fuel_type_primary, c.fuel_type_primary) as fuel_type_primary
       , COALESCE(h.fuel_type_secondary, c.fuel_type_secondary) as fuel_type_secondary, COALESCE(h.nameplatecapacity_mw, c.nameplatecapacity_mw) as nameplatecapacity_mw
       ,h.net_gen_multiplier
       ,h.heat_rate_above_30perc
       ,h.hours_30
       ,h.heat_rate_above_0
       ,h.hours_0
       ,h.full_load_heatrate80
       ,h.hours_80
       ,h.full_load_heatrate85
       ,h.hours_85
       ,c.capacity_factor
       ,c.gross_capacity_factor
INTO pgcr_dev.heat_rate_cap_factor
FROM pgcr_dev.heat_rate h
FULL OUTER JOIN
pgcr_dev.capacity_factor_by_year c
ON h.year=c.year 
AND h.facility_id_orispl=c.facility_id_orispl 
AND h.unit_id=c.unit_id;


