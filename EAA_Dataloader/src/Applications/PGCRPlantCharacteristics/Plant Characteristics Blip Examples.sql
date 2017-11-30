

select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, blip_length, gross_load_mw, gross_load_mw_noblips
from pgcr_dev.plant_characteristics_no_load_blips where gross_load_mw is null and gross_load_mw_noblips >100
order by facility_id_orispl, unit_id, timestamp;

select * from pgcr_dev.plant_characteristics_no_load_blips where blip_length>3 and gross_load_mw_noblips is not null and gross_load_nonull_lead1>10 and gross_load_nonull_lag1>10;

--Scherer
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, rampup, rampdown, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='Scherer' and unit_id=3 and date_part(year,timestamp)=2017 and date_part(mon,timestamp)=1 and date_part(day,timestamp) in (7)
order by dt;

--Crist Electric Generating Plant
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, rampup, rampdown, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='Crist Electric Generating Plant' and unit_id=5 and date_part(year,timestamp)=2016 and date_part(mon,timestamp)=3 and date_part(day,timestamp) in (8)
order by dt;


--Barry Example
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='Barry' and unit_id=5 and date_part(year,timestamp)=2016 and date_part(mon,timestamp)=7 and date_part(day,timestamp) in (4)
order by dt;

--E C Gaston Example
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, rampup, rampdown, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='E C Gaston' and unit_id=5 and date_part(year,timestamp)=2015 and date_part(mon,timestamp)=3 and date_part(day,timestamp) in (16, 17)
order by dt;

--Oklaunion Power Station Example
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='Oklaunion Power Station' and unit_id=1 and date_part(year,timestamp)=2016 and date_part(mon,timestamp)=5 and date_part(day,timestamp) in (26)
order by dt;

--Powerton
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, rampup, rampdown, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='Powerton' and unit_id=52 and date_part(year,timestamp)=2016 and date_part(mon,timestamp)=12 and date_part(day,timestamp) in (14,15)
order by dt;

--Cherokee
select timestamp as dt, state, facility_name, facility_id_orispl, unit_id, technology, gross_load_mw, gross_load_mw_noblips 
from pgcr_dev.plant_characteristics_no_load_blips 
where facility_name='Cherokee' and unit_id=4 and date_part(year,timestamp)=2015 and date_part(mon,timestamp)=5 and date_part(day,timestamp) in (23,24)
order by dt;

--Count of blips before and after treatment
select * from pgcr_dev.vw_plant_characeristics_blip_count;
select * from pgcr_dev.vw_plant_characeristics_blip_count_treated;


