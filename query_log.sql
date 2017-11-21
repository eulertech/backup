select query, trim(querytxt) as sqlquery, starttime,endtime
from stl_query
order by query desc limit 5;

select query, starttime, endtime,
trim(querytxt) as sqlquery
from stl_query
where starttime >= '2017-06-01 00:00' and endtime < '2017-06-01 23:59'
order by starttime desc;

-- show all the active queries
select user_name,status, db_name, pid, query
from stv_recents;
where status = 'Running';


select distinct(country) from eaa_prod.iea_supply where country like '%OPEC%' order by country;

select distinct(asset_name) from eaa_stage.vantage_asset_summary;

select * from eaa_prod.rigpoint_utilization_monthly where country = 'USA' and rig_type='Drillship' order by month;
select distinct(category) from eaa_prod.rigcount order by valuationdate;

select valuationdate,avg(wells) from eaa_prod.rigcount where category = 'Total Producing Wells' group by valuationdate 
order by valuationdate;

select distinct(geography2) from eaa_prod.eia_pet_imports_series_attributes;

select * from eaa_prod.eaa_attributes where sourcecode = 'POILdaWOR.M';

select (count(distinct(well_type_id))*100.0/(select count(*) as totalC from eaa_stage.vantage_well_summary)) from eaa_stage.vantage_well_summary;
select count(distinct(well_type_id))*100.0 / sum(count(*)) over() from eaa_stage.vantage_well_summary;
select distinct(country) from eaa_stage.vantage_well_summary;

select well_id, year, country, production_oil_volume, remaining_reserves from eaa_stage.vantage_well_annual where price_scenario_description = 'Medium';
select distinct(asset_name)as name from eaa_prod.vantage_well_annual order by name;

-- get total supply, demand
select well_id, asset_name, year, remaining_reserves*100 as percentage_remaining_reserve 
from eaa_prod.vantage_well_annual
where asset_name = 'Raniganj South (CBM)' and price_scenario_description = 'Medium' order by year;

select * from ra.absd_ship_search_01oct15 order by vesselname limit 10;
select * from ra.absd_ship_search_01oct15 where port = '21698' order by vesselname;

select distinct(vesseltype) from ra. absd_ship_search_01oct15 where vesseltype like '%TANKER%';
select lrno, count(*) as n from ra.tblcombmovementcalls where arrdate > '2014-01-01' group by lrno  order by n DESC;
select distinct(portname) from ra.tblcombmovementcalls where arrdate > '2014-01-01' and portname like '%Singapore%' order by portname;
select distinct(movetype) from ra.tblcombmovementcalls where arrdate > '2014-01-01';


select distinct portname
  from ra.tblcombmovementcalls
 where portname ilike '%jurong%' ;
 
 

select count(distinct(lrno)) from ra.absd_ship_search where statdecode = 'Crude Oil Tanker';
select count(distinct(lrno)) from ra.absd_ship_search;

select distinct(vesseltype)from ra.tblallaisfiles;
select count(distinct(imo)) from mar_commoditysea.crudeoilvessels;
select count(distinct(lrno)) from ra.tblallaisfiles where vesseltype = 'Tanker';
select top 1 * from  ra.tblallaisfiles where vesseltype = 'Tanker';
select count(distinct(ihslrorimono)) from ra.tblallaisfiles;

select * from ra.absd_ship_search where lrno = '9172844';


