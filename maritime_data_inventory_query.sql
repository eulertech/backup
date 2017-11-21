-- These queries are for answerning the following questions
-- Q1: How many tankers and total ship in our ship registry
select count(distinct(lrno)) from ra.absd_ship_search where statdecode = 'Crude Oil Tanker'; --3950
select count(distinct(lrno)) from ra.absd_ship_search;  -- 220949
select count(distinct(imo)) from mar_commoditysea.crudeoilvessels;  -- 1943

-- How many tankers/ships in AIS data

select count(distinct(ihslrorimono)) from ra.tblallaisfiles where vesseltype = 'Tanker';  -- 30065
select count(distinct(ihslrorimono)) from ra.tblallaisfiles;  --132065


-- Q2: How many AIS pings per day/month and total


-- Q3: How many international journey defined form AIS ping data
