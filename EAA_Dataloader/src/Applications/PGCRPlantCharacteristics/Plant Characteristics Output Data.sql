--The code below provides the plant characteristic metrics and their pivots

--Heat Rate
SELECT *
FROM pgcr_dev.heat_rate;

SELECT 'heat_rate_above_0' as Operating_load, unit_type,MIN(heat_rate_above_0), AVG(heat_rate_above_0), MAX(heat_rate_above_0) FROM pgcr_dev.heat_rate
GROUP BY unit_type
UNION
SELECT 'heat_rate_above_30perc' as Operating_load, unit_type,MIN(heat_rate_above_30perc), AVG(heat_rate_above_30perc), MAX(heat_rate_above_30perc) FROM pgcr_dev.heat_rate
GROUP BY unit_type
UNION
SELECT 'full_load_heatrate80' as Operating_load, unit_type,MIN(full_load_heatrate80), AVG(full_load_heatrate80), MAX(full_load_heatrate80) FROM pgcr_dev.heat_rate
GROUP BY unit_type
UNION
SELECT 'full_load_heatrate85' as Operating_load, unit_type,MIN(full_load_heatrate85), AVG(full_load_heatrate85), MAX(full_load_heatrate85) FROM pgcr_dev.heat_rate
GROUP BY unit_type;

SELECT unit_type, count(*) as cnt,
       AVG(heat_rate_above_0) AS hr_0,
       AVG(heat_rate_above_30perc) AS hr_30,
       AVG(full_load_heatrate80) AS hr_80,
       AVG(full_load_heatrate85) AS hr_85
FROM pgcr_dev.heat_rate
GROUP BY unit_type;

SELECT unit_type, case when heat_rate_above_0 < full_load_heatrate80 then '0<80'
                       when heat_rate_above_0 >= full_load_heatrate80 then '0>=80'
                   end comp0_80,
                   case when heat_rate_above_0 < full_load_heatrate85 then '0<85'
                        when heat_rate_above_0 >= full_load_heatrate85 then '0>=85'
                   end comp0_85,
                   case when heat_rate_above_30perc < full_load_heatrate80 then '30<80'
                        when heat_rate_above_30perc >= full_load_heatrate80 then '30>=80'
                   end comp30_80,
                   case when heat_rate_above_30perc < full_load_heatrate85 then '30<85'
                       when heat_rate_above_30perc >= full_load_heatrate85 then '30>=85'
                   end comp30_85
FROM pgcr_dev.heat_rate;


SELECT unit_type, state, year, case when heat_rate_above_0 < full_load_heatrate80 then '0<80'
                       when heat_rate_above_0 >= full_load_heatrate80 then '0>=80'
                   end comp0_80
FROM pgcr_dev.heat_rate;

--Capacity Factor
SELECT *
FROM pgcr_dev.capacity_factor_by_year where lower(facility_name) like '%lake road%';

SELECT unit_type,
       AVG(capacity_factor) AS avg_capacity_factor,
       MAX(capacity_factor) AS max_capacity_factor
FROM pgcr_dev.capacity_factor_by_year
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(capacity_factor) AS avg_capacity_factor,
       MAX(capacity_factor) AS max_capacity_factor
FROM pgcr_dev.capacity_factor_by_year
WHERE state IS NOT NULL
GROUP BY state;

SELECT *
FROM pgcr_dev.capacity_factor_by_year
WHERE capacity_factor > 1;

--Ramp up/down Rates and static metrics
SELECT *
FROM pgcr_dev.plant_basic_details;

SELECT unit_type,
       AVG(rampup) AS avg_rampup,
       AVG(rampdown) AS avg_rampdown
FROM pgcr_dev.plant_basic_details
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(rampup) AS avg_rampup,
       AVG(rampdown) AS avg_rampdown
FROM pgcr_dev.plant_basic_details
WHERE state IS NOT NULL
GROUP BY state;

SELECT year,
       AVG(rampup) AS avg_rampup,
       AVG(rampdown) AS avg_rampdown
FROM pgcr_dev.plant_basic_details
WHERE year IS NOT NULL
GROUP BY year;

--Min Start Time
SELECT *
FROM pgcr_dev.vw_facilities_min_max_starttimes;

select * from pgcr_dev.heat_rate where unit_id='172B' and facility_name='Tolk Station';

SELECT unit_type,
       AVG(min_start_time) AS avg_min_start_time
FROM pgcr_dev.vw_facilities_min_max_starttimes
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(min_start_time) AS avg_min_start_time
FROM pgcr_dev.vw_facilities_min_max_starttimes
WHERE state IS NOT NULL
GROUP BY state;

--Min Off time
SELECT *
FROM pgcr_dev.vw_facilities_offtime_min_max_runtimes;

SELECT unit_type,
       AVG(min_off_time) AS avg_min_off_time
FROM pgcr_dev.vw_facilities_offtime_min_max_runtimes
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(min_off_time) AS avg_min_off_time
FROM pgcr_dev.vw_facilities_offtime_min_max_runtimes
WHERE state IS NOT NULL
GROUP BY state;

--No of Starts
SELECT *
FROM pgcr_dev.plant_characteristics_no_of_starts;

SELECT unit_type,
       AVG(no_of_starts) AS avg_no_of_starts
FROM pgcr_dev.plant_characteristics_no_of_starts
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(no_of_starts) AS avg_no_of_starts
FROM pgcr_dev.plant_characteristics_no_of_starts
WHERE state IS NOT NULL
GROUP BY state;

SELECT year,
       AVG(no_of_starts) AS avg_no_of_starts
FROM pgcr_dev.plant_characteristics_no_of_starts
WHERE year IS NOT NULL
GROUP BY year;

--Min Runtime-----------------------------------------------------
--above zero
SELECT *
FROM pgcr_dev.vw_facilities_min_max_runtimes;

SELECT unit_type,
       AVG(min_runtime) AS avg_min_runtime
FROM pgcr_dev.vw_facilities_min_max_runtimes
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(min_runtime) AS avg_min_runtime
FROM pgcr_dev.vw_facilities_min_max_runtimes
WHERE state IS NOT NULL
GROUP BY state;

--above 30% of capacity
SELECT *
FROM pgcr_dev.vw_facilities_min_max_runtimes1;

SELECT unit_type,
       AVG(min_runtime1) AS avg_min_runtime1
FROM pgcr_dev.vw_facilities_min_max_runtimes1
WHERE unit_type IS NOT NULL
GROUP BY unit_type;

SELECT state,
       AVG(min_runtime1) AS avg_min_runtime1
FROM pgcr_dev.vw_facilities_min_max_runtimes1
WHERE state IS NOT NULL
GROUP BY state;

--Mapping of facility name and facility_id_orispl
SELECT DISTINCT state,
       facility_name,
       facility_id_orispl
FROM pgcr_dev.plant_characteristics_analysis;

--heat rate and capacity factor correlation analysis
select * FROM pgcr_dev.heat_rate_cap_factor where heat_rate_above_0<0;



