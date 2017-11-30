
DROP VIEW IF EXISTS pgcr_dev.plant_characteristics_min_load_analysis;
CREATE VIEW pgcr_dev.plant_characteristics_min_load_analysis AS
SELECT *, countie*100.0/sum(countie) over (partition by facility_id_orispl, unit_id) as run_percentage
FROM
(SELECT facility_id_orispl, facility_name, unit_id, minimumload_mw, nameplatecapacity_mw, load_bucket, count(*) as countie FROM
(select *
, case when gross_load_mw < 0.1 * nameplatecapacity_mw then '< 10%'
       when gross_load_mw < 0.2 * nameplatecapacity_mw then '< 20%'
       when gross_load_mw < 0.3 * nameplatecapacity_mw then '< 30%'
       when gross_load_mw < 0.4 * nameplatecapacity_mw then '< 40%'
       when gross_load_mw < 0.5 * nameplatecapacity_mw then '< 50%'
       when gross_load_mw < 0.6 * nameplatecapacity_mw then '< 60%'
       when gross_load_mw < 0.7 * nameplatecapacity_mw then '< 70%'
       when gross_load_mw < 0.8 * nameplatecapacity_mw then '< 80%'
       when gross_load_mw < 0.9 * nameplatecapacity_mw then '< 90%'
       when gross_load_mw <= nameplatecapacity_mw then '<100%'
       when gross_load_mw > nameplatecapacity_mw then '>100%'
       ELSE NULL
       END load_bucket
from pgcr_dev.plant_characteristics_no_load_blips
WHERE gross_load_mw is not null
)
GROUP BY facility_id_orispl, facility_name, unit_id, minimumload_mw, nameplatecapacity_mw, load_bucket
);

select * from pgcr_dev.plant_characteristics_min_load_analysis where facility_id_orispl=1364 and unit_id=4;
select * from pgcr_dev.plant_characteristics_min_load_analysis where facility_id_orispl=6019 and unit_id=1;
select * from pgcr_dev.plant_characteristics_min_load_analysis where facility_id_orispl=6017 and unit_id=1;


SELECT distinct facility_id_orispl,facility_name,unit_id,minimumload_mw,nameplatecapacity_mw,minimumload_mw_calc
FROM
(SELECT *
, CASE WHEN load_bucket_rank * spike_flag = min(load_bucket_rank * spike_flag) over (partition by facility_id_orispl, unit_id) THEN cast(trim(substring(load_bucket,2,3)) as int) - 10
         END minimumload_mw_calc
FROM
(SELECT *, case when run_percentage_lag < run_percentage AND run_percentage_lead < run_percentage AND run_percentage > 5 then 1 
               WHEN run_percentage > 10 AND load_bucket != '< 10%' THEN 1
          END Spike_Flag
FROM
(SELECT *
       ,RANK() OVER (PARTITION BY facility_id_orispl,unit_id ORDER BY load_bucket) AS load_bucket_rank
       ,LAG(run_percentage) OVER (PARTITION BY facility_id_orispl,unit_id ORDER BY load_bucket) AS run_percentage_lag
       ,LEAD(run_percentage) OVER (PARTITION BY facility_id_orispl,unit_id ORDER BY load_bucket) AS run_percentage_lead
FROM pgcr_dev.plant_characteristics_min_load_analysis
)))
      




