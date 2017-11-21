--list the number of records for each time series
SELECT C.series_id,  D.shortlabel,C.cnt FROM 
hindsight_prod.series_attributes D
INNER JOIN (
SELECT  A.series_id, COUNT(A.datavalue) AS cnt
FROM hindsight_prod.series_data A
INNER JOIN hindsight_prod.series_attributes B
ON A.series_id = B.series_id
WHERE B.frequency = 'MONT'  
GROUP BY A.series_id
ORDER BY A.series_id) C
ON D.series_id = C.series_id
ORDER BY C.cnt DESC
-- WHERE C.cnt = 1
-- WHERE D.series_id = '1103498854' 
--ORDER BY date
 ;

-- verify queries
select * from hindsight_prod.series_data
--where series_id = '178753568'
where series_id = '140410633'
