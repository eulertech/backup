SELECT O1.series_id, O1.series_row_cnt, O1.Date, A.timestamp
FROM
(
    SELECT series_id, COUNT(*) AS series_row_cnt, GETDATE() AS Date
    FROM
    (
      SELECT 
        series_id, 
        date, 
        datavalue,
        ROW_NUMBER() OVER(PARTITION BY series_id, date ORDER BY datavalue) AS rownum
      FROM hindsight_etl.stg_series_data_cleaned
    ) AS O
    WHERE O.rownum=1 
    GROUP BY series_id
) AS O1
INNER JOIN hindsight_etl.stg_series_attributes_cleaned AS A
    ON O1.series_id=A.series_id
    
    