CREATE TABLE hindsight_prod.extra_weekends
(
    remainder           INT,
    start_dayofweek     INT,
    extra_weekends      INT
);

INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,1,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,1,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,1,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,1,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,1,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,1,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,1,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,2,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,2,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,2,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,2,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,2,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,2,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,2,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,3,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,3,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,3,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,3,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,3,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,3,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,3,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,4,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,4,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,4,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,4,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,4,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,4,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,4,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,5,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,5,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,5,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,5,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,5,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,5,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,5,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,6,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,6,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,6,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,6,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,6,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,6,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,6,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(0,7,0);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(1,7,1);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(2,7,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(3,7,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(4,7,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(5,7,2);
INSERT INTO hindsight_prod.extra_weekends(remainder,start_dayofweek,extra_weekends) VALUES(6,7,2);


SELECT
    O.start_date,
    O.end_date,
    O.workdays_cnt1 + O.remainder - V.extra_weekends AS workdays,
    O.weekend_cnt1 + V.extra_weekends AS weekends,
    O.start_dayofweek,
    O.remainder    
FROM
(    
    SELECT
        start_date,
        end_date,
        (DATEDIFF(day, start_date, end_date) / 7) * 5 AS workdays_cnt1,
        (DATEDIFF(day, start_date, end_date) / 7) * 2 AS weekend_cnt1,
        DATE_PART(dow, start_date) AS start_dayofweek,
        MOD(datediff(day, start_date, end_date), 7) AS remainder
) AS O        
FROM VW_captured AS V
INNER JOIN hindsight_prod.extra_weekends W
    ON V.start_dayofweek=W.start_dayofweek
    AND O.remainder=W.remainder;
    
    