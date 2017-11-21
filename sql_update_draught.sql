-- In this script, it created two temporary table to clean ping data from allais and allorb. Then merged to a cleaned table for updating the draught
-- Then it is used to get a dimension for each ship using above created table

-- Cleaning --
--- select only crude tanker dataset with valid number from ra.tblallaisfiles
--- Use 'sog' which speed over groud, draught >0 
--- only crude ships 
drop table if exists eaa_analysis.lk_pingAIS 
select 
      row_number() over (partition by imo order by change_date, ageminutes) as seqNum,
       ais_id, imo,
       DATEADD(minutes, CAST(FLOOR(ageminutes) AS INTEGER), change_date) AS pingTime,
       latitude, longitude, sog, draught, destination, vessellength, width,
       0 as distance
       into eaa_analysis.lk_pingAIS
  from ra.tblallaisfiles
  where IMO in 
  (select lrno from ra.absd_ship_search where statdecode = 'Crude Oil Tanker' and statuscode = 'S')
  and sog < 2 and draught > 0;

-- same thing above but with ra.tblallorbfiles
drop table if exists eaa_analysis.lk_pingORB 
select 
      row_number() over (partition by imo order by change_date, ageminutes) as seqNum,
       ais_id, imo,
       DATEADD(minutes, CAST(FLOOR(ageminutes) AS INTEGER), change_date) AS pingTime,
       latitude, longitude, sog, draught, destination,vessellength, width,
       0 as distance
       into eaa_analysis.lk_pingORB
  from ra.tblallorbfiles
  where IMO in 
  (select lrno from ra.absd_ship_search where statdecode = 'Crude Oil Tanker' and statuscode = 'S')
  and sog < 2 and draught > 0;


-- Union the two created table
/* JOIN AIS AND ORB DATA */
DROP TABLE IF EXISTS eaa_analysis.lk_pingAIS_ORB;
SELECT *
INTO eaa_analysis.lk_pingAIS_ORB
  FROM eaa_analysis.lk_pingAIS
UNION
SELECT *
  FROM eaa_analysis.lk_pingORB
  order by imo, pingtime;
select top 2 * from eaa_analysis.lk_pingORB;

/*
update eaa_analysis.lk_pingAIS_ORB 
  set distance_in_km = 111.111 *
    DEGREES(ACOS(COS(RADIANS(M1.Latitude))
         * COS(RADIANS(M2.Latitude))
         * COS(RADIANS(M1.Longitude - M2.Longitude))
         + SIN(RADIANS(M1.Latitude))
         * SIN(RADIANS(M2.Latitude))))  
    from eaa_analysis.lk_pingAIS_ORB as M1 
    join eaa_analysis.lk_pingAIS_ORB as M2 ON M1.imo = M2.imo and M1.seqNum = M2.seqNum - 1
*/

-- Compute the neighboring distance for each ship and
-- Shrink the above table by removing all rows with distance_in_km less than 1 km
drop table if exists eaa_analysis.lk_pingAISORB_neighbor_distance;
select * 
    into eaa_analysis.lk_pingAISORB_neighbor_distance
     from 
    (select M1.seqNum as seqNum, M2.seqNum as seqNumTo, M1.imo, M1.pingtime, 
      M1.latitude, M1.longitude, M1. draught, M1.sog,
      M1.destination, M1.vessellength, M1.width,
      111.111 *
      DEGREES(ACOS(COS(RADIANS(M1.Latitude))
           * COS(RADIANS(M2.Latitude))
           * COS(RADIANS(M1.Longitude - M2.Longitude))
           + SIN(RADIANS(M1.Latitude))
           * SIN(RADIANS(M2.Latitude))))  as distance_in_km
      from eaa_analysis.lk_pingAIS_ORB as M1 
      join eaa_analysis.lk_pingAIS_ORB as M2 ON M1.imo = M2.imo and M1.seqNum = M2.seqNum - 1
      order by imo, seqnum) A
    where A.distance_in_km > 0.1;
select count(*) from eaa_analysis.lk_pingAISORB_neighbor_distance;
select top 5 * from eaa_analysis.lk_pingAISORB_neighbor_distance;

--
select top 5 * from ra.tblcombmovementcalls;
--------------------Results Table I -----------------------------
-- merge the cleaned ping table with tblcombmovement table
drop table if exists eaa_analysis.lk_combmovement_updated_draught;
select * 
  into eaa_analysis.lk_combmovement_updated_draught
  from ra.tblcombmovementcalls A
  left join eaa_analysis.lk_pingAISORB_neighbor_distance B on A.lrno = B.imo
  where B.pingtime <= A.arrdate and B.pingtime <=A.priorsaildate and A.priorsaildate is not null;
  
select top 5 * from eaa_analysis.lk_combmovement_updated_draught;
----------------Results Table II ----------------------------------
-- script to get the length, width of the each ship to be merged with tblcombmovementcalls
drop table if exists eaa_analysis.lk_shipdimension
select imo,max(vessellength) as vessellength, max(width) as width, max(draught) as max_draught_seen, min(draught) as min_draught_seen
       into eaa_analysis.lk_shipdimension
  from eaa_analysis.lk_pingAIS_ORB
  group by imo;
   


