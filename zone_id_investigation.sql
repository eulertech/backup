-- A journey of 'Elizabeth' , investigation of bad calls or incomplete calls
-- calls with problem  
-- A. calls within calls (should be labeled as transit)
-- B. calls with wrong starting port 

-------- 1. Look at the journey calls (11 calls between 2014-01-10 and 2014-02-20'
select lrno, arrdate, saildate, zoneid,previouszoneid, movetype,priorportname, portname  from ra.tblcombmovementcalls
      where lrno = '9257149' and saildate > '2013-12-10' and saildate < '2014-02-20';
      
select * from mar_commoditysea.crudeoil_combinedData limit 5;

select lrno, arrdate, saildate, zoneid,previouszoneid, movetype,priorportname, portname  from ra.tblcombmovementcalls
       where zoneid = 0 and previouszoneid = 0
select diff,count(*) from
    (select zoneid - previouszoneid as diff  from ra.tblcombmovementcalls
       where movetype like '%Transit%') A
       group by diff
       order by count(*) DESC
       

-- check whether zoneid is uniue to portname
select previouszoneid, 
   case  
    when (portname_count - previouszoneid_count)!= 0 then 1
    else 0
   end
  from
(select previouszoneid, count(portname) as portname_count, count(previouszoneid) as previouszoneid_count
  from ra.tblcombmovementcalls
  where portname IS NOT NULL
group by previouszoneid) A

-- check whether portname is unique to zoneid
select portname, 
   case  
    when (portname_count - previouszoneid_count)!= 0 then 1
    else 0
   end
  from
(select portname, count(portname) as portname_count, count(previouszoneid) as previouszoneid_count
  from ra.tblcombmovementcalls
  where portname IS NOT NULL
group by portname) A


select previouszoneid, count(*) over(partition by previouszoneid) as numports_perzoneid
  from ra.tblcombmovementcalls
  where portname IS NOT NULL and previouszoneid = '9861'
  group by previouszoneid,portname;

select distinct(portname) from ra.tblcombmovementcalls where previouszoneid = '9321'
 
select previouszoneid, portname, count(*) as numports_perzoneid
  from ra.tblcombmovementcalls
  where portname IS NOT NULL
  group by previouszoneid, portname
  having count(*) > 1
  order by previouszoneid DESC;
  

select count(distinct(portname)) from ra.tblcombmovementcalls where previouszoneid = '9861'
  and portname is not null


select * 
  from mar_commoditysea.crudeoil_updated_draft
  where lrno = 8117512
  order by lrno, mmsi, arrdate; 
  

select * from mar_commoditysea.crudeoil_updated_draft;
