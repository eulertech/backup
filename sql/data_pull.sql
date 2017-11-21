with a as (
              select source, imo as lrno, mmsi, saildate, ping_arrdate as arrdate, 
                     lag(saildate) ignore nulls over (partition by lrno, mmsi order by ping_arrdate) as priorsaildate,
                     draft as draught,
                     (arrdate - lag(saildate) ignore nulls over (partition by lrno, mmsi order by ping_arrdate)) as duration, 
                     priorcountryname, countryname, priorportname, portname
                     from mar_commoditysea.crudeoil_combinedData
                     where source = 'M'),
    b as (
              select source, lrno, mmsi, saildate, arrdate, priorsaildate,
                     draught as arrivaldraught, 
                     lag(draught) ignore nulls over (partition by lrno, mmsi order by arrdate desc) as departuredraught,
                     duration, 
                     priorcountryname, countryname, priorportname, portname
                from a 
               where priorsaildate is not null 
                 and source = 'M')
              select b.*, v.maxdraft
                from b
                join mar_commoditysea.crudeoilvessels v
                  on b.lrno = v.imo
                 and b.mmsi = v.mmsi
               where priorcountryname is not null
                 and arrivaldraught is not null
                 and departuredraught is not null
                 and priorcountryname <> countryname
                 and arrivaldraught > 0
                 and departuredraught > 0
                 and arrivaldraught != departuredraught
                 and arrdate > '2014-01-01';
