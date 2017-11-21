library(AACloudTools)
library(dplyr)
library(lubridate)
library(xlsx)
library(tidyr)
library(stringr)

options(java.parameters = "-Xmx8192m")
source('/home/johnsonp/Repos/EAA_Analytics/Personal/LK/work/Maritime/Scripts/NewtonRootCalc.R')

MyConn <- AACloudTools::GetRedshiftConnection()
AACloudTools::ConfigureAWS(configJSON='./Config/config.json', verbose=FALSE)
calls_raw <- AACloudTools::SqlToDf("select * 
                                    from mar_commoditysea.crudeoil_updated_draft")

intTrips <- calls_raw %>%
              dplyr::mutate(international_marker = ifelse(priorcountryname != countryname, 1, 0)) %>%
              group_by(lrno, mmsi) %>%
              arrange(arrdate) %>%
              mutate(trip_num = cumsum(international_marker)) %>%
              ungroup(lrno, mmsi) %>%
              group_by(lrno, mmsi, trip_num) %>%
              dplyr::mutate(arrdateTrip = first(arrdate), saildateTrip = last(arrdate), 
                            priorcountrynameTrip = first(priorcountryname), countrynameTrip = last(countryname),
                            arrivaldraughtTrip = first(arrivaldraught), departuredraughtTrip = last(departuredraught)) %>%
              ungroup(lrno, mmsi, trip_num) %>%
              distinct(lrno, mmsi, arrdateTrip, saildateTrip, 
                       priorcountrynameTrip, countrynameTrip, 
                       arrivaldraughtTrip, departuredraughtTrip, maxdraft, duration)

View(calls_raw %>% 
       filter(lrno == 9285823))

recTrips <- intTrips %>%
              dplyr::mutate(draft_change_marker = ifelse(arrivaldraughtTrip != departuredraughtTrip, 1, 0)) %>%
              group_by(lrno, mmsi) %>%
              arrange(arrdateTrip) %>%
              mutate(trip_num = cumsum(draft_change_marker)) %>%
              ungroup(lrno, mmsi) %>%
              group_by(lrno, mmsi, trip_num) %>%
              dplyr::mutate(arrdate = first(arrdateTrip), saildate = last(saildateTrip),
                            priorcountryname = first(priorcountrynameTrip), countryname = last(countrynameTrip),
                            arrivaldraught = first(arrivaldraughtTrip), departuredraught = last(departuredraughtTrip)) %>%
              ungroup(lrno,mmsi,trip_num) %>%
              distinct(lrno, mmsi, arrdate, saildate, priorcountryname, countryname, arrivaldraught, departuredraught,maxdraft,duration)

final_recTrips <- recTrips %>%
                    filter( priorcountryname != countryname & !is.na(arrivaldraught) & !is.na(departuredraught)) %>%
                    arrange(arrdate) %>%
                    dplyr::mutate(priorsaildate = lag(saildate))

compressed <- final_recTrips %>% 
                          mutate(duration = as.numeric(difftime(as.Date(saildate), as.Date(arrdate), units = 'hours'))) %>% 
                          arrange(lrno, arrdate)

calls_raw_treated <- calls_raw %>%
                      distinct(lrno, mmsi, maxdraft, arrdate, saildate, priorcountryname, countryname, arrivaldraught, departuredraught, priorsaildate, duration)

str(final_recTrips)
View(compressed %>%
      filter(lrno == 9285823))

# absd_ship_search_raw<- AACloudTools::SqlToDf("select * 
#                                              from ra.absd_ship_search
#                                              where vesseltype in ('TANKER','LIQUEFIED GAS TANKER')")

absd_ship_search <- AACloudTools::SqlToDf("select *
                                             from mar_commoditysea.crudeoilvessels")

absd_ship_search$bblDWT <- 1000 * absd_ship_search$dwt / 835 * 6.29 #in barrels

cleaning <- function(tblCombMovementCalls){
    
    allJourney_draughtDiff_full_clean <- tblCombMovementCalls %>%
      inner_join(absd_ship_search, by = c("lrno" = "imo")) %>%
      filter(statdecode == 'Crude Oil Tanker' & countryname != priorcountryname & arrdate > priorsaildate 
             & !is.na(arrivaldraught) & !is.na(departuredraught) & year(priorsaildate) > 2013) %>%
      dplyr::mutate(priorSailYear = year(priorsaildate), priorSailMonth = month(priorsaildate), priorSailDay = day(priorsaildate),
                    arrivaldraught = ifelse(arrivaldraught > maxdraft.x, maxdraft.x, arrivaldraught), departuredraught = ifelse(departuredraught > maxdraft.x, maxdraft.x, departuredraught),
                    draughtDiff = as.numeric(arrivaldraught) - as.numeric(departuredraught)) %>%
      distinct(lrno, priorcountryname, priorSailYear, priorSailMonth, priorSailDay, arrivaldraught, departuredraught, priorsaildate, arrdate, saildate,
               countryname, portname, draughtDiff)
    
    allJourney_volumeAllMethods <- allJourney_draughtDiff_full_clean %>%  
      inner_join(absd_ship_search, by = c("lrno" = "imo")) %>% 
      mutate(bblFromDWT = bblDWT / (maxdraft * .5) * abs(draughtDiff)) %>%   
      filter(arrivaldraught <= maxdraft & dwt > 0)
    
    graph = allJourney_volumeAllMethods %>%
      dplyr::mutate(year = year(arrdate),
                    month = month(arrdate)) %>%
      group_by(year, month, priorcountryname, countryname) %>%
      dplyr::mutate(estDWT = round(sum(bblFromDWT)/1000,0)) %>%
      distinct(year, month, priorcountryname, countryname, estDWT) %>%
      ungroup(year, month, priorcountryname, countryname) %>%
      arrange(countryname, priorcountryname, year, month) %>%
      filter(countryname == "United States of America")
    
    return(graph)
}

raw <- cleaning(calls_raw_treated)
compressed <- cleaning(compressed)


###### What is accuracy is remove Floating Storage ??

### Taking EIA Data

eia = read.csv("./Personal/JP/Wildcat/EIA_Oil_v1.csv", stringsAsFactors = FALSE)

eia_clean = eia %>%
              gather(date, Oil, X2.15.2014:X7.15.2017) %>%
              mutate( date = as.Date(substring(date, 2), format='%m.%d.%Y'),
                      year = year(date),
                      month = month(date),
                      oilInThousands = ifelse(is.na(Oil),0,Oil))

#eia_clean$country = word(eia_clean$Country,4)
getCountryName <- function(s){
  # sample string "U.S. Imports from United States of Crude Oil"
  pattern = "from (.+?) of"
  m = gregexpr(pattern,s)
  temp = gsub("from | of","",regmatches(s,m))
  return (temp)
}
eia_clean$countryName = getCountryName(eia_clean$Country)

#oilInThousands = ifelse(is.na(Oil),0,Oil),
#country = substring(Country,19,nchar(Country)-32))

reportedOil = eia_clean %>%
                select(countryName, year, month, oilInThousands)

#results = graph %>%
#            left_join(reportedOil, by = c("year" = "year", "month" = "month", "priorcountryname" = "countryName")) %>%
#            mutate(accuracy = ifelse(oilInThousands == 0 & estDWT != 0,0,estDWT/oilInThousands*100))
results = raw %>%
            left_join(graph, by = c("year", "month", "priorcountryname")) %>%
            dplyr::rename(estDWT_raw = estDWT.x, estDWT_compressed = estDWT.y) %>%
            left_join(reportedOil, by = c("year" = "year", "month" = "month", "priorcountryname" = "country")) %>%
            mutate(accuracy_raw = ifelse(oilInThousands == 0 & estDWT_raw != 0,0, estDWT_raw/oilInThousands*100),
                   accuracy_compressed = ifelse(oilInThousands == 0 & estDWT_compressed != 0,0, estDWT_compressed/oilInThousands*100)) %>%
            filter(priorcountryname %in% c('United Arab Emirates','Saudi Arabia', 'Russia', 'Nigeria', 'Iraq', 
                                           'Iran','Kuwait','Angola', 'Canada', 'Norway')) %>%
            select(priorcountryname, countryname.x, year, month, estDWT_raw, estDWT_compressed, oilInThousands, accuracy_raw, accuracy_compressed)

View(results)
