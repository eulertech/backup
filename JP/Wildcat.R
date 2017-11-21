# Here are some key terminology with ships:
#   
#   1. DWT, a.k.a, deadweight tonnage, which is a measure of how much mass a ship is carrying or can safely carry; ** It doesn't include the weight of the ship ** and is a sum of the weights of cargo, fuel, fresh water, ballast water, provisions, passengers, and crew.
# 
# 2. GT, a.k.a, Gross tonnage, is a nonlinear measure of a ship's overall internal volume. Gross tonnage is different from gross register tonnage. Gross tonnage has no unit. It can be used to inverse compute the ship's volume.
# $$GT = K \bullet V$$
# $$K = 0.2 + 0.02 log_{10}(V)$$
# $$GT = V (0.2 log_{10}(V) + 0.2)$$
# 
# 3. draught is the vertical distance between the waterline and the bottom of the hull (keel), with the thickness of the hull included.
# 
# 4. Displacement is the ship's weight. 

# load the packages


suppressPackageStartupMessages({
  
  source('/home/johnsonp/Repos/EAA_Analytics/Personal/LK/work/Maritime/Scripts/NewtonRootCalc.R')
  wants <- c('AACloudTools','knitr','pander','dygraphs','zoo','Quandl','ggplot2','magrittr','dplyr','ggmap','dygraphs','xts','Quandl','sqldf','lattice','ggExtra','gridExtra')
  has <- wants %in% rownames((installed.packages()))
  if(any (!has)) 
    install.packages(wants[!has])
  sapply(wants, require, character.only = TRUE)
})


# set up redshift environment
AACloudTools::ConfigureAWS(configJSON='/home/johnsonp/Repos/EAA_Analytics/Config/config.json', verbose=FALSE)
MyConn <- AACloudTools::GetRedshiftConnection()

downloaded <- TRUE
if(!downloaded){
  # Loading new data set to R environment 
  s3path <- "s3://ihs-temp/TrackingCrudeOilVesselData"
  #Load data 
  AACloudTools::DownloadFileFromS3(s3FileOrFolder=s3path , dataFileOrFolder="../Data/Input/", recursive = TRUE)
  
}

# get a table view of the raw data
tableName <- "absd_ship_search_01oct15"
SqlQuery <- paste0(c("select * ","from ra.",tableName, " order by vesselname limit 10 "), collapse = '')
print(paste0("SQL query to be executed: ", SqlQuery),collapse = ' ')
shipSearch <- dbGetQuery(MyConn, SqlQuery)
head(shipSearch)

# extract data for key columns
col2use <- c('flag','owner','ownercode','ownercodname','vesselname','vesseltype','gt','dwt','draught','breadth','depth',
             'dateofbuild','cob','status','portname')
sql <- paste("select ", paste(col2use, collapse = ','),"from ra.", tableName, " where vesseltype like '%TANKER%' order by ownercodname limit 1000;", collapse = '')
shipData2Use <- dbGetQuery(MyConn, sql)
names(shipData2Use)<-tolower(names(shipData2Use))
names(shipData2Use) <- gsub("ï»¿", "", names(shipData2Use), fixed = TRUE)
head(shipData2Use)


## Distribution of ships by country
# group by country, vessel type, count
col2use <- c('ownercodname','vesseltype')
# shipData2Use %>% 
#   select(one_of(col2use)) %>% 
#   mutate(one = 1) %>% 
#   group_by(ownercodname,vesseltype) %>% 
#   count() %>% 
#   arrange(desc(n)) %>% 
#   head(20)   %>% 
#   ggplot(aes(x = vesseltype, y = n, fill = vesseltype)) +
#   geom_bar(stat = 'identity',position = 'stack') + facet_grid(vesseltype ~ ownercodname) +
#   xlab('Vessel Type') +
#   ylab('Number of vessels') +
#   ggtitle("Distriution of Top 20 Vessel Ownership Per Country Per Vesseltype") +
#   theme(#axis.title.x=element_blank(),
#     axis.text.x=element_blank(),
#     axis.ticks.x=element_blank(),
#     plot.title = element_text(hjust = 0.5))

## Distribution of ships by vesseltype
# Look at the tonnage distribution per vesseltype
# shipData2Use %>% 
#   select(one_of(col2use)) %>% 
#   mutate(one = 1) %>% 
#   group_by(vesseltype) %>% 
#   count() %>% 
#   arrange(desc(n)) %>% 
#   ggplot(aes(x = vesseltype, y = n, fill = vesseltype)) +
#   geom_bar(stat = 'identity',position = 'stack') +
#   xlab('Tanker Type') +
#   ylab('Number of vessels') +
#   ggtitle("Global Tanker Type Distribution") +
#   theme(
#     axis.text.x=element_blank(),
#     axis.ticks.x=element_blank(),
#     plot.title = element_text(hjust = 0.5))

## Distribution of ships by GT, DWT, COB and status
# Look at the distribution of vessel GT, dwt, cob, status
shipData2Use %>% 
  select(vesseltype, gt) %>% 
  filter(vesseltype %in% c('TANKER','LIQUEFIED GAS TANKER')) %>% 
  #  select(vesseltype, gt, dwt, cob, status) %>% 
  ggplot() +
  geom_histogram(aes(x = gt, group = vesseltype, fill = vesseltype), alpha = 0.5) +
  facet_grid(~ vesseltype) +
  xlab('Gross Tonnage') +
  ylab("Density") +
  ggtitle('Gross Toannage Distribution By Type') +
  theme(plot.title = element_text(hjust = 0.5))


shipData2Use %>% 
  select(vesseltype, dwt) %>% 
  filter(vesseltype %in% c('TANKER','LIQUEFIED GAS TANKER')) %>% 
  #  select(vesseltype, gt, dwt, cob, status) %>% 
  ggplot() +
  geom_histogram(aes(x = dwt, group = vesseltype, fill = vesseltype), alpha = 0.5) +
  facet_grid(~ vesseltype) +
  xlab('Gross Tonnage') +
  ylab("Density") +
  ggtitle('Dead Weight Tonnage Distribution By Type') +
  theme(plot.title = element_text(hjust = 0.5))

shipData2Use %>%
  select(vesseltype, cob) %>% 
  group_by(vesseltype,cob) %>% 
  count() %>% 
  arrange(desc(n)) %>% 
  head(30) %>% 
  ggplot(aes(x=cob,y=n)) + facet_grid(~ vesseltype) +
  geom_bar(stat = 'identity') +
  xlab('Country of Birth') +
  ylab("Number of Ship Built") +
  ggtitle('Number of Ship Built By COB') +
  theme(plot.title = element_text(hjust = 0.5), axis.text.x=element_text(angle=90,hjust=1,vjust=0.5))

shipData2Use %>% 
  select(vesseltype, status) %>% 
  group_by(vesseltype, status) %>% 
  count() %>% 
  arrange(desc(n)) %>% 
  head(30) %>% 
  ggplot(aes(x=status,y=n)) + facet_grid(~ vesseltype) +
  geom_bar(stat = 'identity') +
  xlab('Ship Status') +
  ylab("Number of Ship") +
  ggtitle('Ship Status Distribution') +
  theme(plot.title = element_text(hjust = 0.5),
        axis.text.x=element_text(angle=90,hjust=1,vjust=0.5))

## Ship distribution by the port
# shipData2Use %>% 
# select(vesseltype, portname) %>% 
# group_by(vesseltype, portname) %>% 
# count() %>% 
# arrange(desc(n)) %>% 
# head(40) %>% 
# ggplot(aes(x=portname,y=n)) + facet_grid(~ vesseltype) +
# geom_bar(stat = 'identity') +
# xlab('Portname') +
# ylab("Number of Ship") +
# ggtitle('Ship by Port Distribution') +
# theme(plot.title = element_text(hjust = 0.5),
# axis.text.x=element_text(angle=90,hjust=1,vjust=0.5)) 

## Ship building activities
# shipbuilder activities 
dateConvert <- function(df, colname) {
  df[[colname]] <- strptime(paste0(df[[colname]],'28'),'%Y%M%d')
  return(df)
  
}

shipBuildActivity <- shipData2Use %>% 
  select(vesseltype, dateofbuild) %>% 
  filter(dateofbuild != '000000') %>% 
  group_by(vesseltype, dateofbuild) %>% 
  dplyr::mutate(count = n()) %>%
  #count() %>% 
  #dplyr::arrange(desc(n)) %>% 
  head(100)

shipBuildActivity$dateofbuild <- strptime(paste0(shipBuildActivity$dateofbuild,'28'),'%Y%M%d')
dates <- shipBuildActivity$dateofbuild
shipBuildActivity$newDate <- as.Date(dates)
shipBuildActivity$dateofbuild <- NULL


## Ship building activity vs. oil price

brentOilPrice <- Quandl('ODA/POILBRE_USD',type="xts",collapse = 'monthly')
str(shipBuildActivity)
shipBuildActivityDcast <- dcast(shipBuildActivity, newDate ~ vesseltype, sum, value.var = 'count')
rownames(shipBuildActivityDcast) <- shipBuildActivityDcast[,1]
shipBuildActivityDcast$newDate <- NULL
# W/O Brent price
dygraph(as.xts(shipBuildActivityDcast), main = "Ship Building Activities") %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.2,
              hideOnMouseOut = FALSE) %>% 
  dyRangeSelector(height = 20, strokeColor = "")

# With Brent Price
dataXts <- cbind(as.xts(shipBuildActivityDcast),brentOilPrice)
colnames(dataXts)[3] <- "Brent Price(USD)" 
dygraph(dataXts, main = "Ship Building Activities vs. Brent Price") %>%  
  dySeries("LIQUEFIED.GAS.TANKER", drawPoints = TRUE) %>% 
  #dySeries("TANKER", drawPoints = TRUE) %>% 
  dySeries("Brent Price(USD)") %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.2,
              hideOnMouseOut = FALSE) %>% 
  dyRangeSelector(height = 20, strokeColor = "")


### Method I: Use Gross Tonnage (GT)
# only keep tanker type ship
#absd_ship_search_raw<-fread("../Data/Input/absd_ship_search.csv",showProgress = FALSE)

absd_ship_search_raw<- AACloudTools::SqlToDf("select * 
                                             from ra.absd_ship_search
                                             where vesseltype in ('TANKER','LIQUEFIED GAS TANKER')")
absd_ship_search <- absd_ship_search_raw %>% 
  distinct(LRNO, .keep_all = TRUE)
# from GT
estimatedVol <- sapply(absd_ship_search$gt, newtwon_method)
absd_ship_search$maxVolumeM3 <- estimatedVol
# from DWT using medium crude oil density

### Method II: Use Dead Weight Tonnage (DWT)

# Estimate the maximum volume using DWT
# look at the maximum volume using the two methods

## dwt * (2205 lbs / ton) / (density of oil kg per m3 * lb per gallon) 
## https://sciencing.com/convert-metric-tons-barrels-8220711.html
absd_ship_search$bblDWT <- 1000 * absd_ship_search$dwt / 835 * 6.29 #in barrels
absd_ship_search$bblGT <- 1000 * absd_ship_search$maxVolumeM3 / 835 * 6.29

absd_ship_search %>% 
  filter(bblDWT > 0.0) %>% 
  select(bblGT, bblDWT) %>% head(5)

### Estimate the differences

# Evaluate the difference between the two methods
absd_ship_search %>% 
  select(dwt,gt,bblGT, bblDWT) %>% 
  filter(bblDWT > 0.0) %>% 
  mutate(difference = signif(100.0*(bblGT - bblDWT)/bblGT,1)) %>% 
  ggplot(aes(x = 'difference', y = difference)) +
  geom_boxplot()
ggtitle("Percentage difference for the two volume computation methods")

absd_ship_search %>% 
  select(dwt,gt,bblGT, bblDWT) %>% 
  filter(bblDWT > 0.0) %>% 
  ggplot(aes(x=bblGT, y=bblDWT)) + 
  geom_point() + 
  geom_smooth(method = 'lm') +
  ggtitle("Volume computed by GT and DWT")

absd_ship_search %>% 
  select(dwt,gt,bblGT, bblDWT) %>% 
  filter(bblDWT > 0.0) %>% 
  ggplot(aes(x=bblGT, y=bblDWT)) + 
  geom_point() + 
  geom_smooth(method = 'lm') +
  ggtitle("Volume computed by GT and DWT")
### Difference between draught and depth

absd_ship_search %>% 
  transmute(depth = as.numeric(depth)) %>% 
  ggplot(aes(depth)) + geom_histogram(stat = 'bin', binwidth = 1,  center = 0) + 
  ggtitle('Histogram of the water depth(m) for Tankers')

absd_ship_search %>% 
  transmute(draught = as.numeric(draught)) %>% 
  ggplot(aes(draught)) + geom_histogram(stat = 'bin', binwidth = 1, center = 0) +
  ggtitle('Histogram of the draught(m) for Tankers')

absd_ship_search %>% 
  transmute(draught = as.numeric(draught), depth = as.numeric(depth)) %>% 
  mutate(draught_depth_Diff = depth - draught) %>% 
  ggplot(aes(draught_depth_Diff)) + geom_histogram(stat = 'bin', binwidth = 1, center = 0) +
  ggtitle('Histogram of the difference between depth and draught for Tankders')


#Crude Oil Transported Estimation
## Load shipmovement data
#tblCombMovementCallsRaw<-fread("../Data/Input/tblCombMovementCalls.csv",showProgress = FALSE)
SqlQuery <- "with a as (
              select source, imo as lrno, mmsi, saildate, ping_arrdate as arrdate, 
lag(saildate) ignore nulls over (partition by lrno, mmsi order by ping_arrdate) as priorsaildate,
laggeddraft as draught,
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
and arrdate > '2014-01-01';"
tblCombMovementCallsRaw <- dbGetQuery(MyConn, SqlQuery)

names(tblCombMovementCallsRaw) <- gsub("ï»¿", "", names(tblCombMovementCallsRaw), fixed = TRUE)


### Add a new feature: travel duration
# add a column to compute the travel duration

tblCombMovementCalls <- tblCombMovementCallsRaw %>% 
  mutate(duration = as.numeric(difftime(as.Date(saildate), as.Date(arrdate), units = 'hours'))) %>% 
  arrange(lrno, arrdate)  

### Difference between arrivaldraught and departuredraught
# Look at the arrivaldraught and departuredraught
# If arrival draught is greater than departure, which means unload. Otherwise, it means load.

tblCombMovementCalls %>% 
  transmute(draughtDiff = as.numeric(arrivaldraught) - as.numeric(departuredraught) ) %>% 
  ggplot(aes(draughtDiff)) + geom_histogram(stat = 'bin',binwidth = 1, center = 0 ) +
  ggtitle('Histogram of the draught difference between arrival and departure Tankers') 

# look at the duration distribution
tblCombMovementCalls %>% 
  select(duration) %>% 
  filter(duration <1000) %>% 
  ggplot(aes(duration)) + geom_histogram(stat = 'bin',binwidth = 60, center = 0 ) +
  ggtitle('Histogram of the trip duration Tankers') 

# suspicious trips, with maximum duration of 666 days
suspiciousTrip <- tblCombMovementCalls %>% 
  filter(duration > 1440) %>% 
  select(priorcountryname,countryname ,priorportname, portname, duration, saildate, arrdate) %>% 
  arrange(desc(duration))
head(suspiciousTrip)

bad = tblCombMovementCalls %>%
  filter(arrivaldraught > ceiling(maxdraft) | departuredraught > ceiling(maxdraft))

# Remove bad trips
tblCombMovementCallsClean <- tblCombMovementCalls %>% 
  filter(duration < 1440) 

### Statistics about arrivaldraught and departuredraught

draughtDF <- sqldf("select lrno, stdev(arrivaldraught), stdev(departuredraught) from tblCombMovementCallsClean where arrivaldraught is not NULL and departuredraught is not NULL GROUP BY lrno")

head(draughtDF)

draughtDiffDF <- sqldf("select lrno, stdev(arrivaldraught - departuredraught) as stddev_diff from tblCombMovementCallsClean where arrivaldraught is not NULL and departuredraught is not NULL GROUP BY lrno")
head(draughtDiffDF)


## Create trips start and end in the all month
# look at the arrivaldraught, departuredraught for all journeys
allJourney<-sqldf("SELECT A.lrno, priorcountryname, strftime('%Y', A.priorsaildate) as priorSailYear,
                  strftime('%m', A.priorsaildate) as priorSailMonth,
                  strftime('%d', A.priorsaildate) as priorSailDay,
                  A.arrivaldraught, A.departuredraught,
                  A.priorsaildate, A.ArrDate, A.SailDate, 
                  A.countryname, a.portname
                  FROM tblCombMovementCallsClean A 
                  inner join 
                  absd_ship_search B 
                  on 
                  A.lrno = B.lrno
                  WHERE 
                  B.statdecode='Crude Oil Tanker' 
                  and a.countryname<>a.priorcountryname 
                  and ArrDate>priorsaildate 
                  and A.arrivaldraught != 'NULL' and A.departuredraught != 'NULL'
                  and strftime('%Y', A.priorsaildate)>2014
                  ORDER BY A.[priorcountryname], A.lrno, strftime('%Y', A.priorsaildate), strftime('%m', A.priorsaildate)")


# aggregate by countryname, and count the number of trips per country
counts <- sqldf("select priorcountryname, count(lrno) from allJourney group by priorcountryname")
# only get trip with different draught
allJourneyDiffdraught <- sqldf("select * from allJourney where arrivaldraught != departuredraught order by lrno, priorcountryname, strftime('%Y', priorsaildate), strftime('%m', priorsaildate)")
##Compute draught difference using arrival draught and departure draught
# compute draught difference using the arrival, departure draught
allJourney_valid <- allJourney %>% 
  mutate(draughtDiff = (as.numeric(arrivaldraught) - as.numeric(departuredraught)))

# fill in the draught difference using the ship statistics
allJourney_draughtDiff_full <- allJourney_valid %>%
                                filter( abs(draughtDiff) > 1)
#remove trips which doesn't have draughtDiff, there are 2171/21113 trips without draught difference
allJourney_draughtDiff_full_clean <- allJourney_draughtDiff_full[!is.na(allJourney_draughtDiff_full$draughtDiff), ]

# Look at ports and countries
country = 'Saudi Arabia'
test = allJourney_draughtDiff_full_clean[which(allJourney_draughtDiff_full_clean$draughtDiff & allJourney_draughtDiff_full_clean$countryname == country),]

ggplot(test, aes(x = draughtDiff)) +
  geom_histogram(stat = 'bin', binwidth = .5, aes(fill = portname)) +
  theme(legend.position = "none") +
  xlab("Draught Difference (Arrival - Depature)") + 
  ylab("Count of Ships") +
  ggtitle(paste0("Distribution of Draught Differences of Port Arrivals of ", country))


ggplot(allJourney_draughtDiff_full_clean[which(allJourney_draughtDiff_full_clean$draughtDiff > 0),], aes(x=draughtDiff)) +
  geom_bar(aes(fill = priorcountryname)) + 
  theme(legend.position="none")

test = allJourney_draughtDiff_full_clean %>%
  filter(abs(draughtDiff) < 2)

## Compute volume using the draught difference  
# an independent table was created to get the ship dimensions as there is no complete ship dimension in the abs_ship_search data. 
tableName <- "lk_shipdimension"
SqlQuery <- paste0(c("select * ","from eaa_analysis.",tableName), collapse = '')
print(paste0("SQL query to be executed: ", SqlQuery),collapse = ' ')
shipDimension <- dbGetQuery(MyConn, SqlQuery)
head(shipDimension)

colnames(shipDimension)[1] <- 'lrno'
shipDimension$lrno <- as.character(shipDimension$lrno)

# estimate the cargo volume using the draught difference
ratio = 0.8
columnName <- colnames(absd_ship_search)
columnName[1] <- 'lrno'
colnames(absd_ship_search) <- columnName 
volColumns <- c('priorsaildate','lrno','bblFromDraughtDiff','bblGT','bblDWT')

  ggplot(graph, aes(x=bblFromGT, y=bblFromDWT)) + 
  geom_point() + 
  geom_smooth(method = 'lm') +
  ggtitle("Maximum Estimated Barrel Capacity computed by GT and DWT")

  ggplot(graph, aes(x=bblFromDraughtDiff, y=bblFromDWT)) + 
    geom_point() + 
    geom_smooth(method = 'lm') +
    ggtitle("Maximum Estimated Barrel Capacity computed by GT and DWT")
  
  ggplot(graph, aes(x=bblFromSize, y=bblFromGT)) + 
    geom_point() + 
    geom_smooth(method = 'lm') +
    ggtitle("Maximum Estimated Barrel Capacity computed by Size and GT")
  
  ggplot(graph, aes(x = bblFromDraughtDiff, y = bblFromGT)) + 
    geom_point() + 
    geom_smooth(method = 'lm') +
    ggtitle("Volume computed by GT and DWT")
  
allJourney_volumeAllMethods <- allJourney_draughtDiff_full_clean %>%  
  join(shipDimension, by = 'lrno' ) %>% 
  join(absd_ship_search, by = 'lrno') %>% 
  mutate(bblFromDraughtDiff = ratio * width * vessellength * draught * (abs(draughtDiff)/draught) *  6.29, 
    bblFromDWT = bblDWT / draught * abs(draughtDiff),
    bblFromGT  = bblGT / draught * abs(draughtDiff)) %>%   
  filter(!is.na(bblFromDraughtDiff) & arrivaldraught <= draught & dwt > 0)

View(allJourney_volumeAllMethods %>%
  filter(countryname == "United Arab Emirates") %>%
  distinct(lrno, priorcountryname, arrdate, saildate, arrivaldraught, departuredraught, draught))
################################################
################################################
graph = allJourney_volumeAllMethods %>%
  dplyr::mutate(bblSize = ratio*width*vessellength*draught,
                year = year(arrdate),
                month = month(arrdate)) %>%
  group_by(year, month, priorcountryname, countryname) %>%
  dplyr::mutate(estSize = round(sum(bblFromDraughtDiff)/1000,0), 
            estDWT = round(sum(bblFromDWT)/1000,0),
            estGT = round(sum(bblFromGT)/1000,0)) %>%
  distinct(year, month, priorcountryname, countryname, estSize, estDWT, estGT) %>%
    ungroup(year, month, priorcountryname, countryname) %>%
    arrange(countryname, priorcountryname, year, month) %>%
    filter(countryname == "United States of America")
View(graph)

graph1 = allJourney_volumeAllMethods %>%
  dplyr::mutate(bblSize = ratio*width*vessellength*draught,
                year = year(arrdate),
                month = month(arrdate)) %>%
  group_by(year, priorcountryname, countryname) %>%
  dplyr::mutate(estSize = round(sum(bblFromDraughtDiff)/1000,0), 
                estDWT = round(sum(bblFromDWT)/1000,0),
                estGT = round(sum(bblFromGT)/1000,0)) %>%
  distinct(year, priorcountryname, countryname, estSize, estDWT, estGT) %>%
  ungroup(year,priorcountryname, countryname) %>%
  arrange(countryname, priorcountryname, year) %>%
  filter(countryname == "United States of America")
View(graph1)
write.xlsx(graph1, "/home/johnsonp/Repos/EAA_Analytics/Personal/JP/Wildcat/MaximumEstimationShipComparison_year.xlsx")

##########################################
##########################################
oilMove <-  allJourney_volumeAllMethods %>%
  distinct(lrno, priorcountryname, countryname, priorsaildate, arrdate, bblFromDraughtDiff, bblFromDWT, draughtDiff) %>%
  mutate(arrYear = year(arrdate), arrMonth = month(arrdate), arrDay = day(arrdate))
head(oilMove)

ggplot(oilMove, aes(draughtDiff, bblFromDWT)) + geom_point()
ggplot(oilMove, aes(draughtDiff, bblFromDraughtDiff)) + geom_point()

oilReceived <- oilMove %>%
  filter(draughtDiff > 0) %>%
  group_by(countryname) %>%
  dplyr::mutate(count = n(), totalVol = sum(bblFromDWT)) %>%
  ungroup(countryname) %>%
  dplyr::mutate(totalPerc = totalVol / sum(bblFromDWT)) %>%
  distinct(countryname, totalVol, totalPerc, count) %>%
  arrange(desc(totalVol))

oilSent <- oilMove %>%
  filter(draughtDiff < 0) %>%
  group_by(priorcountryname) %>%
  dplyr::mutate(count = n(), totalVol = sum(bblFromDWT)) %>%
  ungroup(priorcountryname) %>%
  dplyr::mutate(totalPerc = totalVol / sum(bblFromDWT)) %>%
  distinct(priorcountryname, totalVol, totalPerc, count) %>%
  arrange(desc(totalVol))

###########################
## Comparing Ship Counts ##
###########################

View(oilReceived)
View(oilSent)

#######################
## Doing Predictions ##
#######################

oilraw <- allJourney_volumeAllMethods %>%
  distinct(priorcountryname, countryname, priorsaildate, arrdate, bblFromDraughtDiff, bblFromDWT, draughtDiff)

##############
## Received ##
##############

day = 20

allJourneyRec <- allJourney_volumeAllMethods %>%
                  distinct(countryname, saildate, arrdate, bblFromDWT) %>%
                  mutate(sailYear = year(saildate), sailMonth = month(saildate), arrYear = year(arrdate), arrMonth = month(arrdate), arrDay = day(arrdate)) %>%
                  filter(arrYear > 2014) %>%
                  select(countryname, sailYear, sailMonth, arrYear, arrMonth, arrDay, bblFromDWT) 

sameMonthBbls <- allJourneyRec %>%
                    filter(arrYear == sailYear & arrMonth == sailMonth & arrDay < day) %>%
                    group_by(countryname, arrYear, arrMonth) %>%
                    dplyr::mutate(bblSameMonth = sum(bblFromDWT)) %>%
                    ungroup(countryname, arrYear, arrMonth) %>%
                    distinct(countryname, arrYear, arrMonth, bblSameMonth)

sameMonthBblsBaseline <- allJourneyRec %>%
                          filter(arrYear == sailYear & arrMonth == sailMonth & arrDay < 32) %>%
                          group_by(countryname, arrYear, arrMonth) %>%
                          dplyr::mutate(bblBaseline = sum(bblFromDWT)) %>%
                          ungroup(countryname, arrYear, arrMonth) %>%
                          distinct(countryname, arrYear, arrMonth, bblBaseline)
                          
allMonthBbls <- allJourneyRec %>%
                  group_by(countryname, arrYear, arrMonth) %>%
                  dplyr::mutate(bblAllMonth = sum(bblFromDWT)) %>%
                  ungroup(countryname, arrYear, arrMonth) %>%
                  distinct(countryname, arrYear, arrMonth, bblAllMonth)                  

combine_Journey <- sameMonthBbls %>%
                    inner_join(sameMonthBblsBaseline, by = c("countryname", "arrYear", "arrMonth")) %>%
                    inner_join(allMonthBbls, by = c("countryname","arrYear","arrMonth")) %>%
                    arrange(countryname, arrYear, arrMonth) %>%
                    group_by(countryname) %>%
                    dplyr::mutate(yearMonth = as.Date(paste0(arrYear,"-", arrMonth, "-",01)),
                                  cumbblSameMonth = cumsum(bblSameMonth),
                                  cumbblAllMonth = cumsum(bblAllMonth),
                                  cumbblBaseline = cumsum(bblBaseline),
                                  ratioSame = cumbblSameMonth / cumbblAllMonth,
                                  ratioBaseline = cumbblBaseline / cumbblAllMonth,
                                  allJourney_Prediction = bblSameMonth / lag(ratioSame,1),
                                  allJourney_Baseline = bblBaseline / lag(ratioBaseline,1),
                                  absolutePercentageErrorSame = abs((bblAllMonth - allJourney_Prediction) / bblAllMonth),
                                  absolutePercentageErrorBaseline = abs((bblAllMonth - allJourney_Baseline) / bblAllMonth),
                                  minMaxAccuracySame = pmin(bblAllMonth, allJourney_Prediction) / pmax(bblAllMonth, allJourney_Prediction),
                                  minMaxAccuracyBaseline = pmin(bblAllMonth, allJourney_Baseline) / pmax(bblAllMonth, allJourney_Baseline)
                    ) %>%
                    ungroup(countryname) %>%
                    distinct(countryname, yearMonth, 
                             bblSameMonth, bblAllMonth, 
                             cumbblSameMonth, cumbblAllMonth, cumbblBaseline, 
                             allJourney_Prediction, allJourney_Baseline,
                             minMaxAccuracySame, minMaxAccuracyBaseline) %>%
                    na.omit()

test = combine_Journey %>%
        filter(countryname == "China")

ggplot(test, aes(x = yearMonth)) +
  geom_line(aes(y = allJourney_Prediction, color = 'red')) +
  geom_line(aes(y = allJourney_Baseline, color = 'blue'))

accuracy <- test %>%
              distinct(yearMonth, minMaxAccuracySame, minMaxAccuracyBaseline) %>%
              dplyr::rename(Chosen_Accuracy = minMaxAccuracySame, Baseline_Accuracy = minMaxAccuracyBaseline) %>%
              transform(Chosen_Accuracy = round(Chosen_Accuracy * 100,2), Baseline_Accuracy = round(Baseline_Accuracy * 100,2))

ggplot(accuracy, aes(x = yearMonth)) +
  geom_line(aes(y = Chosen_Accuracy)) +
  geom_line(aes(y = Baseline_Accuracy, color = 'red'))

##############
##   Sent   ##
##############

day = 25
countryToSee = 'Saudi Arabia'

allJourneySent <- allJourney_volumeAllMethods %>%
  distinct(priorcountryname, priorsaildate, arrdate, bblFromDWT) %>%
  mutate(arrYear = year(arrdate), arrMonth = month(arrdate), sailYear = year(priorsaildate), sailMonth = month(priorsaildate), sailDay = day(priorsaildate)) %>%
  filter(sailYear > 2014) %>%
  select(priorcountryname, sailYear, sailMonth, arrYear, arrMonth, sailDay, bblFromDWT) 

sameMonthBbls <- allJourneySent %>%
  filter(arrYear == sailYear & arrMonth == sailMonth & sailDay < day) %>%
  group_by(priorcountryname, sailYear, sailMonth) %>%
  dplyr::mutate(bblSameMonth = sum(bblFromDWT)) %>%
  ungroup(priorcountryname, sailYear, sailMonth) %>%
  distinct(priorcountryname, sailYear, sailMonth, bblSameMonth)

sameMonthBblsBaseline <- allJourneySent %>%
  filter(arrYear == sailYear & arrMonth == sailMonth & sailDay < 32) %>%
  group_by(priorcountryname, sailYear, sailMonth) %>%
  dplyr::mutate(bblBaseline = sum(bblFromDWT)) %>%
  ungroup(priorcountryname, sailYear, sailMonth) %>%
  distinct(priorcountryname, sailYear, sailMonth, bblBaseline)

allMonthBbls <- allJourneySent %>%
  group_by(priorcountryname, sailYear, sailMonth) %>%
  dplyr::mutate(bblAllMonth = sum(bblFromDWT)) %>%
  ungroup(priorcountryname, sailYear, sailMonth) %>%
  distinct(priorcountryname, sailYear, sailMonth, bblAllMonth)                  

combine_Journey <- sameMonthBbls %>%
  inner_join(sameMonthBblsBaseline, by = c("priorcountryname", "sailYear", "sailMonth")) %>%
  inner_join(allMonthBbls, by = c("priorcountryname","sailYear","sailMonth")) %>%
  arrange(priorcountryname, sailYear, sailMonth) %>%
  group_by(priorcountryname) %>%
  dplyr::mutate(yearMonth = as.Date(paste0(sailYear,"-", sailMonth, "-",01)),
                cumbblSameMonth = cumsum(bblSameMonth),
                cumbblAllMonth = cumsum(bblAllMonth),
                cumbblBaseline = cumsum(bblBaseline),
                ratioSame = cumbblSameMonth / cumbblAllMonth,
                ratioBaseline = cumbblBaseline / cumbblAllMonth,
                allJourney_Prediction = bblSameMonth / lag(ratioSame,1),
                allJourney_Baseline = bblBaseline / lag(ratioBaseline,1),
                absolutePercentageErrorSame = abs((bblAllMonth - allJourney_Prediction) / bblAllMonth),
                absolutePercentageErrorBaseline = abs((bblAllMonth - allJourney_Baseline) / bblAllMonth),
                minMaxAccuracySame = pmin(bblAllMonth, allJourney_Prediction) / pmax(bblAllMonth, allJourney_Prediction),
                minMaxAccuracyBaseline = pmin(bblAllMonth, allJourney_Baseline) / pmax(bblAllMonth, allJourney_Baseline)
  ) %>%
  ungroup(priorcountryname) %>%
  distinct(priorcountryname, yearMonth, 
           bblSameMonth, bblAllMonth, 
           cumbblSameMonth, cumbblAllMonth, cumbblBaseline, 
           allJourney_Prediction, allJourney_Baseline,
           minMaxAccuracySame, minMaxAccuracyBaseline) %>%
  na.omit()

test = combine_Journey %>%
  filter(priorcountryname == countryToSee)

ggplot(test, aes(x = yearMonth)) +
  geom_line(aes(y = allJourney_Prediction, color = 'red')) +
  geom_line(aes(y = allJourney_Baseline, color = 'blue'))

accuracy <- test %>%
  distinct(yearMonth, minMaxAccuracySame, minMaxAccuracyBaseline) %>%
  dplyr::rename(Chosen_Accuracy = minMaxAccuracySame, Baseline_Accuracy = minMaxAccuracyBaseline) %>%
  transform(Chosen_Accuracy = round(Chosen_Accuracy * 100,2), Baseline_Accuracy = round(Baseline_Accuracy * 100,2))

ggplot(accuracy, aes(x = yearMonth)) +
  geom_line(aes(y = Chosen_Accuracy)) +
  geom_line(aes(y = Baseline_Accuracy, color = 'red'))

