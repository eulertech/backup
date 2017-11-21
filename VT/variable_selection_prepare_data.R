
#############################################################################
#
# Purpose: Variable selection and analysis of factors that explain the prices
#
# Author: Valentin Todorov
#
#############################################################################

#rm(list = ls())
#gc(reset = TRUE)


# Increase size of memory for Java
options(java.parameters = "-Xmx8192m")

# Load the needed packages
library(AACloudTools)
library(ggplot2)
library(reshape2)
library(stringr)
library(lubridate)
library(tidyr)
library(forecast)
library(xts)
library(tseries)



#######################################################
# Function to add seasonal dummies
CreateSeasonalDummies <- function(df, start_year_of_df, start_month_of_df, frequency_of_observations) {
  dt1 <- ts(df, start = c(start_year_of_df, start_month_of_df), freq = frequency_of_observations)
  dummies <- forecast::seasonaldummy(dt1)
  
  dt2 <- na.omit(cbind(df, dummies[1:nrow(df), ]))
  row.names(dt2) <- dt2[, 1]
  
  dt3 <- na.omit(xts::as.xts(dt2[, -1])) 
  dataLevels <- dt3 
  cc <- dt3[1, ]
  
  for (i in 1:ncol(dt3)) {
    cc[, i] <- min(dt3[, i])
    
    if(cc[, i] > 0) {
      dataLevels[, i] <- log(dt3[, i])
    } else {
      
      if(cc[, i] <0) {
        dataLevels[, i] <- dt3[, i]
      }
    }
  }
  
  return(dataLevels)
}

#######################################################

# Brent is series_id = 134253300 from Hindsight
yVariable <- "v134253300"

# Start and end periods for training and validation datasets
training_period_start <- "2000-01-01"
training_period_end <- "2013-12-01"
validation_period_start <- "2014-01-01"
validation_period_end <- "2017-08-01"

# Sources of Hindsight data
hindsightSeriesSource <- "hindsight_prod.series_data"
hindsightAttributesSource <- "hindsight_prod.series_attributes"

# Configure the AWS services
AACloudTools::ConfigureAWS(paste0(getwd(), "/Config/config.json", collapse = ""))
myConn <- AACloudTools::GetRedshiftConnection()

# Pull the IDDS data attributes table - keep the important geographies
# This includes only the top 15 oil importing, top 15 oil exporting and top 20 GDP countries and regions
geographies <- paste('153', '172', '244', '64', '190', '70', '17', '144', '146', '80', '231', '232', '153',
                     '82', '86', '235', '210', '41', '166', '238', '170', '239', '111', '49', '11671', '241',
                     '178', '257', '226', '10475', '2464', '11702', '10356', '10432', '3662', '5198', '5230',
                     '3601', '333', '10417', '3614','5157', '364', '5209', '5231', '5210', '499', '5218',
                     '5219', '5223', '5224', '498',
                     #'124', '10608', 'ROOT', '11695', '12059', '5226', '5221', '5222', '5152',
                     #'5178', '327', '331', '10537', '141', '10400', '12837', '5133', '363', '5202', '330',
                     #'5207', '10416', '108', '165', '3', '116', '114', '154', '81', '11177', '5194', 
                     #'126', '12058', 
                     sep = "','")


## Pull data from Hindsight - only keep the years for the analysis:
# Model 1 - Include all variables regardless if they have a forecast or not
# Model 2 - Include only variables with forecasts past 2017
iddsData <- AACloudTools::SqlToDf(paste0("SELECT dat.*
                                          FROM (SELECT series_id
                                                FROM ", hindsightAttributesSource,
                                                " WHERE geo IN ('", geographies, "') AND
                                                        frequency = 'MONT') atr
                                          LEFT JOIN (SELECT series_id, date, datavalue
                                                      FROM ", hindsightSeriesSource,
                                                      " WHERE date >= '2000-01-01' AND
                                                              date <= '2017-08-01') dat
                                          ON atr.series_id = dat.series_id"))

# Create a character series_id and transpose the data from long to wide to use for variable selection
# The WHERE condition in the SqlToDf statement above creates rows with all NAs (!!???). The patch to remove them is iddsData[iddsData$seriesIdModified != "vNA", ]
iddsData$seriesIdModified <- paste0("v", iddsData$series_id)
iddsData <- iddsData[iddsData$seriesIdModified != "vNA", ]
iddsDataDt <- reshape2::dcast(iddsData[, c("date", "datavalue", "seriesIdModified")],
                              date ~ seriesIdModified, value.var = "datavalue")
iddsDataDt$date <- as.Date(iddsDataDt$date, "%Y-%m-%d")


# Figure out how to fix the column name where the name is not properly formated
# For example the training data has a column name ->> "v1.56e+08"



# Remove all series with NAs - at a later point, may include series with few missing and impute them
iddsDataDt <- iddsDataDt[, colSums(is.na(iddsDataDt)) == 0]
sprintf("The number of series with no NAs is: %i", (ncol(iddsDataDt) - 1))     # Subtract 1 for the date column

# Create a table where the target is the first column
allSeriesButYvariable <- colnames(iddsDataDt[, !names(iddsDataDt) %in% c(yVariable, "date")])

# Select a random sample of columns. The sample shouldn't change, otherwise it will mess up the forecasting
set.seed(7894)
sampledSeries <- sample(allSeriesButYvariable, 59000)


# Need to keep only the sampledSeries list in the iddsDataDt frame
# Set the target variable as the first column
iddsDataDtSampled <- iddsDataDt[, names(iddsDataDt) %in% c(yVariable, "date", sampledSeries)]
setcolorder(iddsDataDtSampled, c("date", yVariable, sampledSeries))
iddsDataDtSampled <- iddsDataDtSampled[with(iddsDataDtSampled, order(date)), ]


iddsDataDtSampled[, yVariable]


#############################################################################

## !! The transformation of series should be done on the SNOW cluster not here. There are too many series !!

# Add seasonal dummies, test stationarity of exogenous series and adjust them if needed
# The function expects that the first column is the date - I need to re-write it so that it is agnostic to the ordering
dataLevels <- CreateSeasonalDummies(df = iddsDataDtSampled,
                                    start_year_of_df = 2000,
                                    start_month_of_df = 1,
                                    frequency_of_observations = 12)

# Check stationarity and difference the series as appropriate
# Use the KPSS test for stationarity - The null hypothesis is that the series is stationary (i.e. it has unit root)
kpssAll <- apply(dataLevels, 2, tseries::kpss.test)

pvalAll <- list() 
for (k in 1:length(kpssAll)) {
  pvalAll[[k]] <- kpssAll[[k]]$p.value
}
names(pvalAll) <- colnames(dataLevels) 

# Select the stationary series. Transform the non-stationary by differencing them
condNS <- sapply(pvalAll, function(x) x < 0.05)
condST <- sapply(pvalAll, function(x) x > 0.05)

dataNS <- dataLevels[, names(pvalAll[condNS])]
dataNS2 <- dataNS

for (i in 1:ncol(dataNS)) {
  dataNS2[, i] <- diff(dataNS[, i])
}


# Combine the differenced and the stationary series
# Add back the level for the oil price series
iddsDataDtSampledStationary <- na.omit(merge(dataLevels[, names(pvalAll[condST])],
                                             dataNS2, all = TRUE))
iddsDataDtSampledStationary[, yVariable] <- iddsDataDtSampled[, yVariable][2:nrow(iddsDataDtSampled)]

# For now, drop all the dummmies
iddsDataDtSampledStationary <- iddsDataDtSampledStationary[, !names(iddsDataDtSampledStationary) %in% c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov")]


#############################################################################
# Create training and validation samples
iddsDataDtSampledTraining <- as.data.frame(iddsDataDtSampledStationary[paste(training_period_start, training_period_end, sep = "/")])
iddsDataDtSampledValidation <- as.data.frame(iddsDataDtSampledStationary[paste(validation_period_start, validation_period_end, sep = "/")])


# Store the training and validation data sets
AACloudTools::SaveToS3(iddsDataDtSampledTraining, s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampledTraining_20171012.RData")
AACloudTools::SaveToS3(iddsDataDtSampledValidation, s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampledValidation_20171012.RData")



#############################################################################
########################    Feature Selection    ############################
#############################################################################

# Source the variable selection functions
codeLocation <- paste0("/home/", myRedshiftUsername, "/EAA_Analytics/Development/VariableSelection", collapse = "")
source(paste0(codeLocation, "/EaaRandomForest.R", collapse = ""))
source(paste0(codeLocation, "/EaaBSTSFunction.R", collapse = ""))
source(paste0(codeLocation, "/EaaMutualInfo.R", collapse = ""))
source(paste0(codeLocation, "/EaaLASSO.R", collapse = ""))
source(paste0(codeLocation, "/EaaRandomizedLASSO.R", collapse = ""))


## 1. Run Random Forest - the code expects that the data has been cleaned up from NAs
rfModel <- EaaRandomForest(trainingData = iddsDataDtSampledTraining,
                           numberTrees = 500,
                           outputImportance = TRUE,
                           maxEndNodes = 100)

## 2. BSTS - # of iteration should be divisible by 10. One of the parameters of the bsts function is "ping = niter / 10"
bstsModel <- EaaBSTS(df = iddsDataDtSampledTraining,
                     niter = 5000,
                     nseasons = 12,
                     seed = 7890)

## 3. LASSO
lassoModel <- EaaLASSO(df = iddsDataDtSampledTraining,
                       weights = 1,
                       allfeatures = TRUE,
                       sorted = TRUE)

## 4. Randomized LASSO
randomizedLassoModel <- EaaRandomLASSO(df = iddsDataDtSampledTraining,
                                       nbootstrap = 5,
                                       alpha = 0.2,
                                       allfeatures = TRUE,
                                       sorted = TRUE)

## 5. Mutual Information
mutualInfo <- EaaMutualInfo(df = iddsDataDtSampledTraining,
                            bins = nrow(df)^(1/3))

