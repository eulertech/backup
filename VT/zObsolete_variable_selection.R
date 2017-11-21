
#############################################################################
#
# Purpose: Variable selection and analysis of factors that explain the prices
#
# Author: Valentin Todorov
#
#############################################################################

# Increase size of memory for Java
options(java.parameters = "-Xmx8192m")


rm(list = ls())
gc(reset = TRUE)


# Load the needed packages
library(AACloudTools)
library(ggplot2)
library(reshape2)
library(stringr)
library(lubridate)


# Sources of Hindsight data
hindsightSeriesSource <- "eaa_analysis.idds_data_subset"
hindsightAttributesSource <- "eaa_analysis.idds_attributes_subset"

# Enter your username and project name
myRedshiftUsername <- "valentint"
projectName <- "EAA_Analytics"

# Point to the JSON location
configJSONlocation <- paste("home", myRedshiftUsername, projectName, sep = "/")

# Configure the AWS services
ConfigureAWS(paste0("/", configJSONlocation, "/Config/config.json", collapse = ""))

# Get connection details to pass to the querries
myConn <- GetRedshiftConnection()


# Pull the IDDS data attributes table - keep the important geographies
# This includes top 15 oil importing, top 15 oil exporting and top 20 GDP countries and regions
geographies <- paste('165', '116', '114', '108', '153', '172', '244', '3',
                     '154', '64', '190', '10608', '70', '17', '144', '146',
                     '80', '81', '231', '232', '153', '82', '86', '235', '210',
                     '41', '166', '238', '170', '239', '111', '49', '11671', '241',
                     '178', '257', '226', '124', '10475', '5152', '5178', '11177',
                     '11695', '2464', '327', '331', '11702', '5194', '10356', '10537',
                     '126', '141', '10400', '12058', '12059', '10432', '12837', '3662',
                     '5198', '5133', '5230', '363', '5202', '3601', '330', '5207', '333',
                     '10416', '10417', '3614','5157', '364', '5209', '5231','5210', '5226',
                     '499', '5218', '5219', 'ROOT', '498', '5221', '5222', '5223', '5224',
                     sep = "','")

# Calculate the number of individual series
listOfSeries <- SqlToDf(paste0("SELECT *
                                FROM ", hindsightAttributesSource,
                               " WHERE geo IN ('", geographies, "')", collapse = ""))
sprintf("The number of monthly series in Hindsight is : %i", length(table(listOfSeries$series_id)))

# Pull the data from Hindsight
iddsData_original <- SqlToDf(paste0("SELECT b.*
                           FROM (SELECT top 300 * FROM ", hindsightAttributesSource,
                           " WHERE geo IN ('", geographies, "')) a
                           LEFT JOIN ", hindsightSeriesSource, " b
                           ON a.series_id = b.series_id", collapse = ""))

# Transpose the data
iddsData$seriesIdModified <- paste0("v", iddsData$series_id)
iddsDataTransposed <- dcast(iddsData[, c("date", "datavalue", "seriesIdModified")],
                            date ~ seriesIdModified, value.var = "datavalue")

## Keep only the data I need for the analysis
# Model 1 - Include all variables regardless if they have a forecast or not
# Model 2 - Include only variables with forecasts past 2017
table(year(iddsDataTransposed$date))
iddsDataTransposed2 <- iddsDataTransposed[year(iddsDataTransposed$date) >= 2000 &
                                           iddsDataTransposed$date <= "2017-08-01", ]

table(year(iddsDataTransposed2$date))

# Remove all series with NAs - at a later point, may include series with few missing and impute them
iddsDataDt <- iddsDataTransposed2[, colSums(is.na(iddsDataTransposed2)) == 0]
sprintf("The number of series with no NAs is: %i", (ncol(iddsDataDt) - 1))     # Subtract 1 for the date column


######################################################################
############### Create training and validation samples ###############
######################################################################

# Create a table where the target is the first column
# Brent is series_id = 134253300
yVariable <- "v134253300"
allSeriesButYvariable <- colnames(iddsDataDt[, !names(iddsDataDt) %in% c(yVariable, "date")])

# Select a random sample of columns
length(allSeriesButYvariable)
sampledSeries <- sample(allSeriesButYvariable, 20000)

# Need to keep only the sampledSeries list in the iddsDataDt frame
# Set the target variable as the first column
iddsDataDtSampled <- iddsDataDt[, names(iddsDataDt) %in% c(yVariable, "date", sampledSeries)]
setcolorder(iddsDataDtSampled, c(yVariable, "date", sampledSeries))
iddsDataDtSampled <- iddsDataDtSampled[with(iddsDataDtSampled, order(date)), ]

# Create training and validation datasets
# Training - 01/2000 - 12/2013
# Validation - 01/2014 - 08/2017
iddsDataDtSampledTraining <- subset(iddsDataDtSampled, date > "1999-12-01" & date <= "2013-12-01")
iddsDataDtSampledValidation <- subset(iddsDataDtSampled, date >= "2014-01-01")

# Remove the date field from iddsDataDt -  We don't need date in the dateframe for modeling
iddsDataDtSampledTraining <- iddsDataDtSampledTraining[, !names(iddsDataDtSampledTraining) %in% c("date")]

ncol(iddsDataDtSampledTraining)
View(iddsDataDtSampledTraining)


######################################################################
####################### Feature Selection ############################
######################################################################

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

