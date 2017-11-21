

###########################################################
#
# Purpose: Develop a model to predict vehicle sales at state/make level
# Note: This program differs from the program I wrote for use on my local machine.
# The difference is the import of the data and the variable names. The data for
# this program resides in Redshift and the field names are slightly different.
#
#
# Author: Valentin Todorov
###########################################################



### TO DO:
# Combined data for small states (e.g. MT, AK, ND, WY) and makes (e.g. FIAT, etc.) into 
# Example: acura_ak + acura_wy + acura_nd = acura_other

# Remove combos with low volumes which likely will have no effect on the analysis
# Example: remove all of FIAT, remove 


## Create features
# 1) Volume of vehicles sold by brand
# 2) Market share of vehicles by brand
# 3) make/national volume and state_make/national volume

rm(list=ls())



# Load libraries
library(AACloudTools)
library(reshape2)
library(zoo)
library(data.table)
library(dplyr)
library(ggplot2)
library(plotly)
library(tidyverse)


# Increase size of memory for Java
options(java.parameters = "-Xmx8192m")

# Configure server access
myRedshiftUsername <- "valentint"
projectName <- "EAA_Analytics"
configJSONlocation <- paste("home", myRedshiftUsername, projectName, sep = "/")
ConfigureAWS(paste0("/", configJSONlocation, "/Config/config.json", collapse = ""))
myConn <- GetRedshiftConnection()



# Create a path for the data location
# path <- "C:/Users/bre49823/Desktop/AutoSales"

# Read in the months which are column-delimited instead of comma
# may17Sales <- read.table(paste(path, "/", "monthly_vehicle_sales_MAY2017.csv", sep = ""), header = TRUE, sep = ";")
# apr17Sales <- read.table(paste(path, "/", "monthly_vehicle_sales_APR2017.csv", sep = ""), header = TRUE, sep = ";")


# Create a list of the month for which we have vehicle sales, read them in and append the files
# monthlySales <- c("MAR2017", "FEB2017", "JAN2017", "DEC2016", "NOV2016", "OCT2016", "SEP2016", "JUL2016", "AUG2016", "JUN2016", "MAY2016")

# oemSales <- rbind(may17Sales, apr17Sales)

#for (i in 1:length(monthlySales)) {
#  inData <- read.csv(paste(path, "/monthly_vehicle_sales_", monthlySales[i], ".csv", sep = ""), header = TRUE)
#  oemSales <- rbind(oemSales, inData)
#}


# Get the vehicle sales data from Redshift
oemSales <- SqlToDf(c("select *",
                      "from auto_monthend.oemsales"))


table(oemSales$report_year_month)


# Keep only the major 32 makes (Volvo and Hyundai are missing from the data)
oemSales$make <- gsub(" |-", "_", tolower(oemSales$make))
oemSales$model <- gsub(" |-", "_", tolower(oemSales$model))
oemSales$state <- gsub(" ", "_", tolower(oemSales$state))
oemSales <- oemSales[!(oemSales$make %in% c("alfa_romeo", "bentley", "freightliner", "lamborghini",
                                            "mercury", "mini", "mitsubishi", "smartcar", "volvo",
                                            "plymouth", "sprinter")), ]

# Remove small makes and states
oemSales <- oemSales[!(oemSales$make %in% c("fiat", "ram", "land_rover", "porsche", "jaguar", "chrysler")), ]
oemSales <- oemSales[!(oemSales$state %in% c("ak", "de", "dc", "hi", "id", "me", "mt", "nd", "ne",
                                             "nh", "nm", "pr", "ri", "sd", "vt", "wv", "wy")), ]

table(oemSales$state)



## 1. Forecast by state and total make
oemSalesAggr <- aggregate(oemSales$oem_actuals,
                          by = list(month = oemSales$report_year_month,
                                    state = oemSales$state, make = oemSales$make), FUN = sum)
oemSalesAggr$make_state <- paste(oemSalesAggr$make, "_", oemSalesAggr$state, sep = "")
names(oemSalesAggr)[4] <- "smm_sales_volume"


# Transpose data from long to wide
oemSalesAggrTransposed <- dcast(oemSalesAggr, month ~ make_state, value.var = "smm_sales_volume")


# Transpose the data in the file aggregated by state and make
# Merge the aggregated file the file in which the data is by state/make/model
salesMonthMakeAggr <- aggregate(oemSalesAggr$smm_sales_volume,
                                by = list(month = oemSalesAggr$month, make = oemSalesAggr$make),
                                FUN = sum)
salesMonthMakeAggr$make_total <- paste(salesMonthMakeAggr$make, "_", "total", sep = "")
names(salesMonthMakeAggr)[3] <- "total_sales_volume"

salesMonthMakeAggrTransposed <- reshape2::dcast(salesMonthMakeAggr,
                                                month ~ make_total, value.var = "total_sales_volume")


# Merge the sales by state/make with the national/make sales
modelDf <- merge(oemSalesAggrTransposed, salesMonthMakeAggrTransposed, by = c("month"), all.x = TRUE)


# Remove all columns where some of the values are NAs
modelDf <- modelDf[, colSums(is.na(modelDf)) == 0]


# Create an empy data frame and append to modelDf
# The column names of appendDf and modelDf should be the same
emptyDf <- as.data.frame(matrix(nrow = 1, ncol = length(names(modelDf[, !names(modelDf) %in% c("month")]))))
datesForecast <- as.data.frame(seq(as.Date("2017-06-01"), as.Date("2017-06-01"), by = "month"))
names(datesForecast)[1] <- "month"

appendDf <- as.data.frame(cbind(datesForecast, emptyDf))
names(appendDf)[1:length(names(appendDf))] <- names(modelDf)


# Create date format in the oemSales file and append files
# The modelDfFInal data frame is the data frame whcih contains historical actuals & empty values out till 2018/05
# If needed the date range can be expanded to later than 2018/05
modelDf$month <- seq(as.Date("2011-01-01"), as.Date("2017-06-01"), by = "month")
modelDfFinal <- dplyr::bind_rows(modelDf, appendDf)


#### Prepare the data, lag features, create validation/training/forecast files, and train BSTS model
# Select one state/make to predict --> Toyota-California
modelDfFinalWork <- modelDfFinal

setnames(modelDfFinalWork, old = c("toyota_ca"), new = c("target"))
modelDfFinalWork <- data.frame(modelDfFinalWork[1],
                               modelDfFinalWork$target,
                               modelDfFinalWork[, !(names(modelDfFinalWork) %in% c("target", "month"))])

# Lag all the features - create a lagging function
# Columns 1 & 2 contain the month and target. That's we don't modify them
funcLag <- function(x) {
  lag(zoo(x), 1, na.pad = TRUE)
}
modelDfFinalWork <- data.frame(modelDfFinalWork[1:2],
                               apply(modelDfFinalWork[3:length(names(modelDfFinalWork))], 2, funcLag))
modelDfFinalWork <- modelDfFinalWork[2:nrow(modelDfFinalWork), ]


# Split data into training and validation files
trainDf <- subset(modelDfFinalWork, month <= "2016-06-01")
validateDf <- subset(modelDfFinalWork, month >= "2016-07-01")



#### Use variable selection methods to reduce the set of variables to use in the final models
# Source the variable selection functions
codeLocation <- paste0("/home/", myRedshiftUsername, "/EAA_Analytics/Development/VariableSelection", collapse = "")
source(paste0(codeLocation, "/EaaRandomForest.R", collapse = ""))
source(paste0(codeLocation, "/EaaLASSO.R", collapse = ""))
source(paste0(codeLocation, "/EaaRandomizedLASSO.R", collapse = ""))

featureSelectionDf <- trainDf[, !names(trainDf) %in% ("month")]


# 1. Run Random Forest - the code expects that the data has been cleaned up from NAs
rfModel <- EaaRandomForest(trainingData = featureSelectionDf,
                           numberTrees = 50,
                           outputImportance = TRUE,
                           maxEndNodes = 20)


# 2. LASSO
lassoModel <- EaaLASSO(df = featureSelectionDf,
                       weights = 1,
                       allfeatures = TRUE,
                       sorted = TRUE)


# 3. Randomized LASSO
randomizedLassoModel <- EaaRandomLASSO(df = featureSelectionDf,
                                       nbootstrap = 5,
                                       alpha = 0.2,
                                       allfeatures = TRUE,
                                       sorted = TRUE)


# 3. Randomized feature selection using BSTS or another method
# Develop a framework for variable selection using a general approach, but do the selection multiple times



# Merge the outputs from all feature selection methods and decide which features to use in the models development





#### 1) Develop BSTS Model - The model will be trained on data prior to 2016/07/01, and forecast for 12 months out starting in 2016/07/01
# Train, validate and forecast
library(bsts)

## Train BSTS model
df <- trainDf
colnames(df)[2] <- "target"

# Create linear trend and seasonal components
ss <- AddLocalLinearTrend(list(), df$target)
ss <- AddSeasonal(ss, df$target, nseasons = 12)

# Estimate the BSTS model
bstsModel <- bsts(target ~ .,
                  state.specification = ss,
                  data = df,
                  niter = 5000,
                  seed = 7890)

summary(bstsModel)



# Create a dataframe with the inclusion probabilities for each of the predictors
# The column called "inclusion_probability" should be used for rank ordering predictors
bstsModelInclusionProb <- as.data.frame(summary(bstsModel)$coefficients)
colnames(bstsModelInclusionProb)[5] <- "inclusion_probability"


# Diagnostics of results and forecasts. Plot the results
plot(bstsModel)
plot(bstsModel, "components")
plot(bstsModel, "coefficients")
plot(bstsModel, "predictors")


## Validate BSTS model
# Predict and calculate accuracy based on the Min/Max approach
predictBsts <- predict(bstsModel,
                       newdata = validateDf,
                       horizon = 24,
                       burn = 3000)

forecastsBsts <- data.frame(na.omit(cbind(month = validateDf[, 1],
                                          forecast = as.numeric(predictBsts$median),
                                          actual = as.numeric(validateDf$modelDfFinalWork.target))))
func_accuracy_min_man <- function(x){
  t <- min(x) / max(x)
}

forecastsBsts$accuracyMinMax <- apply(forecastsBsts[, 2:3], 1, func_accuracy_min_man)



# Plot the forecasts and actuals
ggplotly(ggplot(forecastsBsts, aes(x = month)) +
  geom_line(aes(y = forecast), colour = "Red") +
  geom_line(aes(y = actual)) +
  ylab(label = "Vehicle Sales") +
  xlab("Month"))


ggplotly(ggplot(forecastsBsts, aes(x = month)) +
  geom_line(aes(y = accuracyMinMax), colour = "Red") +
  ylab(label = "Accuracy (min/max)") +
  xlab("Month"))



#### 2) Develop ARIMA Model - The model will be trained on data prior to 2016/07/01, and forecast for 12 months out starting in 2016/07/01
library(forecast)

## Train ARIMA model
df <- trainDf
colnames(df)[2] <- "target"

# Estimate ARIMAX
# ARIMA CANNOT BE ESTIMATED with more than ~55 predictors - maybe the parameters need to be modified or I need to have variable selection

arimaXreg <- df[, 3:60]
length(names(df))

arimaxModel <- auto.arima(df$target,
                          xreg = arimaXreg,
                          seasonal = TRUE)

# Validate and predict



# Plot forecasts



# Plot the forecasts from BSTS and ARIMAX








