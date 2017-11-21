
#####################################
#
# Random Forest modeling with the randomForest package
#
# http://www.statistik.uni-dortmund.de/useR-2008/slides/Strobl+Zeileis.pdf
# https://cran.r-project.org/web/packages/randomForest/randomForest.pdf
#
#####################################


# Load the needed packages
library(ggplot2)
library(randomForest)
library(miscTools)
library(MLmetrics)
library(lubridate)
library(AACloudTools)
library(tseries)
library(bsts)
library(tidyr)
library(forecast)


# Increase size of memory for Java
options(java.parameters = "-Xmx8192m")

# Brent is series_id = 134253300 from Hindsight
yVariable <- "v134253300"

# Parameters for Random Forest model
numberTrees <- 1000
#maxEndNodes <- 50
outputImportance <- TRUE

# Load AWS configuration to use AWS services
AACloudTools::ConfigureAWS(paste0(getwd(), "/Config/config.json", collapse = ""))
myConn <- AACloudTools::GetRedshiftConnection(url = Sys.getenv("PGURLRS"))

# Load older data saved to S3
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampled.RData")
# AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampledTraining.RData")



#######################################################
## Functions to calculate direction accuracy and summarize all stats
# 1) Month-over-month directional accuracy
DirectionAccuracyMom <- function(df, forecastSeries, actualSeries) {
  direction_accuracy_mom <- c()
  
  for (i in 2:nrow(df)) {
    direction_accuracy_mom[i] <- sign(df[, forecastSeries][i] - df[, forecastSeries][i-1]) == sign(df[, actualSeries][i] - df[, actualSeries][i-1])
  }
  return(direction_accuracy_mom)
}

# 2) Long term directional accuracy (!! IMPORTANT !! (1) training set -> "last_known_actual" = 1; (2) validation set -> "last_known_actual" = nrow(trainingData))
DirectionAccuracyLongTerm <- function(df, df_trainingData, last_known_actual, forecastSeries, actualSeries) {
  direction_accuracy_lt <- c()
  
  for (i in 2:nrow(df)) {
    direction_accuracy_lt[i] <- sign(df[, forecastSeries][i] - df_trainingData[, actualSeries][last_known_actual]) == sign(df[, actualSeries][i] - df_trainingData[, actualSeries][last_known_actual])
  }
  return(direction_accuracy_lt)
}

# 3) Summarize accuracy statistics
AccuracyStats <- function(dfSet, forecastSeries, actualSeries, directionMomMetric, directionLongTermMetric) {
  endOfValidationPeriods <- c("2014-05-01", "2015-09-01", "2017-05-01")
  modelNames <- c("short_term", "medium_term", "long_term")
  summary_stats <- data.frame(matrix(ncol = 7, nrow = length(endOfValidationPeriods)))
  colnames(summary_stats) <- c("models", "periods", "num_periods", "mape", "rmse", "da", "da_lt")
  
  for (i in 1:length(endOfValidationPeriods)) {
    df <- dfSet[row.names(dfSet) >= "2014-01-01" & row.names(dfSet) <= endOfValidationPeriods[i], ]
    
    summary_stats[i, 1] <- modelNames[i]
    summary_stats[i, 2] <- paste("2014-01-01", endOfValidationPeriods[i], sep = "_")
    summary_stats[i, 3] <- nrow(df)
    summary_stats[i, 4] <- MLmetrics::MAPE(df[, forecastSeries], df[, actualSeries])
    summary_stats[i, 5] <- MLmetrics::RMSE(df[, forecastSeries], df[, actualSeries])
    summary_stats[i, 6] <- (table(directionMomMetric[1:nrow(df)]) / length(na.omit(directionMomMetric[1:nrow(df)])))[2]
    summary_stats[i, 7] <- (table(directionLongTermMetric[1:nrow(df)]) / length(na.omit(directionLongTermMetric[1:nrow(df)])))[2]
  }
  return(summary_stats)
}

# 4) Test of residuals
ResidualsTest <- function(df, forecastSeries, actualSeries) {
  residualsModel <- df[, actualSeries] - df[, forecastSeries]
  
  # Jarque-Bera test for normally distributed
  # Null hypothesis: residuals are normally distributed
  jarqueBeraTest <- tseries::jarque.bera.test(residualsModel)
  
  # General test of nonlinearity: Box-Ljung test
  # Null hypothesis: residuals are independent (portmanteau tests for serial correlation)
  # The statistic measures the accumulated autocorrelation up to lag m of the residuals
  boxLjungTest <- Box.test(diff(residualsModel), type = "Ljung", lag = 20)
  
  return(data.frame(jarqueBeraTest$p.value, boxLjungTest$p.value))
}

# 5) Function to add seasonal dummies and take logs of the exogenous variables
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

# 6) Invert the forecast for training and validation
# For training period use start_value_diffinv = 27.63 which is from 02/2000. This is the brent price for the month prior to the first month (03/2000) in the training set
# For validation period use start_value_diffinv = 110.80 which is from 12/2013. This is the brent price for the month prior to the first month (01/2014) in the validation set
InvertForecasts <- function(df, forecastSeries, training_or_validation_period, start_value_diffinv) {
  
  if (training_or_validation_period == "training") {
    invertedValue <- exp(diffinv(df[, forecastSeries], xi = log(start_value_diffinv)))[-1]
  } else {
    invertedValue <- exp(diffinv(df[, forecastSeries], xi = log(start_value_diffinv)))[-1]
  }
  return(invertedValue)
}

#######################################################


# Auto data - currently load all countries combined, but it might be interesting to create OECD, non-OECD, BRIC, etc.
autoData <- AACloudTools::SqlToDf("SELECT date, sum(value) as auto_sales
                                  FROM eaa_prod.auto_insight_scenario
                                  GROUP BY date
                                  ORDER BY date")
autoData$date <- as.Date(autoData$date, "%Y-%m-%d")


# Curated predictors (from Mihaela)
curatedPredictors <- read.csv(file = "/home/valentint/EAA_Analytics/Personal/VT/brent_forecast_data_mihaela.csv",
                              sep = ",",
                              header = TRUE)
curatedPredictors$date <- as.Date(curatedPredictors$date, "%Y-%m-%d")

curatedPredictors <- curatedPredictors[, !colnames(curatedPredictors) %in%
                                         c("oilPrice", "sp500", "vix", "finStress",  # Drop variables with no, or unreliable, forecasts
                                           "futures3Month")]            # Drop variables with NAs in training or validation

# List of predictors from the automated variable selection
listPredictors <- c("v130144179", "v128136123", "v137013095",
                    "v130144572", "v179708954", "v152049483", "v131027438",
                    "v175309941", "v180197314", "v175309477", "v153100401", "v133630385", "v175309476",
                    "v155478663", "v134253383", "v175309942", "v155944855",
                    "v196744015","v12511747", "v134345266", "v12511748", "v151124965",
                    "v1106237782", "v155355937", "v181062677", "v1106492632",
                    "v179715733", "v12510362")

iddsDataDtMerge <- iddsDataDtSampled[, colnames(iddsDataDtSampled) %in% c("date", yVariable, listPredictors)]
iddsDataDtMerge$date <- as.Date(iddsDataDtMerge$date, "%Y-%m-%d")

fromMihaela <- merge(x = curatedPredictors,
                     y = autoData,
                     by = "date",
                     all.x = TRUE)

tData <- merge(x = iddsDataDtMerge,
               y = fromMihaela,
               by = "date",
               all.x = TRUE)

tData <- subset(tData, date >= "2000-01-01" & date <= "2017-05-01")   # End month is 05/2017 because some of the series from Mihaela end then


#############################################################################

## !! The transformation of series should be done on the SNOW cluster not here. There are too many series !!

# Add seasonal dummies, test stationarity of exogenous series and adjust them if needed
# The function expects that the first column is the date - I need to re-write it so that it is agnostic of the ordering
dataLevels <- CreateSeasonalDummies(df = tData,
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
# Add back the level for the oil price series and the date. However, since we lost one observation, start from the 2nd row
preDataAnalysisDf <- na.omit(merge(dataLevels[, names(pvalAll[condST])],
                              dataNS2, all = TRUE))

preDataAnalysisDf$v134253300_level <- tData[, yVariable][2:nrow(tData)]
preDataAnalysisDf <- as.data.frame(preDataAnalysisDf)

setcolorder(x = preDataAnalysisDf, neworder = c(yVariable, setdiff(colnames(preDataAnalysisDf), yVariable)))

# For now, drop all the dummmies and add the date back
#preDataAnalysisDf <- preDataAnalysisDf[, !colnames(preDataAnalysisDf) %in% c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov")]
preDataAnalysisDf <- preDataAnalysisDf[, !colnames(preDataAnalysisDf) %in% c("date")]


#############################################################################

# Lag the exogenous variables
lagMonths <- +1

dataAnalysisDf <- data.frame(matrix(NA, nrow = nrow(preDataAnalysisDf), ncol = ncol(preDataAnalysisDf)))
colnames(dataAnalysisDf) <- colnames(preDataAnalysisDf)

for (i in 1:ncol(preDataAnalysisDf)) {
  dataAnalysisDf[, i] <- Hmisc::Lag(preDataAnalysisDf[, i], lagMonths)
}


# Define the target
# v134253300_level - level of Brent price
# v134253300_dif - Differenced

colnames(dataAnalysisDf)[colnames(dataAnalysisDf) == "v134253300"] <- "v134253300_dif_lag"
colnames(dataAnalysisDf)[colnames(dataAnalysisDf) == "v134253300_level"] <- "v134253300_level_lag"

dataAnalysisDf$v134253300_dif <- preDataAnalysisDf$v134253300
dataAnalysisDf$v134253300_level <- preDataAnalysisDf$v134253300_level


# Split the data in training and validation
dataAnalysisDf <- dataAnalysisDf[rowSums(is.na(dataAnalysisDf)) == 0, ]
row.names(dataAnalysisDf) <- seq(as.Date("2000-03-01"), as.Date("2017-05-01"), by = "month")



###################################
# Define the target and split the data into training and validation

colnames(dataAnalysisDf)[colnames(dataAnalysisDf) == "v134253300_dif"] <- "target"    # Model (1, 2) - v134253300_dif; Model (3, 4) - v134253300_level
dataAnalysisDf <- dataAnalysisDf[, !colnames(dataAnalysisDf) %in% c("v134253300_dif_lag", "v134253300_level_lag")]     # Remove v134253300_dif because we need the actual level for the current period 
yVar <- "target"


trainingData <- subset(dataAnalysisDf, row.names(dataAnalysisDf) >= "2000-01-01" & row.names(dataAnalysisDf) <= "2013-12-01")
validationData <- subset(dataAnalysisDf, row.names(dataAnalysisDf) >= "2014-01-01" & row.names(dataAnalysisDf) <= "2017-05-01")



#######################################################
## Variable reduction and forecasting with BSTS
# Create local linear trend and add seasonal components


ss <- list()
ss <- AddAutoAr(ss, trainingData$target, lags = 6)
#ss <- AddLocalLinearTrend(ss, trainingData$target)          # This data is differenced and there is no trend
#ss <- AddSeasonal(ss, trainingData$target, nseasons = 12)   # The target is not seasonal

bstsXVar <- c("v12511748","v1106492632","v152049483","v130144572","oecdStocks","globalLiquidsDemand","v12510362","usDollar",
              "v130144179","gasolineDemand","ipGlobal","v134345266","v153100401","v1106237782","v134253383","v131027438")

# Estimate the BSTS model
bstsModel <- bsts(target ~ .,
                  state.specification = ss,
                  data = trainingData[, colnames(trainingData) %in% c("target", bstsXVar)],
                  #data = trainingData[, !colnames(trainingData) %in% c("v134253300_level")],
                  niter = 7000,
                  seed = 789456)


# Diagnostics of the model
# Prints out the posterior inclusion probabilities by variable
# -> Black bars indicate negative coefficients
# -> White bars indicate positive coefficients
# -> Gray is for indeterminate sign
plot(bstsModel, "coefficients")

plot(bstsModel, "components", same.scale = FALSE)  # Shows the amount of contribution by each component
plot(bstsModel, "predictors")                      # Plots the standardized target and predictors 
plot(bstsModel, "residuals")

summary(bstsModel)

# Create a dataframe with the inclusion probabilities for each of the predictors
bstsModelInclusionProb <- as.data.frame(summary(bstsModel)$coefficients)
colnames(bstsModelInclusionProb)[5] <- "inclusion_probability"
bstsModelInclusionProb <- bstsModelInclusionProb[order(bstsModelInclusionProb$inclusion_probability), ]
bstsModelInclusionProb$variables <- row.names(bstsModelInclusionProb)
bstsModelInclusionProb$variables <- factor(bstsModelInclusionProb$variables,
                                           levels = unique(bstsModelInclusionProb$variables))


#######################################################
# Estimate a random forest model using the variable selection from BSTS - use the top whatever (20?) variables

row.names(bstsModelInclusionProb[order(-bstsModelInclusionProb$inclusion_probability), ])[1:20]

xVar <- c("v153100401","v1106492632","v133630385","v134253383","v131027438","v127025996","v180197314","refineryOutput","oecdStocks",
          "otherSectorsDemand","lpgDemand","otherProductsDemand","cpi","usDollar","ipGlobal","v181062677","v175309941","v12511748","Aug","Jan")

xVar <- setdiff(colnames(trainingData), c("v134253300_level", "target"))

xVar <- setdiff(colnames(trainingData),
                c("v134253300_level", "target", "otherProductsDemand", "residualFuelDemand", "v155672306", "chinaImports",
                  "rmspreadXOECD", "Mar", "capUtil", "xopecSupply", "globalUnconv", "refineryOutput", "Apr", "vehicleSales",
                  "rmspreadOECD", "otherSectorsDemand", "jetFuelDemand", "Jul", "rmspreadGlobal", "May", "Jun", "Feb",
                  "pchem", "Aug", "oecdStocks", "naphthaDemand"))


#######################################################
## Predict using Random Forest

## 1. Training (in sample test)
fitEquation <- as.formula(paste0(yVar, "~", paste0(xVar, collapse = " + ")))

set.seed(789456)
rf_model <- randomForest::randomForest(formula = fitEquation,
                                       data = trainingData,
                                       ntree = numberTrees,
                                       importance = outputImportance,
                                       maxNodes = 500)

## Load a saved model
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/rf_model_trees1000_maxnodes500.RData")
rf_model <- rf_model_trees1000_maxnodes500

print(rf_model)
#summary(rf_model)
#str(rf_model)

print("The user provided number of trees for the RsF model is: "); rf_model$ntree
print("The estimated MTRY parameter for the RF model is: "); rf_model$mtry
print("The number of nodes for the RF model is: "); rf_model$forest$nrnodes


# Rank-order predictors by the calculated variable importance
varImportance <- data.frame(cbind("Variables" = row.names(rf_model$importance),
                                  rf_model$importance))

varImportance$AccuracyDecreaseImportance <- as.numeric(as.character(varImportance$X.IncMSE))
varImportance$GiniNodePurity <- as.numeric(as.character(varImportance$IncNodePurity))
varImportance <- varImportance[, !colnames(varImportance) %in% c("X.IncMSE", "IncNodePurity")]

varImportance <- varImportance[order(varImportance$AccuracyDecreaseImportance), ]
varImportance$Variables <- factor(varImportance$Variables, levels = unique(varImportance$Variables))
row.names(varImportance) <- 1:nrow(varImportance)

# Plot the predictors rank-ordered by the inclusion probabilities
ggplot2::ggplot(varImportance, aes(Variables, AccuracyDecreaseImportance)) +
  geom_col() +
  coord_flip()

#######################################################
## Diagnostics
# Shows the reduction in error from the number of trees
plot(rf_model, type = 'l')
#rf_model$mse

# Cross validation
#rf_model_cv <- randomForest::rfcv(trainingData[2:ncol(trainingData)], trainingData$target, cv.fold = 5, step = 0.5)
#with(rf_model_cv, plot(n.var, error.cv, log = "x", type = "o", lwd = 2))



### Accuracy scorecard
## 1. Training
trainingData <- data.frame(brent_fitted_model = rf_model$predicted, trainingData)
trainingData$brent_fitted_model_inverted <- exp(diffinv(trainingData$brent_fitted_model, xi = log(27.63)))[-1]    # Invert the forecast to calculate accuracy stats


# Calculate direction accuracy (We lose one observation in the calculation of direction)
rf_training_direction <- DirectionAccuracyMom(df = trainingData,
                                              forecastSeries = "brent_fitted_model_inverted",
                                              actualSeries = "v134253300_level")
rf_training_direction_lt <- DirectionAccuracyLongTerm(df = trainingData,
                                                      df_trainingData = trainingData,
                                                      last_known_actual = 1,
                                                      forecastSeries = "brent_fitted_model_inverted",
                                                      actualSeries = "v134253300_level")
rf_residuals_test <- ResidualsTest(df = trainingData,
                                   actualSeries = "target",
                                   forecastSeries = "brent_fitted_model")

rf_training_stats <- data.frame(row.names = seq(1),
                                models = "training",
                                period = "200001_201312",
                                num_perids = nrow(trainingData),
                                mape = MLmetrics::MAPE(trainingData$brent_fitted_model_inverted, trainingData$v134253300_level),
                                rmse = MLmetrics::RMSE(trainingData$brent_fitted_model_inverted, trainingData$v134253300_level),
                                da = (table(rf_training_direction) / length(na.omit(rf_training_direction)))[2],
                                da_lt = (table(rf_training_direction_lt) / length(na.omit(rf_training_direction_lt)))[2],
                                test = rf_residuals_test)

# Tests of the residuals for Random Forest
# Check the residuals are white noise
acf((trainingData$target - trainingData$brent_fitted_model), lag.max = 20)
pacf((trainingData$target - trainingData$brent_fitted_model), lag.max = 20)
plot(x = seq(nrow(trainingData)), y = (trainingData$target - trainingData$brent_fitted_model), type = "l")

ggplot(data = trainingData, aes(x = row.names(trainingData), group = 1)) +
  geom_line(aes(y = brent_fitted_model_inverted), color = "Red") +
  geom_line(aes(y = v134253300_level), color = "Green")



## 2. Validation
rf_model_validate <- predict(rf_model,
                             validationData,
                             type = "response",
                             norm.votes = TRUE,
                             predict.all = FALSE)

validationData <- data.frame(brent_forecast = rf_model_validate, validationData)
validationData$brent_forecast_inverted <- exp(diffinv(validationData$brent_forecast, xi = log(110.80)))[-1]    # Invert the forecast to calculate accuracy stats


# Calculate direction accuracy and summarize the accuracy stats
# The long term direction (validation_direction_lt) is calculated by subtracting the last known value from the training data
rf_validation_direction <- DirectionAccuracyMom(df = validationData,
                                                forecastSeries = "brent_forecast_inverted",
                                                actualSeries = "v134253300_level")

rf_validation_direction_lt <- DirectionAccuracyLongTerm(df = validationData,
                                                        df_trainingData = trainingData,
                                                        last_known_actual = nrow(trainingData),
                                                        forecastSeries = "brent_forecast_inverted",
                                                        actualSeries = "v134253300_level")

rf_validation_stats <- AccuracyStats(dfSet = validationData,
                                     forecastSeries = "brent_forecast_inverted",
                                     actualSeries = "v134253300_level",
                                     directionMomMetric = rf_validation_direction,
                                     directionLongTermMetric = rf_validation_direction_lt)

# Print stats for Random Forest
rf_training_stats
rf_validation_stats

ggplot(data = validationData, aes(x = row.names(validationData), group = 1)) +
  geom_line(aes(y = brent_forecast_inverted), color = "Red") +
  geom_line(aes(y = v134253300_level), color = "Green")



#######################################################
### Predict using BSTS

# Load a saved model
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/bsts_model_best1_10000.RData")
bstsModel <- bsts_model_best1_10000

## 1. Training (in sample test)
predictBstsTraining <- predict(bstsModel,
                               newdata = trainingData,
                               horizon = nrow(trainingData),
                               burn = 7000)

trainingData$bsts_brent_fitted <- as.numeric(predictBstsTraining$median)
trainingData$bsts_brent_fitted_inverted <- exp(diffinv(trainingData$bsts_brent_fitted, xi = log(27.63)))[-1]    # Invert the forecast to calculate accuracy stats

plot(predictBstsTraining, plot.original = 167)



# Calculate direction accuracy
bsts_t_direction_accuracy <- DirectionAccuracyMom(df = trainingData,
                                                  forecastSeries = "bsts_brent_fitted_inverted",
                                                  actualSeries = "v134253300_level")

bsts_t_direction_accuracy_lt <- DirectionAccuracyLongTerm(trainingData,
                                                          df_trainingData = trainingData,
                                                          last_known_actual = 1,
                                                          forecastSeries = "bsts_brent_fitted_inverted",
                                                          actualSeries = "v134253300_level")
bsts_residuals_test <- ResidualsTest(df = trainingData,
                                     actualSeries = "target",
                                     forecastSeries = "bsts_brent_fitted")

bsts_training_stats <- data.frame(row.names = seq(1),
                                  models = "training",
                                  period = "200001_201312",
                                  num_perids = nrow(trainingData),
                                  mape = MLmetrics::MAPE(trainingData$bsts_brent_fitted_inverted, trainingData$v134253300_level),
                                  rmse = MLmetrics::RMSE(trainingData$bsts_brent_fitted_inverted, trainingData$v134253300_level),
                                  da = (table(bsts_t_direction_accuracy) / length(na.omit(bsts_t_direction_accuracy)))[2],
                                  da_lt = (table(bsts_t_direction_accuracy_lt) / length(na.omit(bsts_t_direction_accuracy_lt)))[2],
                                  test = bsts_residuals_test)

## Test of the residuals for BSTS
# Check the residuals are white noise
acf((trainingData$target - trainingData$bsts_brent_fitted), lag.max = 20)
pacf((trainingData$target - trainingData$bsts_brent_fitted), lag.max = 20)
plot(x = seq(nrow(trainingData)), y = (trainingData$target - trainingData$bsts_brent_fitted), type = "l")

ggplot(data = trainingData, aes(x = row.names(trainingData), group = 1)) +
  geom_line(aes(y = bsts_brent_fitted_inverted), color = "Red") +
  geom_line(aes(y = v134253300_level), color = "Green")


## 2. Validation
predictBstsValidation <- predict(bstsModel,
                                 newdata = validationData,
                                 horizon = nrow(validationData),
                                 burn = 7000)

validationData$bsts_brent_forecast <- as.numeric(predictBstsValidation$median)
validationData$bsts_brent_forecast_inverted <- exp(diffinv(validationData$bsts_brent_forecast, xi = log(110.80)))[-1]    # Invert the forecast to calculate accuracy stats


# Calculate direction accuracy and summarize the accuracy stats
# The long term direction (validation_direction_lt) is calculated by subtracting the last known value from the training data
bsts_validation_direction <- DirectionAccuracyMom(df = validationData,
                                                  forecastSeries = "bsts_brent_forecast_inverted",
                                                  actualSeries = "v134253300_level")

bsts_validation_direction_lt <- DirectionAccuracyLongTerm(df = validationData,
                                                          df_trainingData = trainingData,
                                                          last_known_actual = nrow(trainingData),
                                                          forecastSeries = "bsts_brent_forecast_inverted",
                                                          actualSeries = "v134253300_level")

bsts_validation_stats <- AccuracyStats(dfSet = validationData,
                                       forecastSeries = "bsts_brent_forecast_inverted",
                                       actualSeries = "v134253300_level",
                                       directionMomMetric = bsts_validation_direction,
                                       directionLongTermMetric = bsts_validation_direction_lt)

# Summarize validation stats for BSTS
# Print stats for BSTS
bsts_training_stats
bsts_validation_stats



#######################################################
### Predict using Neural Network with an AR component

trainingDataTs <- xts::as.xts(trainingData)
validationDataTs <- xts::as.xts(validationData)

nnetarXVar <- setdiff(colnames(trainingData),
                      c("v134253300_level", "target", "otherProductsDemand", "residualFuelDemand", "v155672306", "chinaImports",
                        "rmspreadXOECD", "Mar", "capUtil", "xopecSupply", "globalUnconv", "refineryOutput", "Apr", "vehicleSales",
                        "rmspreadOECD", "otherSectorsDemand", "jetFuelDemand", "Jul", "rmspreadGlobal", "May", "Jun", "Feb",
                        "pchem", "Aug", "oecdStocks", "naphthaDemand", "pmiMfctg", "xoecdLiquidsDemand", "inventoryChange",
                        "gasolineDemand", "lpgDemand", "recessionUS", "dieselDemand", "v155478687", "v138312599", "v12637977"))

## 1. Train NNETAR model
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/nnetar_model_best2_p1_size15_rep100.RData")
nnetar_model <- nnetar_model_best2_p1_size15_rep100
nnetar_model$xreg

#nnetar_model <- forecast::nnetar(y = trainingDataTs$target,
#                                 p = 1,                                                # Define the lags for the AR component
#                                 size = 15,                                            # Number of nodes in the hidden layer
#                                 repeats = 100,                                         # Number of networks to fit with different random starting weights
#                                 xreg = trainingDataTs[, colnames(trainingDataTs) %in% nnetarXVar],
#                                 scale.inputs = TRUE)

trainingData$nnetar_brent_fitted <- as.numeric(nnetar_model$fitted)
trainingData <- trainingData[rowSums(is.na(trainingData)) == 0, ]
trainingData$nnetar_brent_fitted_inverted <- exp(diffinv(trainingData$nnetar_brent_fitted, xi = log(27.47)))[-1]    # Invert the forecast to calculate accuracy stats. We remove a row, that's why the starting actual value is for 04/2000, not 03/2000 as is the case with the other models


# Calculate direction accuracy (We lose one observation in the calculation of direction)
nnetar_training_direction <- DirectionAccuracyMom(df = trainingData,
                                                  forecastSeries = "nnetar_brent_fitted_inverted",
                                                  actualSeries = "v134253300_level")
nnetar_training_direction_lt <- DirectionAccuracyLongTerm(df = trainingData,
                                                          df_trainingData = trainingData,
                                                          last_known_actual = 1,
                                                          forecastSeries = "nnetar_brent_fitted_inverted",
                                                          actualSeries = "v134253300_level")
nnetar_residuals_test <- ResidualsTest(df = trainingData,
                                       actualSeries = "target",
                                       forecastSeries = "nnetar_brent_fitted")

nnetar_training_stats <- data.frame(row.names = seq(1),
                                    models = "training",
                                    period = "200001_201312",
                                    num_perids = nrow(trainingData),
                                    mape = MLmetrics::MAPE(trainingData$nnetar_brent_fitted_inverted, trainingData$v134253300_level),
                                    rmse = MLmetrics::RMSE(trainingData$nnetar_brent_fitted_inverted, trainingData$v134253300_level),
                                    da = (table(nnetar_training_direction) / length(na.omit(nnetar_training_direction)))[1],
                                    da_lt = (table(nnetar_training_direction_lt) / length(na.omit(nnetar_training_direction_lt)))[1],
                                    test = nnetar_residuals_test)

## Test of the residuals for BSTS
# Check the residuals are white noise
acf((trainingData$target - trainingData$nnetar_brent_fitted), lag.max = 20)
pacf((trainingData$target - trainingData$nnetar_brent_fitted), lag.max = 20)

# Plot residuals
plot(x = seq(nrow(trainingData)), y = (trainingData$target - trainingData$nnetar_brent_fitted), type = "l")

ggplot(data = trainingData, aes(x = row.names(trainingData), group = 1)) +
  geom_line(aes(y = nnetar_brent_fitted_inverted), color = "Red") +
  geom_line(aes(y = v134253300_level), color = "Green")



## 2. Validation
nnetar_model_validate <- forecast::forecast(nnetar_model,
                                            xreg = validationDataTs[, colnames(validationDataTs) %in% nnetarXVar])
summary(nnetar_model_validate)
plot(nnetar_model_validate)


validationData$nnetar_brent_forecast <- as.numeric(nnetar_model_validate$mean)
validationData$nnetar_brent_forecast_inverted <- exp(diffinv(validationData$nnetar_brent_forecast, xi = log(110.80)))[-1]    # Invert the forecast to calculate accuracy stats


# Calculate direction accuracy and summarize the accuracy stats
# The long term direction (validation_direction_lt) is calculated by subtracting the last known value from the training data
nnetar_validation_direction <- DirectionAccuracyMom(df = validationData,
                                                    forecastSeries = "nnetar_brent_forecast_inverted",
                                                    actualSeries = "v134253300_level")

nnetar_validation_direction_lt <- DirectionAccuracyLongTerm(df = validationData,
                                                            df_trainingData = trainingData,
                                                            last_known_actual = nrow(trainingData),
                                                            forecastSeries = "nnetar_brent_forecast_inverted",
                                                            actualSeries = "v134253300_level")

nnetar_validation_stats <- AccuracyStats(dfSet = validationData,
                                         forecastSeries = "nnetar_brent_forecast_inverted",
                                         actualSeries = "v134253300_level",
                                         directionMomMetric = nnetar_validation_direction,
                                         directionLongTermMetric = nnetar_validation_direction_lt)

# Summarize validation stats for BSTS
# Print stats for BSTS
nnetar_training_stats
nnetar_validation_stats


ggplot(data = validationData, aes(x = row.names(validationData), group = 1)) +
  geom_line(aes(y = nnetar_brent_forecast_inverted), color = "Brown") +
  geom_line(aes(y = v134253300_level))



#######################################################
# Plot the forecasts from the validation period
ggplot(data = validationData, aes(x = row.names(validationData), group = 1)) +
  geom_line(aes(y = brent_forecast_inverted), color = "Red") +
  geom_line(aes(y = bsts_brent_forecast_inverted), color = "Green") +
  geom_line(aes(y = nnetar_brent_forecast_inverted), color = "Brown") +
  geom_line(aes(y = v134253300_level)) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))


## Output a file with the target, forecasts and exogenous variables for plotting
write.csv(validationData[, colnames(validationData) %in% c("v134253300", "v134253300_level",
                                                           "brent_forecast", "brent_forecast_inverted",
                                                           "bsts_brent_forecast", "bsts_brent_forecast_inverted",
                                                           "nnetar_brent_forecast", "nnetar_brent_forecast_inverted")],
          "/home/valentint/EAA_Analytics/Personal/VT/brent_forecasts_output.csv")




################################################# Best models
# 1. BSTS
# The model in the presentation is bstsModel_best2_wAddAutoAr_7000
#AACloudTools::SaveToS3(bsts_model_best1_10000, s3File = "s3://ihs-bda-data/projects/EAA/Models/bsts_model_best1_10000.RData")
#AACloudTools::SaveToS3(bstsModel_best3_wAddAutoAr_7000, s3File = "s3://ihs-bda-data/projects/EAA/Models/bstsModel_best3_wAddAutoAr_7000.RData")
#AACloudTools::SaveToS3(bstsModel_best2_wAddAutoAr_7000, s3File = "s3://ihs-bda-data/projects/EAA/Models/bstsModel_best2_wAddAutoAr_7000.RData")
#AACloudTools::SaveToS3(bstsModel_best1_wAddAutoAr_7000, s3File = "s3://ihs-bda-data/projects/EAA/Models/bstsModel_best1_wAddAutoAr_7000.RData")

AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/bsts_model_best1_10000.RData")
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/bstsModel_best3_wAddAutoAr_7000.RData")
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/bstsModel_best2_wAddAutoAr_7000.RData")
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/bstsModel_best1_wAddAutoAr_7000.RData")

CompareBstsModels(list(bstsModel_best2_wAddAutoAr_7000,
                       bstsModel_best1_wAddAutoAr_7000,
                       bstsModel_best3_wAddAutoAr_7000,
                       bsts_model_best1_10000),
                  main = "Compare BSTS models")

# 2. NNETAR
# The model in the presentation is nnetar_model_best2_p1_size15_rep100
# AACloudTools::SaveToS3(nnetar_model_best1_p1_size15_rep100, s3File = "s3://ihs-bda-data/projects/EAA/Models/nnetar_model_best1_p1_size15_rep100.RData")
# AACloudTools::SaveToS3(nnetar_model_best2_p1_size15_rep100, s3File = "s3://ihs-bda-data/projects/EAA/Models/nnetar_model_best2_p1_size15_rep100.RData")

# 3. Random Forest
# AACloudTools::SaveToS3(rf_model_trees1000_maxnodes500, s3File = "s3://ihs-bda-data/projects/EAA/Models/rf_model_trees1000_maxnodes500.RData")


#######################################################


## Output a file for the LSTM model
# 1. Use the log differences for the exogenous variables and the differenced target
file_for_lstm_model_logdiff <- data.frame(date = row.names(dataAnalysisDf),
                                          dataAnalysisDf[, colnames(dataAnalysisDf) %in% c("target", xVar)])

setcolorder(file_for_lstm_model_logdiff, neworder = c("date", "target", setdiff(colnames(file_for_lstm_model_logdiff), c("date", "target"))))
write.csv(file_for_lstm_model_logdiff, "/home/valentint/EAA_Analytics/Personal/VT/file_for_lstm_model_logdiff.csv", row.names = FALSE)



# Read in the LSTM forecasts, transform them, and calculate acccuracy
lstmForecasts <- read.csv("/home/valentint/EAA_Analytics/Personal/VT/file_for_lstm_model_logdiff_withForecasts.csv")
lstmForecasts <- data.frame(lstmForecasts,
                            date = as.Date(row.names(dataAnalysisDf), "%Y-%m-%d"),
                            v134253300_level = dataAnalysisDf$v134253300_level,
                            rf_brent_forecast_inverted = c(trainingData$brent_fitted_model_inverted, validationData$brent_forecast_inverted),
                            bsts_brent_forecast_inverted = c(trainingData$bsts_brent_fitted_inverted, validationData$bsts_brent_forecast_inverted))

# inverse the forecast
lstmForecasts$lstm_forecast_brent_price_inverted <- ifelse(lstmForecasts$date >= "2000-01-01" & lstmForecasts$date <= "2013-12-01",
                                                           InvertForecasts(lstmForecasts,
                                                                           training_or_validation_period = "training",
                                                                           forecastSeries = "lstm_forecast_brent_price",
                                                                           start_value_diffinv = 27.63),
                                                           InvertForecasts(lstmForecasts,
                                                                           training_or_validation_period = "validation",
                                                                           forecastSeries = "lstm_forecast_brent_price",
                                                                           start_value_diffinv = 110.80))

## Test 1
lstmForecasts$test1 <- ifelse(lstmForecasts$date >= "2000-01-01" & lstmForecasts$date <= "2013-12-01",
                              exp(diffinv(lstmForecasts$lstm_forecast_brent_price, xi = log(27.63)))[-1],
                              exp(diffinv(lstmForecasts$lstm_forecast_brent_price, xi = log(110.80)))[-1])


ggplot(data = lstmForecasts, aes(x = date, group = 1)) +
  geom_line(aes(y = rf_brent_forecast_inverted), color = "Red") +
  geom_line(aes(y = bsts_brent_forecast_inverted), color = "Green") +
  geom_line(aes(y = lstm_forecast_brent_price_inverted), color = "Orange") +
  geom_line(aes(y = v134253300_level))


