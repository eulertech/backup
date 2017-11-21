#This function ingests a data frame and creates feature rankings from various algorithms
#Input: Data frame with variables as columns
#Output: Data frame with variable rankings
#Author: Lou Zhang

##Load drivers

wants <- c("AACloudTools", "AASpectre", "glmnet", "lmtest", "fpp", "h2o", "fUnitRoots", "bsts", "tseries", "lubridate", "stringr", "plyr", "dplyr","randomForest", "data.table", "forecast", "tidyr", "prophet",
           "doSNOW", "RODBC", "aod", "doParallel", "caret", "beepr", "mlbench", "corrplot", "usdm", "MASS", "dynlm", "nnet", "igraph",
           "foreach", "zoo", "reshape2", "vars", "devtools", "d3Network", "beepr", "MLmetrics", "doBy", "DataCombine", "TSclust", "dyn", "car", "rgp", "e1071", "tsoutliers", "infotheo", "mRMRe")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

EaaAggAll <- function(df, scaled = TRUE, nClust = ncol(df)/50, numberTrees = 500, maxEndNodes = 50) {
  
  # # Load drivers
  
  wants <- c("varhandle", "glmnet", "lmtest", "fpp", "visNetwork", "fUnitRoots", "bsts", "tseries", "lubridate", "stringr", "plyr", "dplyr", "randomForest", "data.table", "forecast", "tidyr", "prophet",
             "doSNOW", "RODBC", "aod", "doParallel", "caret", "beepr", "mlbench", "corrplot", "usdm", "MASS", "dynlm", "nnet", "igraph",
             "foreach", "zoo", "reshape2", "vars", "devtools", "d3Network", "beepr", "MLmetrics", "doBy", "DataCombine", "TSclust", "dyn", "car", "rgp", "e1071", "tsoutliers", "infotheo", "mRMRe")
  has   <- wants %in% rownames(installed.packages())
  if(any(!has)) install.packages(wants[!has])
  sapply(wants, require, character.only = TRUE)
  rm("wants","has")
  
  setwd("~/EAA_Analytics/Development/VariableSelection")
  
  source("EaaLASSO.R")
  source("EaaMutualInfo.R")
  source("EaaRandomForest.R")
  source("EaaTYGNCTest.R")
  source("EaaEDASummary.R")
  source("SpectreGetVariableClusters.R")
  
  range01 <- function(x){(x-min(x))/(max(x)-min(x))}
  
  if(scaled == TRUE) {
    
    for(i in 1:ncol(df)) {
      df[i] <- range01(df[i])
    }
  }
  
  df[is.na(df)] <- 0
  df[colSums(df) == 0] <- NULL
  
  # run through LASSO
  
  lasso <- EaaLASSO(df)
  colnames(lasso) <- c("variableID", "LASSOvalue", "LASSOScaled")
  lasso[is.na(lasso)] <- 0
  
  # run through Mutual Information
  
  mi <- EaaMutualInfo(df)
  mi$variableID <- rownames(mi)
  colnames(mi) <- c("MutualInformation", "MIScaled", "variableID")
  
  # run through Random Forest
  
  rf <- EaaRandomForest(df, numberTrees = numberTrees, maxEndNodes = maxEndNodes)
  colnames(rf) <- c("variableID", "RFVariableImportance", "RFNodePurity")
  
  rf$RFVariableImportanceRaw <- rf$RFVariableImportance
  rf$RFNodePurityRaw <- rf$RFNodePurity
  
  rf$RFVariableImportance <- log(as.numeric(as.character(rf$RFVariableImportance)))
  rf$RFNodePurity <- log(as.numeric(as.character(rf$RFNodePurity)))
  
  rf$RFVariableImportance[is.infinite(rf$RFVariableImportance)] <- 0
  rf$RFVariableImportance[is.na(rf$RFVariableImportance)] <- 0
  
  rf$RFNodePurity[is.infinite(rf$RFNodePurity)] <- 0
  rf$RFNodePurity[is.na(rf$RFNodePurity)] <- 0
  
  rf$RFVariableImportanceScaled <- range01(rf$RFVariableImportance)
  rf$RFNodePurityScaled <- range01(rf$RFNodePurity)
  
  # run through Causality tests
  
  causality <- EaaTYGNCTest(df)
  colnames(causality) <- c("PredictorCausesTarget", "TargetCausesPredictor")
  
  causality$variableID <- rownames(causality)
  
  # run through Cluster Analysis
  
  dfClean <- nearZeroVar(df)
  
  if(length(dfClean) > 0) {
    dfClean <- df[, -c(dfClean)]
  }
  
  if(length(dfClean) == 0) {
    dfClean <- df
  }
  
  clusters <- SpectreGetVariableClusters(data = as.data.table(dfClean), nbOfClust = nClust, nbOfVars = ncol(df)/nClust)
  colnames(clusters) <- c("variableID", "CorOwnCluster", "CorNextCluster", "Cluster")
  
  # clean and aggregate together in one data frame
  
  edaSum <- EaaEDASummary(df)
  c <- as.data.frame(edaSum[1])
  d <- edaSum[2:length(edaSum)]
  
  aggTable <- join_all(list(lasso, mi, rf, causality, c, clusters), by = "variableID", type = "left")
  
  return(list(aggTable, d))
  
}