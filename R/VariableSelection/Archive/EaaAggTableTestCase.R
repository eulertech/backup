# This is the test case for EaaAggTable. It ingests a data frame and creates feature rankings from various algorithms
# Input: Data frame with variables as columns
# Output: Data frame with variable rankings
# Author: Lou Zhang

# # PART 1: LOAD PACKAGES, LOAD DATA# # 

# # Load drivers

setwd("~/EAA_Analytics")

options(java.parameters="-Xmx4g")

wants <- c("AACloudTools", "AASpectre", "glmnet", "lmtest", "fpp", "h2o", "fUnitRoots", "bsts", "tseries", "lubridate", "stringr", "plyr", "dplyr","randomForest", "data.table", "forecast", "tidyr", "prophet",
           "doSNOW", "RODBC", "aod", "doParallel", "caret", "beepr", "mlbench", "corrplot", "usdm", "MASS", "dynlm", "nnet", "igraph",
           "foreach", "zoo", "reshape2", "vars", "devtools", "d3Network", "beepr", "MLmetrics", "doBy", "DataCombine", "TSclust", "dyn", "car", "rgp", "e1071", "tsoutliers", "infotheo", "mRMRe")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

awsConfig <- ConfigureAWS("Config/config.json")  

cl <- AAStartSnow(local=TRUE, awsConfig) 

#  Log-Modulus Transformation with Optional Constant for Zeroes
#  Note that this is reasonable for integers but not for values between -1 and +1
#  x = value
#  base = base (default e)
#  constant = constant to add to all values or just zeroes (default 1)
#  alwaysadd = whether to add constant to all values instead of just zeroes (default TRUE)
logmod <- function(x, base=exp(1), constant=1, alwaysadd=TRUE) {
  if (alwaysadd) {
    ifelse(x != 0, sign(x)*log(abs(x) + constant, base=base), log(constant, base=base))
  } else {
    ifelse(x != 0, sign(x)*log(abs(x), base=base), log(constant, base=base))
  }
}


# vlookup function

vlookup <- function(ref,             # the value or values that you want to look for
                    table,           # the table where you want to look for it; will look in first column
                    column,          # the column that you want the return data to come from,
                    range=FALSE,     # if there is not an exact match, return the closest?
                    larger=FALSE)    # if doing a range lookup, should the smaller or larger key be used?)
{
  if(!is.numeric(column) & !column %in% colnames(table)) {
    stop(paste("can't find column", column, "in table"))
  }
  if(range) {
    if(!is.numeric(table[,1])) {
      stop(paste("The first column of table must be numeric when using range lookup"))
    }
    table <- table[order(table[,1]),] 
    index <- findInterval(ref,table[,1])
    if(larger) {
      index <- ifelse(ref %in% table[,1],index,index+1)
    }
    output <- table[index,column]
    output[!index <= dim(table)[1]] <- NA
    
  } else {
    output <- table[match(ref,table[,1]),column]
    output[!ref %in% table[,1]] <- NA # not needed?
  }
  dim(output) <- dim(ref)
  output
}


# import reference table

setwd("~/EAA_Analytics")

reftable <- AACloudTools::SqlToDf("SELECT * FROM eaa_prod.eaa_attributes WHERE source like 'Connect' and frequency like 'M'")

# import monthly oil production data and predictors

allVars <- SpectreGetSeriesOfInterest(projectName="eaa_prod",
                                      varListId="abc",
                                      "frequency ~ 'MONT'",
                                      'MONT',
                                      "2001-01-01",  
                                      "2016-09-01", 
                                      minCoveragePct = 100)

Sys.sleep(20) 

MonthlySeriesRaw <- SpectreGetData(varList="abc",
                                             frequency='MONT', 
                                             dateFrom= "2001-01-01",
                                             dateTo= "2016-09-01",
                                             hasLagBuffer= FALSE) 

colnames(MonthlySeriesRaw) <- substring(colnames(MonthlySeriesRaw), 2)

oilProductionByMonth <- as.data.frame(AACloudTools::SqlToDf("SELECT * FROM eaa_prod.eaa_data WHERE name = 'ENP.WorldCrudeProduction' AND date <= '2016-08-01' AND date >= '2001-01-01'")$value)

#  oil <- c('v175310959','v134253300','v134253301','v175310959','v134253301','v175310959','v151387513','v153100429','v167792729','v167792760','v12905340','v167792843','v151387540','v151387541','v167792874','v1103596636','v16136954','v167792719','v12637810','v14326136','v153100430','v16136957','v12638003','v151387542','v151387512','v127401234','v180123009','v12638005','v16136955','v164401578','v167792833','v167792888','v16137003','v151124975','v167792762','v124220223','v151368216','v124220225','v167792761','v178708765','v167792875','v151387504','v175309942','v133629970','v175309941','v156653965','v163438224','v188509022','v175309567','v12511714','v12637814','v156653101','v153100174','v180946093','v12511866','v168704178','v168704177','v151124966','v130144523','v178708763','v12637812','v151124965','v12509208','v180122938','v169800232','v157568507','v142001988','v16137039')

#  MonthlySeriesA <- MonthlySeries[, !(names(MonthlySeries) %in% oil)]
#  MonthlySeriesA <- MonthlySeriesA[, 4:ncol(MonthlySeriesA)]

oilPrice <- as.data.frame(AACloudTools::SqlToDf("SELECT * FROM eaa_prod.eaa_data WHERE name = 'SPBRENTaUK.M' AND date <= '2016-08-01' AND date >= '2001-01-01'")$value)

colnames(oilProductionByMonth) <- "target"
colnames(oilPrice) <- "target"

# # PART 2: DIFFERENCING# # 

# conduct differencing and LOG the predictors
# oil production

#  AR <- slide(oilProductionByMonth, Var = colnames(oilProductionByMonth), slideBy = -1)
#  AR <- AR[2]
#  
#  AR[is.na(AR)] <- 0
#  colnames(AR) <- "AR"
#  
#  oilProductionByMonth <- (oilProductionByMonth - AR) / AR
#  oilProductionByMonth <- as.data.frame(oilProductionByMonth)
#  oilProductionByMonth[1,] <- 0
#  
#  colnames(oilProductionByMonth) <- "target"
#  
#  # oil price
#  AR <- slide(oilPrice, Var = colnames(oilPrice), slideBy = -1)
#  AR <- AR[2]
#  
#  AR[is.na(AR)] <- 0
#  colnames(AR) <- "AR"
#  
#  oilPrice <- (oilPrice - AR) / AR
#  oilPrice <- as.data.frame(oilPrice)
#  oilPrice[1,] <- 0
#  
#  colnames(oilPrice) <- "target"
#  
#  
#  # predictors
#  list_predictors <- colnames(MonthlySeriesA)
#  
#  k <- data.frame(1:nrow(MonthlySeriesA))
#  
#  for(i in 1:ncol(MonthlySeriesA)) {
#    j <- logmod(x = MonthlySeriesA[[i]])
#    k <- cbind(k,j)
#  }
#  
#  MonthlySeriesA <- k
#  MonthlySeriesA[is.na(MonthlySeriesA)] <- 0
#  MonthlySeriesA <- MonthlySeriesA[2:ncol(MonthlySeriesA)]
#  colnames(MonthlySeriesA) <- list_predictors
#  
#  MonthlySeriesASlide <- (1:188)
#  k <- as.data.frame(1:188)
#  
#  MonthlySeriesTest <- ts(MonthlySeriesA)
#  MonthlySeriesTestLagged <- dplyr::lag(MonthlySeriesTest, 1)
#  MonthlySeriesTestLagged[is.na(MonthlySeriesTestLagged)] <- 0
#  
#  MonthlySeriesTest <- (MonthlySeriesTest-MonthlySeriesTestLagged)/MonthlySeriesTestLagged
#  
#  MonthlySeriesTest <- as.data.frame(MonthlySeriesTest)
#  
#  colnames(MonthlySeriesTest) <- gsub("MonthlySeriesTest-MonthlySeriesTestLagged).MonthlySeriesTestLagged","",colnames(k))
#  MonthlySeriesTest[is.na(MonthlySeriesTest)] <- 0
#  
#  colnames(MonthlySeriesTest) <- list_predictors
#  
#  for(i in 1:ncol(MonthlySeriesTest)) {
#    
#    MonthlySeriesTest[[i]][is.infinite(MonthlySeriesTest[[i]])] <-0
#    MonthlySeriesTest[[i]][is.na(MonthlySeriesTest[[i]])] <-0
#    
#    
#  }
#  
#  MonthlySeriesA <- MonthlySeriesTest

# # PART 3: TRIMMING# # # 

# predictor restrictions to geo

MonthlySeriesRaw[is.na(MonthlySeriesRaw)] <- 0

setwd("~/EAA_Analytics/Development/VariableSelection")

source("EaaGeoFilter.R")

df <- EaaGeoFilter(MonthlySeriesRaw, consumer = "FALSE")

reftableConsidered <- reftable[reftable$name %in% colnames(df),]

zeroes <- apply(df, 2,function(x) any(x==0) )

df <- df[,!zeroes]

# trimming

source("EaaEDASummary.R")

edaSum <- EaaEDASummary(df)
edaSum2 <- as.data.frame(edaSum[1])

edaSumRobust <- edaSum2[edaSum2$endDate > "2016-08-01" & edaSum2$startDate <= "2000-01-01",]
df <- df[colnames(df) %in% edaSumRobust$variableID]

df <- cbind(oilPrice, df)

setwd("~/EAA_Analytics")

# # # # # # # # # # # 
# AGGREGATED TABLE
# # # # # # # # # # # 

# EaaAggAll takes several feature selection methods and aggregates them together in one data frame. You are required to feed it 
# a dataframe with the target variable in the first column

# This function may respond poorly if the series are differenced, or if >11,000 series are sent.

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

# add description and score

aggTable <- EaaAggAll(df, scaled = TRUE)

aggTableB <- as.data.frame(aggTable[1])
aggTableB$description <- vlookup(ref = aggTableB$variableID, table = reftable, column = 3)
aggTableB$BlendedScore <- aggTableB$LASSOScaled + aggTableB$MIScaled + aggTableB$RFNodePurityScaled

aggTableB <- aggTableB[,c(1,(ncol(aggTableB)-2):(ncol(aggTableB)), 2:(ncol(aggTableB)-3))]

aggTableB <- aggTableB[ order(-aggTableB[,"BlendedScore"]), ]

# upload to redshift

EaaUploadAggTable <- function(df, name) {

wants <- c("AASpectre", "AACloudTools")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

dataRaw <- varhandle::unfactor(df)

rowWithInvalidChars <-
  dataRaw[sapply(dataRaw$description, 
               function(x) {is.na(utf8ToInt(x)[1])},
               USE.NAMES=FALSE), c("variableID", "description")]

rowWithInvalidChars$description <- 
  iconv(rowWithInvalidChars$description, from = "latin1", to = "utf8", sub = "?")

dataRaw$description[dataRaw$variableID %in% rowWithInvalidChars$variableID] <- rowWithInvalidChars$description


#  Sets up the Logger.  "Logger" is a global variable
#  This one has 2 outputs: to the console with verbositiy up to "INFO"
#                          to a file, with verbosity up to "DBG"
Logger <- AALogger()
Logger$SetLogFile("csvUpload.log", logNr = 2,  overwrite = TRUE)
Logger$SetLogLevel("DBG", logNr = 2)
Logger$SetLogLevel("DBG", logNr = 1) #  dbg time only;  this log is typically set to be much less verbose

#  *** Initialize the configuration for Amazon Services (Redshift, S3 etc.)
#  kludge to use the config elsewhere than its conventional place, i.e. ./Config/config.json)
savedWd <- getwd()
setwd("../AASpectre")
ConfigureAWS("Config/config.json")
setwd(savedWd)
rm(savedWd)

#  *** write the two files to Redshift tables:  ***
featSelTable <- paste("eaa_analysis.", name, sep = "")
CreateTableInRedShift(featSelTable, dataRaw, recreate = TRUE)
UploadTableToRedshift(dataRaw, featSelTable, truncate = TRUE)

}

setwd("~/EAA_Analytics")

EaaUploadAggTable(aggTableB, "feature_selection_matrix")

AAStopSnow(cl, awsConfig) 

