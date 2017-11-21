
## Source the variable selection functions
source("./Development/VariableSelection/EaaLASSO_v2.R")
source("./Personal/VT/random_sampling_vs.R")

library(plyr)
library(AACloudTools)

# Global options
runInParallel <- FALSE   # TRUE is for parallel and FALSE is for sequential
localSNOW <- FALSE      # TRUE for using the local workstation, and FALSE when using R server


# Configuration for user to use AWS services
awsConfig <- AACloudTools::ConfigureAWS("./Config/config.json")

# Engage SNOW cluster, if required
clusterIsOn <- AACloudTools::AreEC2InstancesRunning(awsConfig)
if (!clusterIsOn && runInParallel && !localSNOW) {
  print("Starting EC2 instances... !! Make sure you stop them !!")
  AACloudTools::StartEC2Instances(awsConfig)
} else {
  print("The EC2 instances will not start")
}

# Before starting the SNOW cluster, the EC2 instances need to start
counter <- 1
while (clusterIsOn == FALSE) {
  clusterIsOn <- AACloudTools::AreEC2InstancesRunning(awsConfig)
  cat("\r", counter)
  counter <- counter + 1
  Sys.sleep(1)
}
cat("The EC2 instances have started")

# Start SNOW cluster
if (runInParallel) {
  cl <- AACloudTools::AAStartSnow(local = localSNOW, awsConfig)    # local should always equal FALSE when running on the R server
} else {
  cat("Cluster will not start")
}


# Sample size is the size of each sample - for Lasso it should be about 750 - 1000. Sampling is without replacement. Each variable has equal P() to be selected
# A list with the parameters for the payload function getFeatureSelectionModel(), which for this case is EaaLasso
appConfig <- list(
  inputData = testDf,                        # This should be iddsDataDtSampledTraining  !!
  target = "v134253300",                    
  chunk = 5,                                 # Number of samples to be drawn from the training file
  numberOfSamplesEachVar = 3,                # Number of times to reshuffle the training file
  rootDir = basename(getwd()),
  alpha = 1,
  nfolds = 10,
  lambda = NULL,
  coefficientThreshold = 0.1e-5
)


populationDf <- appConfig$inputData
cat(sprintf("The variable selection file has %i variables including the target variable", ncol(populationDf)))

populationSize <- ncol(populationDf[2:length(populationDf)])
numberOfPartitions <- populationSize / appConfig$chunk
totalSamples <- appConfig$numberOfSamplesEachVar * numberOfPartitions

# Test if the selected sample size is valid - The modulo of populationSize/sampleSize should be 0
if (populationSize %% appConfig$chunk == 0) {
  cat(sprintf("The selected sample size will provide %i samples per file, and a total of %i samples", numberOfPartitions, totalSamples))
  cat(sprintf("\n Each variable will be included in %i samples", appConfig$numberOfSamplesEachVar))
} else {
  cat("Change the sample size. The modulo is not 0")
}



# Sample the input data, and create a list of variable vectors which will constitue the chunks to be processed by the payload function
createChunks <- function(chunk, appConfig, logger) {
  listWithChunks <- list()
  
  for (s in (1:numberOfSamplesEachVar)) {
    inputData_no_target <- appConfig$inputData[, !names(appConfig$inputData) %in% target]
    sampledlistOfPredictors <- names(inputData_no_target)
    
    for (i in (1:numberOfPartitions)) {
      listSelectedPreds <- sample(sampledlistOfPredictors,
                                  size = chunk,
                                  replace = FALSE)
      
      listWithChunks[[paste("Sample_", s, "_", i, sep = "")]] <- c(target, listSelectedPreds)
      
      # Remove from the list of predictors those that have already been selected for a sample
      sampledlistOfPredictors <- setdiff(sampledlistOfPredictors, listSelectedPreds)
    }
  }
  return(listWithChunks)
}


# Create a dataframe from each chunk
listAllChunks <- createChunks(chunk = appConfig$chunk, appConfig = appConfig)
for (i in listAllChunks) {
  print(appConfig$inputData[, names(appConfig$inputData) %in% i])
}



##############################
# This function partitions the whole set of candidate predictors, and for each partition produces a rank-ordering of the predictors by the strength of association with the target.
# The argument chunk is an integer, used as a seed to affect the randomness of the paritioning process


appConfig <- list(
  inputFileName = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampled.RData",
  numberOfVariablesPerPartition = 500,
  target = "v134253300"
)


getPartitions <- function(randomSeed, totalNumberOfElements, numberOfPartitions) {
  set.seed(randomSeed)
  allIdx <- seq(totalNumberOfElements)
  paritionSize <- int(totalNumberOfElements / numberOfPartitions)
  
  retVal <- list()
  for (i in 1:(numberOfPartitions - 1)) {
    partition <- sample(allIdx, size = paritionSize, replace = FALSE)
    retVal[[i]] <- partition
    allIdx <- setdiff(allIdx, partition)
  }
  retVal[[numberOfPartitions]] <- allIdx
  return(retVal)
}



payLoadFunction <- function(chunk, appConfig, logger) {
  dfAll <- AACloudTools::LoadFromS3(appConfig$inputFileName)
  targetVar <- dfAll[, appConfig$target]
  
  dfAll <- dfAll[, !colnames(dfAll) %in% appConfig$target]
  partitionList <- getPartitions(chunk, ncol(dfAll), ceiling(ncol(dfAll) / appConfig$numberOfVariablesPerPartition))
  
  retVal <- list()
  for (partition in partitionList) {
    df <- cbind(targetVar, dfAll[, partition])
    
    # process with the payload function
    
    # append the output to an output
    
  }
}















# Test the function

sprintf("The number of data partitions to apply the payload function on is %i", length(tt))

appConfig$inputData[, names(appConfig$inputData) %in% test[[1]]]



####################################################################

# This is the payload function. In this project it is EaaLasso
getFeatureSelectionModel <- function(chunk, params, logger) {
  lassoModel <- EaaLasso(df = chunk,
                         alpha = params$alpha,
                         nfolds = params$nfolds,
                         parallel = FALSE,        # Do not use the parallel processing from the glmnet package
                         lambda = params$lambda)
  
  # Filter out the variables with low impact
  retVal <- lassoModel[!lassoModel$coef.value < params$coefficientThreshold, ]
  return(retVal)
}

# ParallelDoCall requires that the exported function be global
gblCopyOfPayload <<- getFeatureSelectionModel

lassoImportantVariables <- AACloudTools::ParallelDoCall(awsConfig, appConfig, dataPartitions,
                                                        packages = c("AACloudTools", "glmnet"),    # Sends the packages to the worker nodes. List any packages that I need
                                                        export = c("gblCopyOfPayload","EaaLasso"),            # Sends any functions that will be needed to the worker nodes
                                                        combine = "rbind",                         # Specify what to do with the output from each worker node
                                                        savePayloadLog = "No",                     # 
                                                        savePayloadLogTo = NULL,                   # Can be saved to S3
                                                        doPar = runInParallel)

###########
## !! VERY IMPORTANT TO STOP THE SNOW CLUSTER !!! ##
###########

## Combine the results from all LASSO models and add the short and long labels from the Attributes file
combinedPredictors <- plyr::rbind.fill(lassoImportantVariables)
names(combinedPredictors) <- c("predictors", "estimatedCoefficients")

## It would be better to substitute this with a summaryBy()
iddsDataAttributes <- AACloudTools::SqlToDf(paste0("SELECT series_id, shortlabel, longlabel
                                                    FROM ", hindsightAttributesSource,
                                                   " WHERE geo IN ('", geographies, "') AND
                                                            frequency = 'MONT'"))

iddsDataAttributes$seriesIdModified <- paste0("v", iddsDataAttributes$series_id)

combinedPredictors <- merge(x = combinedPredictors,
                            y = iddsDataAttributes,
                            by.x = "predictors",
                            by.y = "seriesIdModified",
                            all.x = TRUE)



# Save results (make sure awsConfig is loaded)
AACloudTools::SaveToS3(combinedPredictors, s3File = "s3://ihs-bda-data/projects/EAA/Models/combinedPredictors_lasso.RData")


# Stop the SNOW cluster
AACloudTools::AAStopSnow(cl, awsConfig)
if (clusterIsOn) {
  AACloudTools::StopEC2Instances(awsConfig)
}

counterEC2Stop <- 1
while (clusterIsOn == FALSE) {
  clusterIsOn <- AACloudTools::AreEC2InstancesRunning(awsConfig)
  cat("\r", counterEC2Stop)
  counterEC2Stop <- counterEC2Stop + 1
  Sys.sleep(1)
}
print("The EC2 instances have stopped")



