
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


# numberOfSamplesEachVar - number of times to reshuffle the main dataset
# sampleSizePerFile - number of columns in the sampled files
# Sample size is the size of each sample - for Lasso it should be about 750 - 1000
# Sampling is without replacement. Each variable has equal P() to be selected
numberOfSamplesEachVar <- 100            # Number of times to reshuffle the training file
sampleSizePerFile <- 50                 # Number of samples to be drawn from the training file

populationDf <- dfSynthetic[1:201]      # The data set should be iddsDataDtSampledTraining
ncol(populationDf)
nrow(populationDf)

cat(sprintf("The variable selection file has %i variables including the target variable", ncol(populationDf)))

populationSize <- ncol(populationDf[2:length(populationDf)])
numberIterations <- populationSize / sampleSizePerFile
totalSamples <- numberOfSamplesEachVar * numberIterations

# Test if the selected sample size is valid - The modulo of populationSize/sampleSize should be 0
if (populationSize %% sampleSizePerFile == 0) {
  cat(sprintf("The selected sample size will provide %i samples per file, and a total of %i samples", numberIterations, totalSamples))
  cat(sprintf("\n Each variable will be included in %i samples", numberOfSamplesEachVar))
} else {
  cat("Change the sample size. The modulo is not 0")
}


# A list with the parameters for the payload function getFeatureSelectionModel(), which for this case is EaaLasso
appConfig <- list(
  alpha = 1,                       #
  nfolds = 10,                     #
  lambda = NULL,                   #
  coefficientThreshold = 0.1e-5,   #
  rootDir = basename(getwd())      #
)

# Create data partitions by sampling from the main analytical file
# The partitions need to be in a list, the components of each will be sent to the workers
dataPartitions <- createSamples(df = populationDf,
                                target = "target",             # For Brent forecasts this should be v134253300
                                numberOfSamplesEachVar = numberOfSamplesEachVar,
                                numberIterations = numberIterations,
                                sampleSizePerFile = sampleSizePerFile)

sprintf("The number of data partitions to apply the payload function on is %i", length(dataPartitions))
#str(dataPartitions)

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

####################
## !! VERY IMPORTANT TO STOP THE SNOW CLUSTER !!! ##
####################

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




