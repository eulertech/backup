

source("./Personal/VT/create_synthetic_data.R")
source("./Development/VariableSelection/EaaLASSO_v2.R")


# Load AWS configuration to use AWS services
AACloudTools::ConfigureAWS(paste0(getwd(), "/Config/config.json", collapse = ""))
myConn <- AACloudTools::GetRedshiftConnection(url = Sys.getenv("PGURLRS"))

# Load older data saved to S3
AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampled.RData")
sampleDf <- iddsDataDtSampled[, !colnames(iddsDataDtSampled) %in% c("date")]

# Scale the data, remove columns which have NAs and select only the number of columns with a modulo 0 when divided by 500
sampleDfScaled <- as.data.frame(apply(sampleDf, 2, function(x) (x - mean(x)) / sd(x)))
sampleDfScaled <- sampleDfScaled[, colSums(is.na(sampleDfScaled)) == 0]
sampleDfScaled <- sampleDfScaled[1:21000]


# Create a data frame in which the first column is the target, the next 5 are the columns used to create the target and the remaining are columns from the original data frame
# The columns used to create the target are removed from the data frame
sim <- simulateTarget(sampleDfScaled, seed = runif(1, 1000, 9999), nVarsToCreateTarget = 5)
sampleDfScaled$target  <- sim[[1]]
sampleDfScaled$drivers <- sim[[2]]
plot.ts(cbind("target" = sampleDfScaled$target, sampleDfScaled$drivers), main = "Target & Drivers")

dfSynthetic <- as.data.frame(cbind(sim,
                                   sampleDfScaled[, colnames(sampleDfScaled) %in% setdiff(colnames(sampleDfScaled), c("target", "drivers", names(sim[[2]])))]))



###########################################################
#
# Run variable selection using the parallel process
#
###########################################################


# Analysis of results
dfSyntheticResults <- as.data.frame(table(combinedPredictors$predictors))
names(dfSyntheticResults)[1] <- "predictors"
dfSyntheticResults$pctInclusionFrequency <- dfSyntheticResults$Freq / 100
dfSyntheticResults <- dfSyntheticResults[order(-dfSyntheticResults$pctInclusionFrequency), ]

syntheticDrivers <- dfSyntheticResults[grepl("drivers.", dfSyntheticResults$predictors), ]


dfSyntheticResults$predictors <- factor(dfSyntheticResults$predictors, levels = unique(dfSyntheticResults$predictors))



write.csv(dfSyntheticResults, "./Personal/VT/synthetic_results.csv")

ggplot2::ggplot(dfSyntheticResults, aes(x = predictors, y = pctInclusionFrequency, group = 1)) +
  geom_line() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  ylab(label = "Inclusion probability") +
  xlab("Predictors")



tgp::friedman.1.data()



