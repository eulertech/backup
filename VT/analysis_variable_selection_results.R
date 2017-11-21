
## This is analysis of the data from the run on Saturday 10/30 (5:30pm)

library(ggplot2)
library(AACloudTools)

# Load configuration for AWS services
awsConfig <- AACloudTools::ConfigureAWS("./Config/config.json")

##
numberOfSamplesEachVar <- 1000    # Number of times to reshuffle the training file
sampleSizePerFile <- 500          # Number of samples to be drawn from the training file
sampledTrainingFileSize <- 21500
cat(sprintf("The variable selection file has %i variables including the target variable", sampledTrainingFileSize))

# The results are saved in an S3 bucket
AACloudTools::LoadFromS3("s3://ihs-bda-data/projects/EAA/Models/combinedPredictors_lasso.RData")

# Downlod the file to ecamine it
write.csv(combinedPredictors, "/home/valentint/EAA_Analytics/Personal/VT/combinedPredictors_lasso.csv")

combinedPredictorsSummary <- data.frame(table(combinedPredictors$predictors))
names(combinedPredictorsSummary) <- c("predictors", "inclusionFrequency")

# Data transformations and prepare the file for plotting
# The aggregate() is a working patch right now - it needs to be reworked
combinedPredictorsSummary$pctInclude <- combinedPredictorsSummary$inclusionFrequency / numberOfSamplesEachVar
combinedPredictorsSummary$pctIncludeGt60 <- ifelse(combinedPredictorsSummary$pctInclude >= 0.6, 1, 0)
combinedPredictorsSummary <- merge(x = combinedPredictorsSummary,
                                   y = aggregate(ind ~ predictors + series_id + shortlabel,
                                                 data.frame(combinedPredictors, ind = 1),
                                                 FUN = length),
                                   by.x = "predictors",
                                   by.y = "predictors",
                                   all.x = TRUE)

combinedPredictorsSummary <- combinedPredictorsSummary[order(-combinedPredictorsSummary$pctInclude), ]
combinedPredictorsSummary$predictors <- factor(combinedPredictorsSummary$predictors, levels = unique(combinedPredictorsSummary$predictors))
write.csv(combinedPredictorsSummary, "/home/valentint/EAA_Analytics/Personal/VT/combinedPredictorsSummary_lasso.csv")


# Plot results
plotFeatures <- ggplot2::ggplot(combinedPredictorsSummary, aes(x = predictors, y = pctInclude, group = 1)) +
                                geom_line() +
                                theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
                                ylab(label = "Inclusion probability") +
                                xlab("Predictors")
ggplot2::ggsave("/home/valentint/EAA_Analytics/Personal/VT/combinedPredictorsSummaryFull.jpg",
                plot = plotFeatures,
                height = 15,
                width = 30)




