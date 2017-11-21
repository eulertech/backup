

library(AACloudTools)

object.size(c(iddsData, iddsDataDt, iddsDataDtSampledTraining))


# Increase size of memory for Java
options(java.parameters = "-Xmx8192m")


# Configure the AWS services
AACloudTools::ConfigureAWS(paste0(getwd(), "/Config/config.json", collapse = ""))
myConn <- AACloudTools::GetRedshiftConnection(url = Sys.getenv("PGURLRS"))

AACloudTools::LoadFromS3(s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampledTraining.RData")


# Sources of Hindsight data
hindsightSeriesSource <- "hindsight_prod.series_data"
hindsightAttributesSource <- "hindsight_prod.series_attributes"

series_needed <- paste(gsub("v", "", names(iddsDataDtSampledTraining)), collapse = ",")
write.csv(series_needed, "/home/valentint/EAA_Analytics/Personal/VT/_seris_needed.csv")

a <- Sys.time()
## Pull data from Hindsight - only keep the years for the analysis:
iddsData <- AACloudTools::SqlToDf(paste0("SELECT *
                                          FROM eaa_analysis.idds_data_analysis"))

Sys.time() - a

# Create a character series_id and transpose the data from long to wide to use for variable selection
# The WHERE condition in the SqlToDf statement above creates rows with all NAs (!!???). The patch to remove them is iddsData[iddsData$seriesIdModified != "vNA", ]
iddsData$seriesIdModified <- paste0("v", iddsData$series_id)
iddsData <- iddsData[iddsData$seriesIdModified != "vNA", ]
iddsDataDtSampled <- reshape2::dcast(iddsData[, c("date", "datavalue", "seriesIdModified")],
                                     date ~ seriesIdModified, value.var = "datavalue")

# Check if the column names are the same
setdiff(names(iddsDataDtSampledTraining), names(iddsDataDtSampled))
setdiff(names(iddsDataDtSampled), names(iddsDataDtSampledTraining))
table(names(iddsDataDtSampled) %in% names(iddsDataDtSampledTraining))


AACloudTools::SaveToS3(iddsDataDtSampled, s3File = "s3://ihs-bda-data/projects/EAA/Models/iddsDataDtSampled.RData")





