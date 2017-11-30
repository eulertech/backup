rm(list=ls())

# On a clean machine install the following packages.  R has a bug in that it installs the packages
# if they don't exist but does not load them successfully the first time
reqPackages <- c("jsonlite")
lapply(reqPackages, function(x) if(!require(x, character.only = TRUE)) install.packages(x, repos = 'http://cran.us.r-project.org'))
rm(reqPackages)
library(jsonlite)

# Process each file and create csv files for attributes and data
ProcessFile <- function(dataDirectory, filePrefix, attrsOrder) {
  inputFile <- file.path(dataDirectory, paste0(filePrefix, ".json"))
  message(Sys.time(), " - Processing input file: ", inputFile, ".  Please wait...")
  
  inputData <- fromJSON(inputFile)

  # Extract the series attributes and save to file
  seriesAttributes <- inputData
  seriesAttributes <- seriesAttributes[ , -which(names(seriesAttributes) %in% c("data"))]
  seriesAttributes <- seriesAttributes[c(unlist(as.list(strsplit(attrsOrder, ",")[[1]]), recursive = TRUE, use.names = TRUE))]
  outputFile <- file.path(dataDirectory, paste0(filePrefix, "_series_attributes.txt"))
  write.table(seriesAttributes, file = outputFile, sep = "|", row.names = FALSE, col.names = FALSE)
  
  # Extract the series data and save to file
  listOfSeries <- apply(inputData, 1, function(x) {data.frame(series_id=x$series_id, key=x$data[,1], value=as.numeric(x$data[,2]))})
  outputFile <- file.path(dataDirectory, paste0(filePrefix, "_series_data.txt"))
  if (file.exists(outputFile)) file.remove(outputFile)
  ret <- lapply(listOfSeries, function(series) write.table(series, file = outputFile, append = TRUE,
    sep = "|", row.names = FALSE, col.names = FALSE, na = "NULL"))
  
  message(Sys.time(), " - Done.")
}

# dataDirectory <- "E:/Work/Big Data Analytics/Opportunities/AbuDhabi/EIA"
# filePrefixes <- c("STEO", "PET_IMPORTS", "PET")
# ret <- sapply(filePrefixes, function(filePrefix) ProcessFile(dataDirectory, filePrefix))

args = commandArgs(trailingOnly=TRUE)

fileName = "C:/Users/zik40226/workspace/PythonRedshift/Temp/STEO.json" # for testing
attrsOrder = "series_id,name,units,f,copyright,source,geography,start,end,lastHistoricalPeriod,last_updated,description" # for testing

if(length(args)>0)
    fileName = args[1]
    attrsOrder = args[2]

library(tools)
dataDirectory <- dirname(fileName)
filePrefix <- file_path_sans_ext(basename(fileName))
ProcessFile(dataDirectory, filePrefix, attrsOrder)
