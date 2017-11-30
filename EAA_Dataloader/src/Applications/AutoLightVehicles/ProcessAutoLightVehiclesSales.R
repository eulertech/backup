rm(list=ls())

# Process each file and create csv files for attributes and data
ProcessFile <- function(dataDirectory, filePrefix) {
  inputFile <- file.path(dataDirectory, paste0(filePrefix, ".txt"))
  message(Sys.time(), " - Processing input file: ", inputFile, ".  Please wait...")
  
  inputData <- read.table(inputFile, skip = 4, header = TRUE, sep = ",")

  # Extract the series attributes and save to file
  seriesAttributes <- inputData
  
  # Remove the time series columns.  Instead they get stored in a separate series data file
  #seriesAttributes <- seriesAttributes[ , -(grep("CY.",names(seriesAttributes)))]
  
  outputFile <- file.path(dataDirectory, paste0(filePrefix, "_series_attributes.txt"))
  write.table(seriesAttributes, file = outputFile, sep = "|", row.names = FALSE)
  
  # Extract the series data and save to file
  # listOfSeries <- apply(inputData, 1, function(x) {data.frame(series_id=x$series_id, key=x$data[,1], value=as.numeric(x$data[,2]))})
  # outputFile <- file.path(dataDirectory, paste0(filePrefix, "_series_data.csv"))
  # if (file.exists(outputFile)) file.remove(outputFile)
  # ret <- lapply(listOfSeries, function(series) write.table(series, file = outputFile, append = TRUE,
  #   sep = ",", row.names = FALSE, col.names = FALSE, na = "NULL"))
  
  message(Sys.time(), " - Done.")
}

args = commandArgs(trailingOnly=TRUE)

fileName = "C:/Users/zik40226/workspace/PythonRedshift/Temp/IHS+LV_Sales_Base_Global_2016M12.txt" # for testing
if(length(args)>0)
  fileName = args[1]

library(tools)
dataDirectory <- dirname(fileName)
filePrefix <- file_path_sans_ext(basename(fileName))
ProcessFile(dataDirectory, filePrefix)
