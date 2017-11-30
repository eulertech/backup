# Process the Totem XML file
rm(list=ls())

# On a clean machine install the following packages.  R has a bug in that it installs the packages
# if they don't exist but does not load them successfully the first time
reqPackages <- c("XML", "dplyr")
lapply(reqPackages, function(x) if(!require(x, character.only = TRUE)) install.packages(x, repos = 'http://cran.us.r-project.org'))
rm(reqPackages)

library(XML)
library(dplyr)

ProcessListObject <- function(node) {
  # If the node is a list, recursively process the list and flatten it.  dfKids is a flattened data frames
  dfKids <- bind_rows(lapply(node, function(nodeItem) { if (typeof(nodeItem) == "list") ProcessListObject(nodeItem)}))
  
  # Process the elements at the root level.
  lst <- unlist(lapply(node, function(nodeItem) { if (typeof(nodeItem) != "list") nodeItem}))

  dfRoot <- dfKids # Assume there is only data from kids
  if (length(lst)>0) {
    # If there is data at the root level, use that first
    dfRoot <- data.frame(t(unlist(lst)), stringsAsFactors=FALSE) # Remove NULLS, transpose and create data frame
    if (nrow(dfKids)>0)
      dfRoot <- cbind(dfRoot, dfKids) # If there is data from kids, add to the data frame
  }  

  dfRoot
}

# Process each file and create csv files for attributes and data
ProcessFile <- function(dataDirectory, filePrefix) {
  inputFile <- file.path(dataDirectory, paste0(filePrefix, ".xml"))
  message(Sys.time(), " - Processing input file: ", inputFile, ".  Please wait...")
  
  xmlfile <- xmlTreeParse(inputFile, useInternalNodes = TRUE)
  class(xmlfile)
  topxml <- xmlRoot(xmlfile)
  nodeList <- xmlApply(topxml, xmlToList)
  df <- ProcessListObject(nodeList)
  
  outputFile <- file.path(dataDirectory, paste0(filePrefix, ".txt"))
  write.table(df, file = outputFile, sep = "|", row.names = FALSE)
  message(Sys.time(), " - Done processing file.")
  
  df
}

args = commandArgs(trailingOnly=TRUE)

fileName = "E:/Work/Big Data Analytics/Opportunities/AbuDhabi/Totem/113500_DD_20150824_Commodities_Oil_RESULTS_10.xml" # for testing
#fileName = "E:/Work/Big Data Analytics/Opportunities/AbuDhabi/Totem/test.xml"
if(length(args)>0)
  fileName = args[1]

library(tools)
dataDirectory <- dirname(fileName)
filePrefix <- file_path_sans_ext(basename(fileName))
df <- ProcessFile(dataDirectory, filePrefix)