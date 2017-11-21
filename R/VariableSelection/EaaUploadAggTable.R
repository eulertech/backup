# This function takes a dataframe and a user defined name and uploads it to the eaa_analysis schema in Redshift
# Input: any dataframe
# Output: none in R, dataframe uploaded to Redshift
# Author: Lou Zhang

wants <- c("AASpectre", "AACloudTools", 'varhandle')
has   <- wants %in% rownames(installed.packages())
if (any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")


EaaUploadAggTable <- function(df, name) {
  
  dataRaw <- varhandle::unfactor(df)
  
  if('description' %in% colnames(dataRaw)){
  
  rowWithInvalidChars <-
    dataRaw[sapply(dataRaw$description, 
                   function(x) {is.na(utf8ToInt(x)[1])},
                   USE.NAMES = FALSE), c("variableID", "description")]
  
  rowWithInvalidChars$description <- 
    iconv(rowWithInvalidChars$description, from = "latin1", to = "utf8", sub = "?")
  
  dataRaw$description[dataRaw$variableID %in% rowWithInvalidChars$variableID] <- rowWithInvalidChars$description
  
  }
  
  #  Sets up the Logger.  'Logger' is a global variable
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
  featSelTable <- paste("eaa_analysis.", name, sep= "")
  CreateTableInRedShift(featSelTable, dataRaw, recreate = TRUE)
  UploadTableToRedshift(dataRaw, featSelTable, truncate = TRUE)
  
}
