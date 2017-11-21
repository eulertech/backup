
#######################################################################
#
# Purpose: Code to pull the data series and attributes from Redshift
#          in general. The example here pulls data from Hindsight
#
# The code makes use of the AACloudTools package
#
#
# Author: Valentin Todorov
#
#######################################################################


#######################################################################
#
# Important: Before you can use this code, you will need do two things:
# 1. Add the username and password to the redshift_username_password.JSON file located in your project directory
# For example, if the name of the project is EAA_Analytics, the JSON will be located in /home/valentint/EAA_Analytics/Config
#
#
# 2. Create two folders in your root directory called [project name]_Data and [project name]_Data/Temp
# For example, if the project is called EAA_Analytics, the folders will be:
#     /home/valentint/EAA_Analytics_Data
#     /home/valentint/EAA_Analytics_Data/Temp
#
#######################################################################



# Increase size of memory for Java
options(java.parameters = "-Xmx16384m")


rm(list=ls())

# Install the needed packages and load them in the current session
wants <- c("AACloudTools", "reshape2", "stringr", "lubridate")
neededPackages <- wants %in% rownames(installed.packages())
if(any(!neededPackages)) install.packages(wants[!neededPackages])
sapply(wants, require, character.only = TRUE)

library("AACloudTools")
library("reshape2")
library("stringr")
library("lubridate")


# Enter your username and project name
myRedshiftUsername <- "valentint"
projectName <- "EAA_Analytics"

# Select data frequency - Applicable only for Hindsight
dataFrequency <- "MONT"


# Point to the JSON location
configJSONlocation <- paste("home", myRedshiftUsername, projectName, sep = "/")

# Configure the AWS services
ConfigureAWS(paste0("/", configJSONlocation, "/Config/config.json", collapse = ""))

# Get connection details to pass to the querries
myConn <- GetRedshiftConnection()


# Pull the IDDS data attributes table
iddsDataAttributes <- SqlToDf("SELECT * FROM eaa_analysis.idds_attributes_subset")
iddsDataAttributes$startdate_year <- year(mdy(iddsDataAttributes$startdate))
iddsDataAttributes$enddate_year <- year(mdy(iddsDataAttributes$enddate))

# Analysis of the data
# Remove series with (1) start date after 2000, and (2) end date prior to 2010 and archived series
table(iddsDataAttributes$startdate_year)


table(iddsDataAttributes[iddsDataAttributes$startdate_year < 2001 &
                           iddsDataAttributes$enddate_year > 2015 &
                           iddsDataAttributes$seriestype != 'ARCH', ]$startdate_year)

sum(table(iddsDataAttributes[iddsDataAttributes$startdate_year < 2001 &
                               iddsDataAttributes$enddate_year > 2015 &
                               iddsDataAttributes$seriestype != 'ARCH', ]$startdate_year))


# Pull the IDDS attributes and data tables
iddsDataAttributes <- iddsDataAttributes[iddsDataAttributes$startdate_year < 2001 &
                                           iddsDataAttributes$enddate_year > 2015 &
                                           iddsDataAttributes$seriestype != 'ARCH', ]

xVars <- paste0(iddsDataAttributes[, "series_id"], collapse = ",")
length(strsplit(xVars, split = ',')[[1]])


# (!) This process consumes significant resources
iddsData <- SqlToDf(paste0("SELECT series_id, date, datavalue
                              FROM eaa_analysis.idds_data
                              WHERE series_id IN (", xVars, ")", collapse = ""))

# Transpose the data from long to wide
iddsData <- iddsData[year(as.Date(iddsData$date)) >= 1990, ]






# Transpose the data from long to wide
iddsData$seriesIdModified <- paste0("v", iddsData$series_id)
iddsDataDt <- dcast(iddsData[, c("date", "datavalue", "seriesIdModified")],
                    date ~ seriesIdModified, value.var = "datavalue")

# Drop the single columns which has a series_id long 11 char
iddsDataDt <- iddsDataDt[, colnames(iddsDataDt) != "v1.56e+08"]



# Save the tables to Redshift
CreateTableInRedShift("eaa_analysis.idds_data_subset", iddsData, TRUE)
UploadTableToRedshift(iddsData, "eaa_analysis.idds_data_subset", TRUE)




