
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


rm(list = ls())

# Install the needed packages and load them in the current session
# AACloudTools can be installed via the AASpectre distribution

library(AACloudTools)

wants <- c("reshape2", "stringr", "lubridate")
neededPackages <- wants %in% rownames(installed.packages())
if(any(!neededPackages)) install.packages(wants[!neededPackages])
sapply(wants, require, character.only = TRUE)

# Enter your username and project name
myRedshiftUsername <- "valentint"
projectName <- "EAA_Analytics"


# Point to the JSON location
configJSONlocation <- paste("home", myRedshiftUsername, projectName, sep = "/")

# Configure the AWS services
ConfigureAWS(paste0("/", configJSONlocation, "/Config/config.json", collapse = ""))

# Get connection details to pass to the querries
myConn <- GetRedshiftConnection()


# Select data frequency - Applicable only for Hindsight
dataFrequency <- "MONT"

# Pull the IDDS data attributes table
iddsDataAttributes <- SqlToDf(paste0("SELECT series_id, series_key, mnemonic_source, mnemonic, dri_mnemonic,
                                        wefa_mnemonic, frequency, seriestype, startdate, enddate,
                                        shortlabel, longlabel, concept, geo, unit, industry, realnominal,
                                        seasonaladjustment, source, bank
                                      FROM hindsight_prod.series_attributes
                                        WHERE frequency IN ('", dataFrequency, "')", collapse = "")
                              )

iddsDataAttributes$startdateYear <- year(mdy(iddsDataAttributes$startdate))


# Pull the IDDS data attributes table
xVars <- paste0(iddsDataAttributes[, "series_id"], collapse = ",")
iddsData <- SqlToDf(paste0("SELECT *
                              FROM hindsight_prod.series_data
                                WHERE series_id IN (", xVars, ")", collapse = ""))


# Save the tables to Redshfit
# Tables saved to AWS can have at most 1600 columns !!!
CreateTableInRedShift("eaa_analysis.idds_attributes", iddsDataAttributes, TRUE)
UploadTableToRedshift(iddsDataAttributes, "eaa_analysis.idds_attributes", TRUE)

CreateTableInRedShift("eaa_analysis.idds_data", iddsData, TRUE)
UploadTableToRedshift(iddsData, "eaa_analysis.idds_data", TRUE)














