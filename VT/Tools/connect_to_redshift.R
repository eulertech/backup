
#################################################
#
# Code to establish connection to Redshift
#
#################################################


library(AACloudTools)

# Point to the JSON location
configJSONlocation <- "/home/valentint/AASpectreQuickStartTemplate/"

# Set the working directory
setwd(configJSONlocation)

# Configure the AWS services
ConfigureAWS(paste0(configJSONlocation, "./Config/config.json", collapse = ""))

# Get connection details to pass to the querries
myConn <- GetRedshiftConnection()


######### Examples
# Get top 100 records from a Redshift table
mydf <- dbGetQuery(myConn, "select top 10 *
                            from hindsight_prod.series_attributes
                            where frequency in ('MONT', 'QUAR')")

# Create a dataframe from a querry
iddsData <- SqlToDf(c("select top 10 *",
                      "from hindsight_prod.series_attributes",
                      "where frequency in ('MONT', 'QUAR')"))


