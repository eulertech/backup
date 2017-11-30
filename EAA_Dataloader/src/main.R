#########################################################################
#     Main.R script to run ALL SQL and R scripts                        #
#     Date :   6/7/2016                                                 #
#                                                                       #
#     NOTE: You will need to install PostGres psql to run this code     #
#           http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-from-psql.html
#                                                                       #
#     Author: Christopher Lewis                                         #
#                                                                       #
#########################################################################

# Clean the working environment 
rm(list=(ls(all=TRUE)))
gc()
cat("\014") # Clear output window

# On a clean machine install the following packages.  R has a bug in that it installs the packages
# if they don't exist but does not load them successfully the first time
reqPackages <- c("jsonlite")
lapply(reqPackages, function(x) if(!require(x, character.only = TRUE)) install.packages(x, repos = 'http://cran.us.r-project.org'))
rm(reqPackages)

workingDirBaseName <- "src"
if (basename(getwd()) != workingDirBaseName)
    setwd("src")
    #stop(paste0("Set working directory to: ", workingDirBaseName, ". You current directory path is: ", getwd()))
# Make sure "python" is in the path
system("python Main.py")