#########################
# 
# This function loads all required packages
#
#
#########################


# Install the needed packages
wants <- c("glmnet", "bsts", "tseries", "lubridate", "stringr", "plyr", "dplyr", "randomForest",
           "data.table", "forecast", "tidyr", "prophet", "ggplot2",
           "doSNOW", "doParallel", "caret", "beepr", "mlbench", "corrplot", "MASS", "nnet",
           "foreach", "zoo", "reshape2", "devtools", "doBy", "rgp", "e1071")
neededPackages <- wants %in% rownames(installed.packages())
if(any(!neededPackages)) install.packages(wants[!neededPackages])
sapply(wants, require, character.only = TRUE)

rm("wants", "neededPackages")

