#This function is to find the optimal order of integration for diff(time series)
#and transform the original time series to stationary data
#Input: A Time series vector
#Output: A differenced time series vector of the same lenth using the optimal order of interation
#Author: Liang Kuang
#Date: 2017-04-26
#example: dfOut <- apply(dfIn,2,OptimalDiff, alpha = 0.05, testMethod = c('kpss'))

OptimalDiff <- function(tsIn, alpha = 0.05, testMethod = c('kpss')){
  # load libraries
  wants <- c("forecast")
  has   <- wants %in% rownames(installed.packages())
  if(any(!has)) install.packages(wants[!has])
  sapply(wants, require, character.only = TRUE)
  rm("wants","has")
  
  nd <- ndiffs(tsIn, alpha = alpha, test = testMethod)
  # difference time series
  if(nd !=0) {
    tsOut <- diff(tsIn, lag = nd)
  }else{
    tsOut <- tsIn
  }
 return(tsOut) 
}