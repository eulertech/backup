# This function  is intended to compute the porosity of a time series vector.
# The computed porosicy(completeness) can be then used to screen feature variables in a dataframe.
# This function find the blocks of mimssing data and track the size of each block
#
# Input: Time Sereis Vector
#        tolerance: default 1 discrete missing value
#        Missing Value: NA or 0 or user specified (e.g. -99999 )
#        batch: when used in apply function, set it to TRUE and only adjuested.porosity will be generated. 
# Output: A list contains:
#         1. total.porosity.score 2. adjusted.porosity.score (recommended) 3. missing.blocksize
#
# e.g.
# > a <-  c(1,2,NA,3,NA,NA,4,5,6,7,8,NA,9,10,NA,NA)
# > result <- Porosity(a)
# > result$adjusted.porosity.score
# > result$total.porosity.score
#
# for dataframe usage. e.g. apply(dfIn,2,PorosityScore,tolerance=1,batch=TRUE)
#
# Author: Liang Kuang
# Date: 2017-04-26

PorosityScore<- function(tsIn,tolerance = 1,missingValue = NA,batch = FALSE){
  #tsIn <- c(1,2,NA,3,NA,NA,4,5,6,7,8,NA,9,10,NA,NA)
  
  mVal = -99999999.9999 
  if(is.na(missingValue)) {
    tsIn[is.na(tsIn)] <- mVal
  }else{
    mVal = missingValue
  }
  idx <- which(tsIn == mVal )
  # Compute the total sparsity of the data
  totalPorosity <- length(idx) / length(tsIn)
  
  result <- list() 
  
  count <- 0
  i = 1
  while(i <= length(tsIn)) {
    if(tsIn[i] == mVal){
      count <-  count + 1
    }else{
      if(count !=0){
        result <- append(result,count)
        }
      count <- 0
    } 
    
    i <-  i +1
  } 
  
  if(count !=0) {
    result <- append(result,count)
  } 
  
  if(length(result) ==0){
      adjPorosity <- 0
      blockSizeVec <- NA
      sprintf("The average porosity is: %5.1f.", mean(blockSizeVec))
      sprintf("The total and adjusted porosity score is:(%5.1f , %5.1f)", totalPorosity,adjPorosity)
      resultlist <-  list("total.porosity.score" =  totalPorosity ,"adjusted.porosity.score" = adjPorosity, 
                  "missing.blocksize" = blockSizeVec) 
  }else{
      # convert it to a vector
      blockSizeVec <- sapply(result,sum) # Map OF number of missing value in each missing blocks
     # If the spacing of the missing data is continous (>1), bad (e.g. [2,3,3,4,4,1,1,5,6,6])
      AvgPorosity <- mean(blockSizeVec)                            # The smaller,  the better
     # adjusted porosity score
      resVecAdj <- blockSizeVec[blockSizeVec>=tolerance] 
      adjPorosity <- sum(resVecAdj)/length(tsIn) 
      sprintf("The average porosity is: %5.1f.", mean(blockSizeVec))
      sprintf("The total and adjusted porosity score is:(%5.1f , %5.1f)", totalPorosity,adjPorosity)
      resultlist <-  list("total.porosity.score" =  totalPorosity ,"adjusted.porosity.score" = adjPorosity, 
                  "missing.blocksize" = blockSizeVec) 
  }
  
 if(batch <- TRUE) {
  # for using with apply function 
  # only return adjusted porosity since total porosity is too easy to compute
    return(adjPorosity)
 }else{
    return(resultlist) 
 }  
}