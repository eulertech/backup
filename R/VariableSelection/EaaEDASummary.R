# This function takes a dataframe and returns several key EDA measures, 
# including information about missing values start/end dates if available, and degrees of autocorrelation and seasonality
# Input: dataframe with columns as variables
# Output: dataframe with variables and their EDA characteristics
# Author: Lou Zhang

EaaEDASummary <- function(df) {
  
  setwd("~/EAA_Analytics/Development/VariableSelection")
  
  source("EaaPorosityScore.R")
  
  #  load libraries
  wants <- c("forecast", "lattice")
  has   <- wants %in% rownames(installed.packages())
  if(any(!has)) install.packages(wants[!has])
  sapply(wants, require, character.only = TRUE)
  rm("wants", "has")
  
  # vlookup function
  vlookup <- function(ref,                # the value or values that you want to look for
                      table,              # the table where you want to look for it; will look in first column
                      column,             # the column that you want the return data to come from,
                      range = FALSE,        # if there is not an exact match, return the closest?
                      larger = FALSE)       # if doing a range lookup, should the smaller or larger key be used?)
  {
    if(!is.numeric(column) & !column %in% colnames(table)) {
      stop(paste("can't find column", column, "in table"))
    }
    if(range) {
      if(!is.numeric(table[,1])) {
        stop(paste("The first column of table must be numeric when using range lookup"))
      }
      table <- table[order(table[,1]),] 
      index <- findInterval(ref,table[,1])
      if(larger) {
        index <- ifelse(ref %in% table[,1],index,index+1)
      }
      output <- table[index,column]
      output[!index <= dim(table)[1]] <- NA
      
    } else {
      output <- table[match(ref,table[,1]),column]
      output[!ref %in% table[,1]] <- NA # not needed?
    }
    dim(output) <- dim(ref)
    output
  }
  
  # initialize EDA data frame
  EDASummary <- as.data.frame(c(1:ncol(df)))
  
  # compute number of zeroes for each variable
  for(i in 1:ncol(df)) {
    
    EDASummary$variableID[i] <- colnames(df[i])
    EDASummary$numZeroes[i] <- sum(df[[i]] == 0)
    
  }
  
  # compute maximum number of consecutive zeroes
  for(i in 1:ncol(df)) {
    
    y <- rle(df[[i]])
    EDASummary$consecZeroes[i] <- max(y$lengths[y$values == 0])
    
  }
  
  # compute percent missing overall
  for(i in 1:ncol(df)) {
    
    EDASummary$percentMissing[i] <- round(EDASummary$numZeroes[i]/nrow(df), 6)
    
  }
  
  # lookup start date
  if(exists(x = "reftable") && is.data.frame(get("reftable"))) {
    
    for(i in 1:ncol(df)) {
      
      EDASummary$startDate[i] <- vlookup(ref = EDASummary$variableID[i], table = reftable, column = 8)
      
    }
    
    EDASummary$startDate <- as.Date(EDASummary$startDate)
    
  }
  
  # lookup end date
  if(exists(x = "reftable") && is.data.frame(get("reftable"))) {
    
    for(i in 1:ncol(df)) {
      
      EDASummary$endDate[i] <- vlookup(ref = EDASummary$variableID[i], table = reftable, column = 9)
      
    }
    
    EDASummary$endDate <- as.Date(EDASummary$endDate)
    
  }
  
  # compute forecast length duration
  if(exists(x = "reftable") && is.data.frame(get("reftable"))) {
    
    EDASummary$forecastLengthYears <- 0
    
    for(i in 1:ncol(df)) {
      
      if(is.na(EDASummary[i,]$startDate) == FALSE) {
        
        EDASummary$forecastLengthYears[i] <- (EDASummary$endDate[i] - today())/365
        
      }
    }
  }
  
  # compute optimal differences
  for(i in 1:ncol(df)) {
    
    if(EDASummary$percentMissing[i] < 1) {
      
      EDASummary$optimalDiff[i] <- ndiffs(df[[i]])
      
    }
    
    if(EDASummary$percentMissing[i] == 1) {
      
      EDASummary$optimalDiff[i] <- NA
      
    }
  }
  
  # compute cyclicality
  for(i in 1:ncol(df)) {
    
    if(EDASummary$percentMissing[i] < 1) {
      
      EDASummary$Periodicity[i] <- findfrequency(df[[i]])
      
      if(EDASummary$Periodicity[i] > 60) {
        
        EDASummary$Periodicity[i] <- 1
        
      }
    }
    
    if(EDASummary$percentMissing[i] == 1) {
      
      EDASummary$Periodicity[i] <- 1
      
    }
  }
  
  # compute porosity score
  df2 <- df
  df2[df2 == 0] <- NA
  
  for(i in 1:ncol(df)) {
    
    EDASummary$Porosity[i] <- EaaPorosityScore(df2[[i]])$adjusted.porosity.score
    
  }
  
  EDASummary$`c(1:ncol(df))` <- NULL
  EDASummary$consecZeroes[is.infinite(EDASummary$consecZeroes)] <- 0
  EDASummary$robustnessScore <- 2/(EDASummary$percentMissing+1) + 2/(EDASummary$consecZeroes + 1) + 2/(EDASummary$Porosity + 1)
  
  
  # create visualizations
  numZeroesGraph <- histogram(EDASummary$numZeroes, main = "Distribution of Number of Missing Values")
  consecZeroesGraph <- histogram(EDASummary$consecZeroes, main = "Distribution of Consecutive Missing Values")
  optimalDiffGraph <- histogram(EDASummary$optimalDiff, main = "Distribution of Optimal Differencing")
  periodicityGraph <- histogram(EDASummary$Periodicity, main = "Distribution of Periodicity")
  porosityGraph <- histogram(EDASummary$Porosity, main = "Distribution of Porosity")
  forecastLengthGraph <- histogram(EDASummary$forecastLengthYears, main = "Distribution of Length of Forecast (Years)")
  robustnessGraph <- histogram(EDASummary$robustnessScore, main = "Distribution of Robustness Score")
  
  return(list(EDASummary, numZeroesGraph, consecZeroesGraph, optimalDiffGraph, periodicityGraph, porosityGraph, forecastLengthGraph, robustnessGraph))
  
}

