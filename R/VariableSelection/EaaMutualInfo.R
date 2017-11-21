# This function takes a data frame and returns a mutual information coefficient for each variable
# You can also specify the number of bins you want your data discretized into
# Input: dataframe with columns as variables and target as first column
# Output dataframe with mutual information for each variable
# Author: Lou Zhang

# Load drivers
wants <- c("infotheo")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

EaaMutualInfo <- function(df, bins = nrow(df)^(1/3)) {
  
  range01 <- function(x){
    
    (x-min(x))/(max(x)-min(x))
    
  }
  
  predictors <- discretize(df[2:ncol(df)],nbins = bins)
  
  colnames(predictors) <- colnames(df[,2:ncol(df)])
  target <- discretize(df[1], nbins = bins)
  
  mi <- sapply(1:(ncol(df)-1), function(i) mutinformation(predictors[i],target))
  MutInfo <- as.data.frame(mi, row.names = colnames(df)[2:ncol(df)])
  
  colnames(MutInfo) <- c("MutualInformation")
  MutInfo$Scaled <- range01(MutInfo$MutualInformation)
  return(MutInfo)
  
}


