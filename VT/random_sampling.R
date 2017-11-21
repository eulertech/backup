#####################################
#
# This is a function to do random sampling with replacement
# Author: Valentin Todorov
#
# Purpose: This function samples data from a dataframe with replacement
# Input: A dataframe whose first column is the target which we predict
# Output: A dataframe with randomly selected columns. The first column is still the target variable
#
#####################################


DataSample <- function(dataframe,
                       sampleSize,
                       replace = TRUE) {
 
  # Draw a random sample from the input dataframe 
  df_sample <- sample(dataframe[, 2:ncol(dataframe)],
                      size = sampleSize,
                      replace = TRUE,
                      prob = NULL)
  
  # Create the output data frame. Attach the target variable as the first column
  df_sample <- data.frame(dataframe[1], df_sample)
  names(df_sample)[1] <- "target"
  
  return (df_sample)
}


