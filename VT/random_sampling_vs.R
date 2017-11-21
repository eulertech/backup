#####################################
#
# This is a function to do random sampling without replacement
# Author: Valentin Todorov
#
# Purpose: This function samples data from a dataframe with replacement
# Input: A dataframe whose first column is the target which we predict
# Output: A dataframe with randomly selected columns. The first column is still the target variable
#
#####################################


createSamples <- function(df, target, numberOfSamplesEachVar, numberIterations, sampleSizePerFile) {
  dp_list <- list()
  
  for (s in 1:numberOfSamplesEachVar) {
    target <- df[, colnames(df) %in% target]
    df_no_target <- df[2:length(df)]
    
    # Create a list with the variables in the dataframe and randomize their ordering
    listPredictors <- sample(colnames(df_no_target))
    
    # After each sampling iteration, remove from the population the vector the sampled predictors
    for (i in (1:numberIterations)) {
      listSelectedPreds <- sample(listPredictors,
                                  size = sampleSizePerFile,
                                  replace = FALSE)
      randomDf <- data.frame(target,
                             df_no_target[, names(df_no_target) %in% listSelectedPreds])
      dp_list[[paste("Sample_", s, "_", i, sep = "")]] <- randomDf
      
      # Remove from the list of predictors those that have already been selected for a sample
      listPredictors <- setdiff(listPredictors, listSelectedPreds)
    }
  }
  return(dp_list)
}


