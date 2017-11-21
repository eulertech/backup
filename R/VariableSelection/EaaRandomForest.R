
##############################################################
# 
# Purpose of function: Variable selection using Random Forest
# This function rank orders features using Random Forest
# The ordering is done using variable importance
# 
# Important: The function expects a data frame in which the
# first element is the target and the remaining are the features
#
#
# Author: Valentin Todorov
#
##############################################################

EaaRandomForest <- function(trainingData,
                            numberTrees,
                            outputImportance = TRUE,
                            maxEndNodes) {
  
  # Install the needed packages
  wants <- c("randomForest")
  neededPackages <- wants %in% rownames(installed.packages())
  if(any(!neededPackages)) install.packages(wants[!neededPackages])
  sapply(wants, require, character.only = TRUE)
  
  rm("wants", "neededPackages")
  
  
  # Extract the names of the target and create a list with features
  yVar <- names(trainingData)[1]
  xVar <- names(trainingData)[2:length(names(trainingData))]
  
  # Estimate model using Random Forest
  fitEquation <- as.formula(paste0(yVar, "~", paste0(xVar, collapse = " + ")))
  
  set.seed(78946)
  estimateRfModel <- randomForest(formula = fitEquation,
                                  data = trainingData,
                                  ntree = numberTrees,
                                  importance = outputImportance,
                                  maxNodes = maxEndNodes)
  
  # Rank-order predictors by the calculated variable importance
  varImportance <- data.frame(cbind("Variables" = rownames(estimateRfModel$importance),
                                    estimateRfModel$importance))
  
  varImportance$AccuracyDecreaseImportance <- as.numeric(as.character(varImportance$X.IncMSE))
  varImportance$GiniNodePurity <- as.numeric(as.character(varImportance$IncNodePurity))
  varImportance <- varImportance[, !names(varImportance) %in% c("X.IncMSE", "IncNodePurity")]
  
  varImportance <- varImportance[order(-varImportance$AccuracyDecreaseImportance), ]
  rownames(varImportance) <- 1:nrow(varImportance)
  
  return(varImportance)
}
