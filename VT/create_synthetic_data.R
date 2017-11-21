################################################################################
#
# This code is a subset of http://tfs-emea.ihs.com:8080/tfs/emea_ihs_collection/AdvancedAnalytics/_git/AASpectre?path=%2Ftests%2F_demoGetFeatures.R
#  
# Purpose: Create a synthetic dataset to test variable selection algorithms with it
# Author: Francisco Marco-Serrano (modified by Valentin Todorov)
################################################################################



# Random target for validation #1

simulateTarget <- function(data, seed, nVarsToCreateTarget) {
  #Simulates a target variable
  # arg: data.table
  # ret: list with target and real drivers

  set.seed(seed)
  nVars     <- nVarsToCreateTarget
  rndVars   <- sample(ncol(data), nVars)
  rndCoeffs <- runif(nVars + 1, min = -1, max = 1)
  
  y <- mapply("*", as.data.frame(cbind(rep(1, nrow(data)),
                                       data[, colnames(data)[rndVars]])), rndCoeffs)
  y <- rowSums(y) + rnorm(nrow(data))
  x <- data[, colnames(data)[rndVars]]
  
  list(target = y, drivers = x)
}

# plot.ts(cbind("target" = sim$target, sim$drivers), main = "Target & Drivers")

