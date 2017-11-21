#################################################################
#
# This is a modified version of the EaaLASSO function in the current directory
#
# This function requires a cleaned data frame of predictors, and another with the target.
# Both data frames must have the same time period and number of observations.
#
# INPUT:
# df: dataframe with column target/predictors, with the target at the beginning
# weights : a vector of weights, need to be length of dataset
# 
# OUTPUT:
# Returns a dataframe of selected features
# 
# Author: Valentin Todorov
#################################################################


## Load libraries
library("glmnet")


EaaLasso <- function(df, alpha = 1, nfolds = 10, parallel, lambda = NULL) {

  # Create vectors with the historical values for the predictors
  # Y and X matrices are created as numeric
  y <- apply(as.matrix(df[1]), 2, as.numeric)
  x <- apply(as.matrix(df[2:ncol(df)]), 2, as.numeric)

  train_rows <- c(1:nrow(df))
  y.train <- y[train_rows]
  x.train <- x[train_rows, ]

  # Estimate Lasso model (glmnet) using cross validation (cv) (Lasso: Alpha = 1)
  # nfolds specifies the number of cross validations to perform
  # The estimation can be run in parallel if there is a SNOW cluster
  cvfit <- cv.glmnet(x.train, y.train,
                     alpha = alpha,
                     lambda = lambda,
                     nfolds = nfolds,
                     parallel = parallel)
  
  # Obtain the values for the predictors
  coeffs <- data.frame(coef.name = as.character(dimnames(coef(cvfit, lambda = "lambda.min"))[[1]]),
                       coef.value = matrix(coef(cvfit, lambda = "lambda.min")),
                       stringsAsFactors = FALSE)
  
 # coeffs <- data.frame(lapply(coeffs, as.character), stringsAsFactors = FALSE)
  coeffs <- coeffs[!coeffs$coef.name == "(Intercept)", ]
  coeffs <- coeffs[!coeffs$coef.value == 0, ]

  return(coeffs)
}
