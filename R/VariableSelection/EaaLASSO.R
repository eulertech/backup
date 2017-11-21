# This function requires a cleaned data frame of predictors, and another with the target.
# Both data frames must have the same time period and number of observations.

# INPUT:
# df: dataframe with column target/predictors, with the target at the beginning
# weights : a vector of weights, need to be length of dataset

# OUTPUT:
# Returns a dataframe of selected features

#Author: Lou Zhang

## Load drivers
wants <- c("glmnet", "lmtest")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

EaaLASSO <- function(df, weights = 1, allfeatures = TRUE, sorted = TRUE) {

  range01 <- function(x){
    
    (x-min(x))/(max(x)-min(x))
    
    }

  y <- apply(as.matrix(df[1]), 2, as.numeric)
  x <- apply(as.matrix(df[2:ncol(df)]), 2, as.numeric)

  train_rows <- c(1:nrow(df))
  x.train <- x[train_rows, ]

  y.train <- y[train_rows]

  cvfit <- cv.glmnet(x.train, y.train)

  lm.fit <- cvfit$glmnet.fit

  coeffs <- data.frame(coef.name = dimnames(coef(cvfit, lambda = "lambda.min"))[[1]], coef.value = matrix(coef(cvfit, lambda = "lambda.min")))
  coeffs <- coeffs[!coeffs$coef.name == "(Intercept)",]
  
  if(allfeatures == FALSE) {
    coeffs <- coeffs[coeffs$coef.value != 0,]
  }
  
  coeffs$coef.value[coeffs$coef.value < 0] <- 0
  
  coeffs$ScaledSignificance <- range01(coeffs$coef.value)
  if(sorted == TRUE){coeffs <- coeffs[ order(-coeffs[,3]), ]}

  return(coeffs)
}
