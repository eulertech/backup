#####################################
#
# LASSO research
# This is code to implement LASSO
# Sources:  http://ricardoscr.github.io/how-to-use-ridge-and-lasso-in-r.html
#           https://web.stanford.edu/~hastie/glmnet/glmnet_alpha.html
#           https://cran.r-project.org/web/packages/glmnet/glmnet.pdf
#
#####################################



# Import needed libraries
library(glmnet)


# Data manipulations
df <- iddsDataDtSampledTraining
colnames(df)[1] <- "target"


# Create vectors with the historical values for the predictors
# Y and X matrices are created as numeric
y <- apply(as.matrix(df[1]), 2, as.numeric)
x <- apply(as.matrix(df[2:ncol(df)]), 2, as.numeric)

trainRows <- c(1:nrow(df))
yTrain <- y[train_rows]
xTrain <- x[train_rows, ]


# Estimate Lasso model (glmnet) using cross validation (cv) (Lasso: Alpha = 1)
set.seed(456123)
lassoModel <- cv.glmnet(xTrain, yTrain,
                        alpha = 1,
                        lambda = NULL,
                        nfolds = 10,
                       # parallel = TRUE
                       )


# Diagnostic checks of results
plot(lassoModel)                  # Plot the lambdas from the cross validation
plot(lassoModel$glmnet.fit, xvar = "lambda", label = TRUE)
lassoModel$lambda.min             # Obtain the minimum lambda  
lassoModel$lambda.1se


# Obtain the values for the predictors
coeffs <- data.frame(coef.name = as.character(dimnames(coef(lassoModel, lambda = "lambda.min"))[[1]]),
                     coef.value = matrix(coef(lassoModel, lambda = "lambda.min")))

coeffs <- data.frame(lapply(coeffs, as.character), stringsAsFactors = FALSE)
coeffs <- coeffs[!coeffs$coef.name == "(Intercept)", ]
selectedPreds <- coeffs[!coeffs$coef.value == 0, ]


