
#####################################
#
# Random Forest modeling with the randomForest package
#
# http://www.statistik.uni-dortmund.de/useR-2008/slides/Strobl+Zeileis.pdf
# https://cran.r-project.org/web/packages/randomForest/randomForest.pdf
#
#####################################


# Load the needed packages
library(randomForest)


# Random forest parameters
numberTrees <- 1000
maxEndNodes <- 50
outputImportance <- TRUE
df <- iddsDataDtSampledTraining


####
# Estimate a random forest model
yVar <- names(df)[1]
xVar <- names(df)[2:16600] # If df has more than 16600 columns I get "Error: protect(): protection stack overflow"

# Estimate model using Random Forest
fitEquation <- as.formula(paste0(yVar, "~", paste0(xVar, collapse = " + ")))

set.seed(78946)
rf_model <- randomForest::randomForest(formula = fitEquation,
                                       data = df,
                                       ntree = numberTrees,
                                       importance = outputImportance,
                                       maxNodes = maxEndNodes)
print(rf_model)
#summary(rf_model)
#str(rf_model)

print ("The user provided number of trees for the RsF model is: "); rf_model$ntree
print ("The estimated MTRY parameter for the RF model is: "); rf_model$mtry
print ("The number of nodes for the RF model is: "); rf_model$forest$nrnodes


####
# Rank-order predictors by the calculated variable importance
varImportance <- data.frame(cbind("Variables" = rownames(rf_model$importance),
                                  rf_model$importance))

varImportance$AccuracyDecreaseImportance <- as.numeric(as.character(varImportance$X.IncMSE))
varImportance$GiniNodePurity <- as.numeric(as.character(varImportance$IncNodePurity))
varImportance <- varImportance[, !names(varImportance) %in% c("X.IncMSE", "IncNodePurity")]

varImportance <- varImportance[order(varImportance$AccuracyDecreaseImportance), ]
varImportance$Variables <- factor(varImportance$Variables, levels = unique(varImportance$Variables))
rownames(varImportance) <- 1:nrow(varImportance)


# Plot the inclusion probabilities by predictor
ggplot2::ggplot(varImportance, aes(Variables, AccuracyDecreaseImportance)) +
          geom_col() +
          coord_flip()


#### Diagnostics
# Shows the reduction in error from the number of trees
plot(rf_model, type = 'l')
rf_model$mse

# Cross validation
rf_model_cv <- randomForest::rfcv(df[2:ncol(df)], df$v134253300,
                                  cv.fold = 5,
                                  step = 0.5)
with(rf_model_cv, plot(n.var, error.cv, log = "x", type = "o", lwd = 2))



#################
# Parallel random forest

library(randomForest)
library(foreach)

x <- matrix(runif(500), 100)
y <- gl(2, 50)


rf <- foreach(ntree = rep(10000, 6), .combine = combine, .multicombine = TRUE, .packages = 'randomForest') %dopar% {
                randomForest(x, y, ntree = ntree,
                             importance = TRUE,
                             replace = TRUE)
              }


