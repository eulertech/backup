
#####################################
#
# Random Forest research with the randomForest package
#
# http://www.statistik.uni-dortmund.de/useR-2008/slides/Strobl+Zeileis.pdf
# https://cran.r-project.org/web/packages/randomForest/randomForest.pdf
#
#####################################


# Call the needed packages
library(party)


# Random forest parameters
trainingData <- iddsDataDtSampledTraining[, 1:20]
numberTrees <- 500
mtry <- 5


# Extract the names of the target and create a list with features
yVar <- names(trainingData)[1]
xVar <- names(trainingData)[2:length(names(trainingData))]

# Estimate model using Random Forest
fitEquation <- as.formula(paste0(yVar, "~", paste0(xVar, collapse = " + ")))

set.seed(78946)

ctree_model <- cforest(fitEquation,
                       data = trainingData,
                       controls = cforest_unbiased(ntree = numberTrees, mtry = mtry))

# Permutation importance
varimp(ctree_model)

plot(ctree_model)

