# BSTS function

# This function requires a dataframe with the first column as the target
# niter must be greater than 10, and nseasons is set at 12 (monthly) as default
# Input: df with first column as target
# Output: df with variables and their bsts coefficients
# Author: Lou Zhang

wants <- c("bsts")
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")

EaaBSTS <- function(df, niter, nseasons = 12, seed = 7890) {

colnames(df)[1] <- "target"

# Add local trends and seasonal components
ss <- AddLocalLinearTrend(list(), df$target)
ss <- AddSeasonal(ss, df$target, nseasons = nseasons)

bstsModel <- bsts(target ~., state.specification = ss, data = df, niter = niter, seed = seed)


# Create a dataframe with the inclusion probabilities for each of the predictors
# The column called "inclusion_probability" should be used for rank ordering predictors
bstsCoeffFinal <- as.data.frame(summary(bstsModel)$coefficients)
colnames(bstsCoeffFinal)[5] <- "inclusion_probability"
bstsCoeffFinal$series_id <- row.names(bstsCoeffFinal)

# Keep only the inclusion probabilities and the series_id
bstsCoeffFinal <- bstsCoeffFinal[, names(bstsCoeffFinal) %in% c("inclusion_probability", "series_id")]


return(bstsCoeffFinal)
}

