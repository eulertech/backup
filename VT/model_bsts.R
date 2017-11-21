#####################################
#
# BSTS research
# This is code to implement BSTS
#
#
#####################################


# Call the BSTS package
library(bsts)


# Data manipulations
df <- iddsDataDtSampledTraining
colnames(df)[1] <- "target"

# Create local linear trend and add seasonal components
ss <- AddLocalLinearTrend(list(), df$target)
ss <- AddSeasonal(ss, df$target, nseasons = 12)


# Estimate the BSTS model
# It is important to pick a large number of iterations > 4000
bstsModel <- bsts(target ~ .,
                  state.specification = ss,
                  data = df,
                  niter = 5000,
                  seed = 7890)

summary(bstsModel)

# Create a dataframe with the inclusion probabilities for each of the predictors
# The column called "inclusion_probability" should be used for rank ordering predictors
bstsModelInclusionProb <- as.data.frame(summary(bstsModel)$coefficients)
colnames(bstsModelInclusionProb)[5] <- "inclusion_probability"


### Diagnostics of results and forecasts

# Prints out the posterior inclusion probabilities by variable
# -> Black bars indicate negative coefficients
# -> White bars indicate positive coefficients
# -> Gray is for indeterminate sign
plot(bstsModel, "coefficients")

# Shows the amount of contribution by each component
plot(bstsModel, "components")

# Plots the standardized target and predictors 
plot(bstsModel, "predictors")


# Predict using BSTS - needs a data frame
predictBsts <- predict(bstsModel,
                       newdata = iddsDataDtSampledValidation,
                       horizon = 24,
                       burn = 1000)

forecastsBsts <- as.data.frame(cbind(brentForecast = as.numeric(predictBsts$median),
                                     brentActual = as.numeric(iddsDataDtSampledValidation$v134253300)))
forecastsBsts$date <- as.Date(iddsDataDtSampledValidation$date)


# Plot the forecasts
ggplot(forecastsBsts, aes(x = date)) +
  geom_line(aes(y = brentForecast), colour = "Red") +
  geom_line(aes(y = brentActual)) +
#  guides(fill = guide_legend(reverse = TRUE)) +
#  scale_fill_discrete(breaks = c("Forecast","Actial")) +
  ylab(label = "Brent Price ($/barel oil)") +
  xlab("Date")


# Lookup the feature names for a subset of predictors based on the series id
# Add the inclusion probabilities by feature from the BSTS model output and sort
listFeatures <- c("16136650", "128134181", "12510119", "12640401")      # List out the series id for which I want to lookup the labels
listFeaturesTransformed <- paste("v", listFeatures, sep = "")

listFeaturesDf <- subset(listOfSeries[, names(listOfSeries) %in% c("series_id", "shortlabel", "startdate", "enddate")], series_id %in% listFeatures)
bstsModelInclusionProb$series_id <- gsub("v", "", row.names(bstsModelInclusionProb))
finalBstsFeaturesList <- merge(listFeaturesDf, bstsModelInclusionProb, by = "series_id", all.x = TRUE)

finalBstsFeaturesList <- finalBstsFeaturesList[with(finalBstsFeaturesList, order(inclusion_probability, decreasing = TRUE)), ]


