
###################################
#
# This code simulates an AR1 and a random walk process
# 
# The code is from the BSTS training conducted at Google
# Source: https://sites.google.com/site/stevethebayesian/googlepageforstevenlscott/course-and-seminar-materials/bsts-bayesian-structural-time-series
#
###################################


sampleSize <- 1000
numberOfSeries <- 1000

# Simulate an AR1 process
ManyAr1 <- matrix(nrow = sampleSize, ncol = numberOfSeries)
for (i in 1:numberOfSeries) {
  ManyAr1[, i] <- arima.sim(model = list(ar = .95), n = sampleSize)
}


# Simulate a random walk
ManyRandomWalk <- matrix(nrow = sampleSize, ncol = numberOfSeries)
for (i in 1:numberOfSeries) {
  ManyRandomWalk[, i] <- cumsum(rnorm(sampleSize))
}


# Plot a single AR1 and RW series
series_num <- sample(1:1000, 1)
plot.ts(ManyAr1[, series_num], plot.type = "single", main = "AR 1")
plot.ts(ManyRandomWalk[, series_num], plot.type = "single", main = "Random Walk")


# Plot all 1000 series
par(mfrow = c(1, 2))
plot.ts(ManyAr1, plot.type = "single", main = "AR 1")
plot.ts(ManyRandomWalk, plot.type = "single", main = "Random Walk")


