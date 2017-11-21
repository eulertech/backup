
library(forecast)
library(xts)
library(tseries)

start_year <- 2000
start_month <- 1

dt <- iddsDataDtSampled[1:10]

# Add seasonal dummies
dt1 <- ts(dt, start = c(start_year, start_month), freq = 12)
dummies <- forecast::seasonaldummy(dt1)

dt2 <- na.omit(cbind(dt, dummies[1:nrow(dt), ]))
row.names(dt2) <- dt2[, c("date")]

dt3 <- na.omit(xts::as.xts(dt2[, -1]))
dataLevels <- dt3
cc <- dt3[1, ]

for (i in 1:ncol(dt3)) {
  cc[, i] <- min(dt3[, i])
  
  if(cc[, i] > 0) {
    dataLevels[, i] <- log(dt3[, i])
  } else {
    
    if(cc[, i] <0) {
      dataLevels[, i] <- dt3[, i]
    }
  }
}

rm(dt1, dt2, dt3, cc, dummies)


# Check stationarity and difference the series as appropriate - The null hypothesis is that the series is stationary (i.e. it has unit root)
# Use the KPSS test for stationarity
kpssAll <- apply(dataLevels, 2, tseries::kpss.test)

pvalAll <- list() 
for (k in 1:length(kpssAll)) {
  pvalAll[[k]] <- kpssAll[[k]]$p.value
}
names(pvalAll) <- colnames(dataLevels) 


# Select the stationary series. Transform the non-stationary by differencing them.
condNS <- sapply(pvalAll, function(x) x < 0.05)
condST <- sapply(pvalAll, function(x) x > 0.05)

dataNS <- dataLevels[, names(pvalAll[condNS])]
dataNS2 <- dataNS

for (i in 1:ncol(dataNS)) {
  dataNS2[, i] <- diff(dataNS[, i])
}

# Combine the differenced and the stationary series
# Add back the level for the oil price series
iddsDataDtSampledStationary <- na.omit(merge(dataLevels[, names(pvalAll[condST])],
                                             dataNS2, all = TRUE))

iddsDataDtSampledStationary$v134253300 <- iddsDataDtSampled$v134253300[2:nrow(iddsDataDtSampled)]



# For now, drop all the dummmies
iddsDataDtSampledStationary <- iddsDataDtSampledStationary[, !names(iddsDataDtSampledStationary) %in% c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov")]



  
iddsDataDtSampledTraining_t <- iddsDataDtSampledStationary[paste(training_period_start, training_period_end, sep = "/")]
iddsDataDtSampledValidation_t <- iddsDataDtSampledStationary[paste(validation_period_start, validation_period_end, sep = "/")]










