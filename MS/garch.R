library(readxl)
library(zoo)
library(dplyr)
library(rugarch)
library(Quandl)

#Import futures data from Morningstar 

brent_lou <- read_excel("C:/Users/mfp43003/Desktop/EAA/Forward contracts/BrentCrude2000-2009.xlsx")

brent_contracts <- brent_lou[,c(3,4,8)]
brent_contracts$CLOSE <- as.numeric(brent_contracts$CLOSE)
brent_contracts$EXPIRATION_DATE <- as.Date(brent_contracts$EXPIRATION_DATE, format = "%Y-%m-%d")
brent_contracts$TRADE_DATETIME <- as.Date(brent_contracts$TRADE_DATETIME, format = "%Y-%m-%d")
brent_contracts$year <- substr(brent_contracts$TRADE_DATETIME, 1,4)
brent_contracts$month <- substr(brent_contracts$TRADE_DATETIME, 6,7)
brent_contracts$maturity_weeks <- difftime (brent_contracts$EXPIRATION_DATE, brent_contracts$TRADE_DATETIME, units="weeks")
brent_contracts$maturity_months <- round((as.yearmon(brent_contracts$EXPIRATION_DATE) - 
                                      as.yearmon(brent_contracts$TRADE_DATETIME))*12,0)

#get spot prices 

Quandl.api_key("RkhEDYYV3ZrCQDsX51wi")
brent_spot = Quandl("EIA/PET_RBRTE_D", start_date="2000-01-04", type="xts")
TRADE_DATETIME <- as.Date(index(brent_spot), format="%Y-%m-%d")
brent_spot<-as.data.frame(brent_spot)
colnames(brent_spot) <- "Brent_Spot_Price"
brent_spot<-cbind(TRADE_DATETIME, brent_spot)


#add spot prices next to futures
contracts <- na.omit(merge(brent_contracts, brent_spot, by="TRADE_DATETIME", 
                                 all.x = TRUE)) 


averages <- contracts %>% group_by(year, month, maturity_months) %>% 
            summarise(futures_averages_by_maturity = mean(CLOSE),
                      brent_spot_price = mean(Brent_Spot_Price))

  
futures_12month <- ts(subset(averages, maturity_months==12),start=c(2000,1),freq=12)
futures_6month <- ts(subset(averages, maturity_months==6),start=c(2000,1),freq=12)
futures_3month <- ts(subset(averages, maturity_months==3),start=c(2000,1),freq=12) 


ts.plot(futures_12month[,4], futures_12month[,5], col=1:2)
legend ("topleft", c("futures-12month", "spot price"), lty=1, col=1:2)

ts.plot(futures_3month[,4], futures_3month[,5], col=1:2)
legend ("topleft", c("futures-3month", "spot price"), lty=1, col=1:2)


#Compute annualized excess returs
returns_12month <- (((log(futures_12month[,"futures_averages_by_maturity"]) - log(futures_12month[,"brent_spot_price"]))/log(futures_12month[,"brent_spot_price"])))
returns_6month <- (((log(futures_6month[,"futures_averages_by_maturity"]) - log(futures_6month[,"brent_spot_price"]))/log(futures_6month[,"brent_spot_price"])))*(12/6)
returns_3month <- (((log(futures_3month[,"futures_averages_by_maturity"]) - log(futures_3month[,"brent_spot_price"]))/log(futures_3month[,"brent_spot_price"])))*(12/3)


ts.plot(returns_3month, returns_6month, returns_12month, col=1:3)
legend ("topleft", c("returns-3month","returns-6month","returns-12month"), lty=1, col=1:3)

#Estimate time-varying risk premium for horizons h=3,6,12 months

garchSpec <- ugarchspec(
   variance.model=list(model="sGARCH",
                      garchOrder=c(1,1)),
  mean.model=list(armaOrder=c(0,0)), 
  distribution.model="std")

garch_3month <- ugarchfit(spec=garchSpec, data=returns_3month)
garch_6month <- ugarchfit(spec=garchSpec, data=returns_6month)
garch_12month <- ugarchfit(spec=garchSpec, data=returns_12month)

rp_3month <- ts(garch_3month@fit$sigma, start=c(2000,1), freq=12)
rp_6month <- ts(garch_6month@fit$sigma, start=c(2000,1), freq=12)
rp_12month <- ts(garch_12month@fit$sigma, start=c(2000,1), freq=12)

ts.plot(rp_3month, rp_6month, rp_12month, col=1:3)
legend ("topleft", c("rp-3month","rp-6month","rp-12month"), lty=1, col=1:3)

adjusted_returns_12month <- returns_12month - rp_12month
adjusted_log_futures <- (1+adjusted_returns_12month)*log(futures_12month[,"futures_averages_by_maturity"])

plot(adjusted_log_futures, main= "Adjusted vs Unadjusted 12-month Futures", ylab="Logged prices")
lines(log(futures_12month[,"futures_averages_by_maturity"]), col=2)
lines(log(futures_12month[,"brent_spot_price"]), col=3)
legend ("topleft", c("adjusted","unadjusted", "spot"), lty=1, col=1:3)

