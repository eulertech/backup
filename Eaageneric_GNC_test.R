# Applying generic granger causality test for pairwise dataset
# Input: dataIn - dataframe with target at column one and the rest varaibles at 2:end
# Output: p-value dataframe for x$y and y$x GNC test
# external file needed: select.lag.R
# Liang Kuang
# 2017-03-22

Eaageneric_GNC_test <- function(dataIN,max.lag=12){

library('forecast')
source('SelectLags.R')
if(missing(dataIN)){
  stop("Required input Dataframe (y,x1,x2...) not found!")
}

nd <- ndiffs(dataIN[,1], alpha = 0.05, test = c('kpss'))
# difference time series
if(nd !=0) {
	dy <- diff(dataIN[,1], lag = nd)
}else{
	dy <- dataIN[,1]
}
cnames <- names(dataIN)
gnc_array <- matrix(0,nrow = dim(dataIN)[2]-1,ncol=2)

for (i in 2:dim(dataIN)[2]) {
  if(nd !=0){ 
	dx <- diff(dataIN[,i],lag=nd)
	}else{
	dx <- dataIN[,i]
	}
  optimal_lag <- SelectLags(dx,dy,max.lag = max.lag)
  gnc1 <- grangertest(dx~dy, order = optimal_lag$selection$aic)   # x$y
  gnc2 <- grangertest(dy~dx, order = optimal_lag$selection$aic)   # y$x
  gnc_array[i-1,1] <- gnc1$`Pr(>F)`[2]
  gnc_array[i-1,2] <- gnc2$`Pr(>F)`[2]
  #convert it to dataframe
  onames1 <- sapply(2:dim(dataIN)[2], function(i) paste0(cnames[i],'$',cnames[1]))
  onames2 <- sapply(2:dim(dataIN)[2], function(i) paste0(cnames[1],'$',cnames[i]))
  gnc_df <- data.frame(gnc_array)
  colnames(gnc_df) <- c(onames1,onames2)
}

return(gnc_df)

}
