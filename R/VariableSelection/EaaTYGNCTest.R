#########################################################################################
# This function is to compute pairwise granger causality test using TY methods.
# Input: dataIN - dataframe with target at column one and the rest varaibles at 2:end
# Output: p-value dataframe for x$y and y$x GNC test
# ref:   https://www.christophpfeiffer.org/2012/11/07/toda-yamamoto-implementation-in-r/
# Author: Liang Kuang
#########################################################################################  
EaaTYGNCTest <-  function(dataIN, max.lag = 12, lag.method = 'AIC'){
 
  source('SelectLagVARS.R')
  
  wants <- c("urca","vars","aod","zoo","tseries","forecast")
  has   <- wants %in% rownames(installed.packages())
  if(any(!has)) install.packages(wants[!has])
  sapply(wants, require, character.only = TRUE)
  rm("wants","has")
  
  
  if(missing(dataIN)) {
    stop("Required input dataframe(y,x1,x2,...xn) is missing !")
  }
  
  cnames <- names(dataIN)
  gnc_array <- matrix(0,nrow = (dim(dataIN)[2]-1),ncol=2)
  
  nd <- ndiffs(dataIN[,1],alpha = 0.05, test=c('kpss'))
  
  for (i in 2:dim(dataIN)[2]) {
    
    #steps:
    # 1. find the integration order I(k) using ndiffs
    # 2. select optimal lag order p(1...m) using VARselect which may return more than one,max =4 values
    # 3. Build the VAR model, do serial.test on the VAR.p models and select the one with the most serial correlation for VAR models
    # 4. Build an augumented VAR (p+k)
    # 5. Run causality test from VARS model
    # 6. Save the two-way pairwise results to an dataframe
    
    sprintf("Processing the %d th column: %s.",i,names(dataIN)[i])
    pselect <- SelectLagVARS(dataIN[,c(1,i)],max.lag=max.lag, method=lag.method)
    
    # pselect <- SelectLagVARS(dataIN[,c(1,i)],max.lag=max.lag, method=lag.method)
    V.p <- VAR(dataIN[,c(1,i)],p=pselect + nd, type="both")
    
    a <- sum(is.na(V.p$varresult[[cnames[1]]]$coefficients))
    b <- sum(is.na(V.p$varresult[[cnames[i]]]$coefficients))
    
    if(a >0 || b >0) {
    # check whether there are NAs in const and trend, if there are NA, return NA
      gnc_array[i-1,1] <- NA
      gnc_array[i-1,2] <- NA
    }else{
    # d. Augumented VAR
    # V.aug <- VAR(dataIN[,c(1,i)],p=pselect+nd,type="both")
    # e. Causality test
    ct1 <- vars::causality(V.p,cause = cnames[i]) # x$y
    ct2 <- vars::causality(V.p, cause = cnames[1]) #y$x
    gnc_array[i-1,1] <- as.numeric(ct1$Granger$p.value[1])
    gnc_array[i-1,2] <- as.numeric(ct2$Granger$p.value[1])
    }
}
  # convert it to dataframe 
  
  gnc_df <- data.frame(gnc_array)
  colnames(gnc_df) <- c('var$target','target$vars')
  rownames(gnc_df) <- colnames(dataIN)[2:dim(dataIN)[2]]
  
  return(gnc_df)
}
