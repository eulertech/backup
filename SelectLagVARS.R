SelectLagVARS <- function(dataIN,max.lag=12,pvals=0.05, method = "auto") {
  # select the p value to be used in the VAR models
  # input: dataIN - a dataframe with n > 1 columns
  #        max.lag - maxium lag to be tested (default 12)
  #        method - method for choose the right lag 'p', options: "auto","AIC","HQ","SC","FPE"
  # output: return a numeric value for lag p
  # Liang Kuang
  # 2017-03-22
#library(fUnitRoots)
library(urca)
library(vars)
library(aod)
library(zoo)
library(tseries)
if(missing(dataIN)) {
  stop("The input dataframe is required!")
}

vs <- VARselect(dataIN,lag = max.lag, type="both")$selection

if(length(unique(vs))==1) {
  return(vs[1])
}

if(method == "auto") {
  pmin <- pvals
  k = 1
  for (i in 1:length(unique(vs))) {
      p <- serial.test(VAR(dataIN, p = vs[i], type = "both"))$serial$p.value
      if(p>pmin){
        # select the lag order has the most serial correlation for var
        pmin <- p
        k = i
      }
  }
 return(vs[k])
}else if(method == "AIC"){
  return(vs[1])
}else if(method == "HQ"){
  return(vs[2])
}else if(method == "SC") {
  return(vs[3])
}else if(method == "FPE"){
  return(vs[4])
}else {
  stop("Not a valid critera! Options are: AIC, HQ, SC,FPE ")
}


}
