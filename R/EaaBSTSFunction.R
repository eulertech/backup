#BSTS function

#This function requires a dataframe with the first column as the target
#niter must be greater than 10, and nseasons is set at 12 (monthly) as default


eaaBSTS <- function(df, niter, nseasons=12){
  
wants <- c('bsts')
has   <- wants %in% rownames(installed.packages())
if(any(!has)) install.packages(wants[!has])
sapply(wants, require, character.only = TRUE)
rm("wants","has")
  
colnames(df)[1] <- 'target'

ss <- AddLocalLinearTrend(list(), df$target)
ss <- AddSeasonal(ss, df$target, nseasons = nseasons)

bstsModel <- bsts(target ~., state.specification = ss, data = df, niter = niter)

bstsCoeff <- as.data.frame(bstsModel$coefficients)


bstsCoeffFinal <- as.data.frame(1:nrow(bstsCoeff))
bstsCoeffList <- as.data.frame(1:nrow(bstsCoeff))

for(i in 1:ncol(bstsCoeff)){
  
  if(sum(bstsCoeff[i]) > 0){
    
    bstsCoeffFinal <- cbind(bstsCoeffFinal, bstsCoeff[i])
  }
  
}

bstsCoeffList <- as.data.frame(colnames(bstsCoeffFinal))
return(bstsCoeffList)
}

