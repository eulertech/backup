

require(AACloudTools)
require(AASpectre)

awsConfig <- ConfigureAWS("Config/config.json") 

InitializeMonthly <- function() {
  appConfigM              <- list()
  appConfigM$rootDir      <- "AASpectre"
  appConfigM$frequency    <- "MONT"
  appConfigM$dateFrom     <- "2000-01-01"
  appConfigM$dateTo       <- "2016-09-01"
  appConfigM$lagBuffer    <- TRUE
  appConfigM$sqlCond      <- "frequency='MONT' AND seriestype NOT LIKE 'ARCH'"
  appConfigM$minCovPct    <- 0
  appConfigM$fcastPeriods <- 1
  appConfigM$maxNAs       <- 1
  appConfigM$type         <- "ts"
  
  appConfigM
}

appConfigM <- InitializeMonthly()


allVars <- SpectreGetVarInfo(varIds=NULL, 
                             sqlCondition=appConfigM$sqlCond) 

x_varlist <- New_SpectreVarList(id="eaa", 
                                projectName="eaa_test", 
                                listType="I",
                                listName="eaa variables", 
                                shortDescr="eaa variables", 
                                longDescr="eaa variables to keep") 

df <- data.frame(allVars$mnemonic) 

cc <- x_varlist$ImportListFromDf(df,
                                 persist=TRUE)

x_DT <- SpectreGetData(varList="eaa",
                       frequency=appConfigM$frequency, 
                       dateFrom=appConfigM$dateFrom,
                       dateTo=appConfigM$dateTo,
                       hasLagBuffer=appConfigM$lagBuffer)

###############################################################################

allVars <- SpectreGetSeriesOfInterest(projectName="eaa_test",
                                      varListId="v1",
                                      appConfigM$sqlCond,
                                      appConfigM$frequency,
                                      appConfigM$dateFrom,
                                      appConfigM$dateTo,
                                      minCoveragePct=appConfigM$minCovPct)

x_DT <- SpectreGetData(varList="v1",
                       frequency=appConfigM$frequency, 
                       dateFrom=appConfigM$dateFrom,
                       dateTo=appConfigM$dateTo,
                       hasLagBuffer=appConfigM$lagBuffer)



x_DT <- SpectreGetFiltered(data=x_DT,
                           frequency=appConfigM$frequency,
                           forecastPeriods=appConfigM$fcastPeriods,
                           maxNA=appConfigM$maxNAs,
                           hasLagBuffer=FALSE)

x_DT <- SpectreGetImputed(data = x_DT,
                          type=appConfigM$type)

sum(is.na(x_DT))





