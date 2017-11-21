# This function is to implement various Causality Test within R framework,
# The causality test methods include: Granger Causality Test, TY Causality Test,
# and VAR causality test.
# The steps for the causality test:
# 1. Check completeness of the data, remove NAs
# 2. Test stationarity and Convert the data into stationary dataset for both target
#    and dependent variables if exist
# 3. Select the best lag (p)
# 4. Run GNC test for one-way (var - target) and two-way
# 5. Return a list of causality test results and summary of results
# Input: Data - Cleaned dataset in dataframe format, the first column is target variable
#        autoSelectP -- whether automatially select the lag of p using ANOVA
#        detrend -- whether to detrend the target and dependent variables
#        method -- causality test method
# Output: Dataframe with 2*(d-1) columns (col1: var name; col2: test1(var-target) p-value,
#                                       col2: test2 p-value)
# small p-value indicate causality (alpha = 0.05)

# Arthur: Liang Kuang
# email: liang.kuang@ihsmarkit.com
# date: 2017-03-21
# Reference: 1. http://davegiles.blogspot.com/2011/04/testing-for-granger-causality.html
#            2. http://www.sciencedirect.com/science/article/pii/0304407694016168
#            3. https://www.r-bloggers.com/chicken-or-the-egg-granger-causality-for-the-masses/
EaaCausality <- function(inputDataFrame, autoSelectP = TRUE, method = 'generic'){
  t1 <- Sys.time()
  library('forecast')
  library('fpp')
  source('SelectLags.R')
  # 1. check all the required input variables
  if(missing(inputDataFrame)){
    stop("The input DataFrame is required!")}
  if(class(inputDataFrame)!='data.frame'){
    inputDataFrame <- as.data.frame(inputDataFrame)
  }

  # check and remove NAs in the dataset
  # remove rows/record with NAs, rowSums is faster than complete.cases
  missingNA <- is.na.data.frame(inputDataFrame)
  print(sprintf("There are (%d rows,and %d) columns has missing values",
                sum(rowSums(missingNA)),sum(colSums(missingNA))))
  print("Removing records with NA values.")
  data2use <- inputDataFrame[rowSums(missingNA)==0,]
  # or
  #data2use <- inputDataFrame[complete.cases(inputDataFrame),]

  if(method == 'generic'){
      gnc_df <- Eaageneric_GNC_test(data2use)
    } else if(method == 'TY'){
      gnc_df <- TY_GNC_test(data2use)
    }

  sprintf("Total elapsed time is %d mins\n",Sys.time()-t1)
  print('Job successfully finished!')
  return(gnc_df)

}
