GetClusters <- function(data, maxVars) {
  #Private function to calculate clusters in a recursive style driven by public function
  # arg: data     - data matrix, rows as variables
  #      maxVars  - integer, maximum number of variables per cluster
  # ret: clusters - character vector

  nn <- nrow(data)
  if (nn < 2) {
    clusters <- rep(1L, nn)
    names(clusters) <- rownames(data)
    return(clusters)
  }

  maxDrivers  <- 5000 #limit set by technical/memory limits to avoid stackoverflow
  pctSample   <- 10
  nbOfDrivers <- floor(pctSample/100*nn)

  drivers <- sample(nn, nbOfDrivers) #unless...
  if (nbOfDrivers > maxDrivers){
    drivers <- sample(nn, maxDrivers)
  }
  if (nbOfDrivers < 100){
    drivers <- 1:nn
  }

  #TODO (FM) make a better informed decision on k
  projectedClusters <- floor(nn/maxVars)
  k <- floor(quantile(x=1:projectedClusters, probs = seq(0.25, 1, 0.25)))

  #Adjustments for small data
  if (nbOfDrivers < 25) k <- c(2, 3, 5)
  if (nbOfDrivers < 10)  k <- 2


  theData <- data[drivers, ]
  res  <- dtwclust::tsclust(series=theData,
                            type="partitional",
                            k=k,
                            distance="dtw_basic",
                            seed=123)

  #Evaluate k to select the one with optimal Silouhete value
  if (length(k) > 1) {
    evaluate   <- sapply(res, cvi, type = "Sil")
    theModel   <- res[[which.max(evaluate)]]
  } else {
    theModel   <- res
  }

  clusters <- dtwclust::predict(theModel, newdata=data)
  clusters
}



#' Cluster drivers
#'
#' \code{SpectreGetVariableClusters} uses a kmeans algorithm for the clustering of
#' the drivers in a wide dataset (measured in the thousands).
#'
#' The function -inspired by the YADING algorithm- acts in a three step procedure:
#' (1) sampling the data to a manageable size, (2) obtaining the clusters,
#' (3) asigning the remainder of drivers to the clusters.
#'
#' This process is repeated iteratively, similarly to a hierarchical method, until
#' each cluster contains at maximum \code{maxVars} drivers.
#'
#'
#' @note While inspired on the YADING algorithm, this function in its current state
#' only implements a basic logic, not fully taking onboard YADING. For instance,
#' no dimensionality reduction (i.e. PAA) has been applied.
#'
#' See: Ding et al. (2015). YADING: Fast Clustering of Large-Scale Time Series Data.
#' Proceedings of the VLDB Endowment, Vol.8, No.5. Available from: http://www.vldb.org/pvldb/vol8/p473-ding.pdf
#' (Accessed 4/4/2017).
#'
#' @note This function is computationally expensive, and for very wide datasets (+10000 variables)
#' you may be willing to run it on the server in a parallelised environment.
#'
#' @param data : a data.table containing the drivers to be clustered.
#' @param maxVars : an integer stating the maximum number of variables a cluster
#' should contain. This is used as a threshold to iterate until each cluster
#' contains \code{maxVars} as a maximum.
#'
#' @return integer : named vector containing the allocated clusters.
#'
#' @author Francisco Marco-Serrano
#'
#' @seealso \code{link{tsclust}}
#'
#' @export
SpectreGetClusters <- function(data, maxVars) {

  # *** Basic validation of the arguments ****
  if (!is.data.table(data)) {
    stop("Invalid argument: 'data'. Expecting a data.table.")
  }
  if (ncol(data) == 0 || nrow(data) == 0) {
    stop("Invalid argument: 'data'. data is empty!")
  }
  if (ncol(data) < 2) {
    stop("Invalid argument: 'data'. data needs to have at least 2 columns to be clustered!")
  }
  if (maxVars > ncol(data)) {
    stop("Invalid argument: 'maxVars' exceeds number of variables in data.table!")
  }
  if (maxVars < 2) {
    stop("Invalid argument: 'maxVars' means 1 variable per cluster!")
  }

  if ("date" %in% colnames(data)) data <- data[, !"date", with=FALSE]
  n <- ncol(data)

  data <- t(data) #it transforms into matrix - 1 time series per row

  #All variables begin with cluster #1
  theClusters <- rep(1L, n)
  names(theClusters) <- rownames(data)
  newIDs <- 1
  maxVarsInCluster <- n
  failsafe <- FALSE #to avoid infinite loop in lack of convergence

  while (maxVarsInCluster > maxVars & failsafe==FALSE) {

    #Recalculate clusters
    newClusters <- foreach (subCluster=seq_along(newIDs),
                            .packages=c('AACloudTools', 'dtwclust'),
                            .verbose=TRUE) %dopar% {

      varsInSubCluster <- names(theClusters)[theClusters==newIDs[subCluster]]
      thisData         <- data[varsInSubCluster,]

      #Further clusterise ONLY subClusters that exceed maxVars
      nsub <- length(varsInSubCluster)
      if (nsub>maxVars) {
        clusters <- GetClusters(thisData, maxVars)
      } else {
        clusters <- rep(1L, nsub)
      }

      clusters        <- paste0(newIDs[subCluster], "_", clusters) #even at the risk of a long name
      names(clusters) <- varsInSubCluster #funnily, names were being stripped out
      clusters
    }

    theClusters      <- unlist(newClusters)
    maxVarsPre       <- maxVarsInCluster
    maxVarsInCluster <- max(table(theClusters))
    if (maxVarsInCluster==maxVarsPre) failsafe=TRUE               #doesn't converge; repeated run -> stop
    if (quantile(table(theClusters), 0.95)<maxVars) failsafe=TRUE #good enough; repeated run -> stop

    uniqueClusters <- unique(theClusters)
    newIDs         <- seq_along(uniqueClusters)
    theClusters    <- plyr::mapvalues(theClusters, from=uniqueClusters, to=newIDs)

  }

  msg <- sprintf("Finished clustering %s drivers!", n)

  theClusters
}

#' @export
SpectreGetVariableClusters <- function(data,
                                       method="local",
                                       nbOfClust,
                                       nbOfVars){


  warning("This function is obsolete!")

  res <- ClustVarLV::CLV_kmeans(data, method=method, clust=nbOfClust,
                                rho=0.2, strategy="kplusone")

  sink("TempOutput.txt")
  ClustOut <- summary(res)
  sink()
  Varlist <- vector()
  AllClust_TopVars <- NULL

  for (i in 1:nbOfClust){
    df1 <- ClustOut$groups[[i]]
    df1 <- data.frame(cbind(Vars=row.names(df1), df1))
    colnames(df1)<- c("Vars","Cor_Own_Cluster","Cor_Next_Cluster")
    df1$Cor_Own_Cluster<-as.numeric(as.character(df1$Cor_Own_Cluster))
    df1$Cor_Next_Cluster<-as.numeric(as.character(df1$Cor_Next_Cluster))
    df2 <- df1[order(-df1$Cor_Own_Cluster, df1$Cor_Next_Cluster),]
    df2$Cluster<-i
    Cluster_TopVar<-df2[1:nbOfVars, ]
    AllClust_TopVars<- rbind(Cluster_TopVar, AllClust_TopVars)
  }
  clean <- is.na(AllClust_TopVars$Cluster)
  AllClust_TopVars[!clean, ]
}
