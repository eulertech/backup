context("Tests of ParallelDoCall()")


getMock_Chunks1 <- function(nbChunks) {
  fullList <- list(
    ZeOne=list(chunkId="One",
             x=c(2, 7, 9),
             city="Paris",
             deltaTime=33
    ),
    LeDeux=list(chunkId="Two",
             x=c(5, 88, 21),
             city="New Delhi",
             deltaTime=-12,
             FailMe=TRUE
    ),
    DerDrei=list(chunkId="Three",
               x=c(2, 11, 42),
               city="Denver",
               deltaTime=-14
    ),
    ElQuatro=list(chunkId="Four",
               x=c(5, 44, 6),
               city="Rio de Janeiro",
               deltaTime=34
    ),
   AlKhamsa=list(chunkId="Five",
                  x=c(1, 8, 8),
                  city="Sydney",
                  deltaTime=22
    )
  )

  fullList[seq(nbChunks)]
}

getMock_AppConfig1 <- function() {
  retVal <- list(minTemp=32, maxTemp=68)
  retVal$rootDir <- "Mnt_FreightRatesForecast"
  retVal
}

mock_Payload1 <- function(chunk, appConf, Logger) {
  Logger$Log("Payload1: starting", "MILESTONE")

  Logger$Log(sprintf("Payload1: Info msg: City is %s", chunk$city), "INFO")
  Logger$Log(sprintf("Payload1: very debuggy stuff. MaxTmp = %d", appConf$maxTemp), "INFO")

  Logger$Log("Payload1: Done", "MILESTONE")

  if (!is.null(chunk$FailMe) && chunk$FailMe) {
    retVal <- NULL
  } else {
    retVal <- Logger$Origin
  }
  retVal
}

test_that("InferPayloadCc() works", {
  plFct <- "MyBigFct"  # name of the payload function

  # By convention a NULL return form the  payload function signify an error of sort
  # InferPayLoad() produces arbitrary code and message for these cases.
  tstOut <- InferPayloadCc(NULL, plFct)
  expect_equal(tstOut$code, -9999)
  expect_match(tstOut$msg, plFct)
  expect_match(tstOut$msg, "Unspecified error")

  # When an object (of any type) is returned, the payload function is assumed to have
  # been successful (unless, see below, some attributes are available and tell otherwise)
  obj <- list()
  tstOut <- InferPayloadCc(obj, plFct)
  expect_true(is.na(tstOut$code))
  expect_match(tstOut$msg, "Unspecified status")

  # when only cc_int is provided, InferPayloadCc() takes it at face value and creates a
  # message which includes this value and the payload fct name.
  attr(obj, "cc_int") <- 4243
  tstOut <- InferPayloadCc(obj, plFct)
  expect_equal(tstOut$code, 4243)
  expect_match(tstOut$msg, plFct)
  expect_match(tstOut$msg, "4243")

  # when both cc_int and cc_msg are provided, InferPayloadCc() takes these at face value
  obj <- "BBB"             # ensure that obj can be anything; not just list etc.
  attr(obj, "cc_int") <- -33
  attr(obj, "cc_msg") <- "Major collapse of the triple inductor"
  tstOut <- InferPayloadCc(obj, plFct)
  expect_equal(tstOut$code, -33)
  expect_equal(tstOut$msg, attr(obj, "cc_msg"))
})

if (FALSE) {
test_that("initial test", {
  awsConf   <-  ConfigureAWS("./Config/config.json")
  appConfig <- getMock_AppConfig1()
  chunks    <- getMock_Chunks1(3)

  s3UrlForLog <- GetS3TempUrl()

  dataOut <- ParallelDoCall(awsConf,
                            appConfig, chunks,
                            packages=NULL, export=c("mock_Payload1"),  savePayloadLog="Yes", savePayloadLogTo=s3UrlForLog, doPar=FALSE)

  snowCluster <- AACloudTools::AAStartSnow(local=TRUE, awsConf)

  chunks <- list(chunks[[2]])
  dataOut <- ParallelDoCall(awsConf,
                            appConfig, chunks,
                            packages=c("AACloudTools"), export=c("mock_Payload1"),  savePayloadLog="No", savePayloadLogTo=NULL, doPar=TRUE)

  dataOut <- ParallelDoCall(awsConf,
                            appConfig, chunks,
                            packages=c("AACloudTools"), export=c("mock_Payload1"),  savePayloadLog="Yes", savePayloadLogTo=s3UrlForLog, doPar=TRUE)

  dataOut <- ParallelDoCall(awsConf,
                            appConfig, chunks,
                            packages=c("AACloudTools"), export=c("mock_Payload1"),  savePayloadLog="FailOnly", savePayloadLogTo=s3UrlForLog, doPar=TRUE)

  AACloudTools::AAStopSnow(snowCluster, awsConf)

})

}
