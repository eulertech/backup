## ParallelDoCall.R
##   ParallelDoCall() function (see documentation)
##
## This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
##
## Author: Marc Veillet 11/22/2016
##

# Infer the Chunk ID
# The chunks are objects and their specific structure and semantics is transparent
# to the ParallelDoCall framework.  However the application can, optionally, follow
# some conventions which allow the framework to find an Identity key for each
# chunk.  This key can then be used to label various things such as the log file
# name, some log events etc.  When a true ID cannot be found, this function produces
# one which is based on the Process ID and the current time.
InferChunkId <- function(obj) {
  if (is.list(obj) && !is.null(obj$chunkId)) {
    retVal <- as.character(obj$chunkId)
    retVal <-gsub("[ / :]", "_", retVal)
  } else {
    hexSecs <- as.hexmode(as.integer(difftime(Sys.time(), as.Date("2016-01-01"), units="secs")))
    retVal <- toupper(paste0(as.hexmode(Sys.getpid()), "_", hexSecs))
  }
  retVal
}

# Infer the Condidition Code of a Payload function
# i.e. whether it was successful if there is a clear text error message etc.
# This function encapsulate the conventions associated with how Payload functions can
# signify their success/error status.
# arguments:
#   retObj  the object, whatever it may be, returned by the payload function
#   payloadFctName character string : the name of the payload function
# retObj : the object returned by the Payload function
# payloadFctName : the name of the payload fct (this name is added to default $msg values)
# return value : a list with the following elements
#    $code integer = integer code
#    $msg  character string = clear text message
#    [future: we could add some exception object or stack dump or...]
InferPayloadCc <- function(retObj, payloadFctName) {

  # Default return values: assume no info will be available to infer status
  ccInt <- NA
  ccMsg <- "Unspecified status, a priori OK"

  if (is.null(retObj)) {
    ccInt <- -9999   # = undefined error
    ccMsg <- sprintf("Unspecified error (NULL return from %s)", payloadFctName)
  } else {
    if ("cc_int" %in% names(attributes(retObj))) {
      ccInt <- as.integer(attr(retObj, "cc_int", exact=TRUE))
      ccMsg <- sprintf("Code %d from %s", ccInt, payloadFctName)
    }
    if ("cc_msg" %in% names(attributes(retObj))) {
      ccMsg <- attr(retObj, "cc_msg", exact=TRUE)
    }
  }

  list(code=ccInt, msg=ccMsg)
}

# on the way in: chunk ID
#on the way out: attributes named ConditionCode (or ResultCode )
#   NULL or exists attribute named ConditionCode or some like that which is _negative_


#' Call a function in parallel
#'
#' \code{ParallelDoCall} Run multiple partitions of some data/problem in parallel
#'
#' This function simplifies the running of arbitrary logic against partitions of
#' a data set.  It handles the mundane but error-prone tasks of packaging and dispatching
#' work units to the multiple threads and of collecting the output of these threads.
#' It also introduces various features aimed at facilitating the collection of
#' logs and more generally at ensuring that error conditions can be detected.
#'
#' Users of \code{ParallelDoCall} can therefore focus exclusively on supplying
#' the list partitions and the "Payload" function, i.e. the  function which
#' effectively processes one partition at a time.
#'
#' The nature and shape of the partitions (aka chunks) as well as that of the return
#' value of the "Payload" function are completely transparent to the framework, although
#' they can optionally comply to a few simple conventions as so to benefit from further
#' services offered by the framework.
#'
#' @param awsConfig an object such as one returned by \code{configureAWS()}, i.e. the list of the various
#'   configuration and parameters used to access various the Amazon Web Services.
#' @param appConfig list : a list of options and parameters which control the behavior of the
#'   application. This optional argument is merely passed as-is to the Payload function, its
#'   specific nature and usage is transparent to the ParallelDoCall function.
#' @param chunks list : a list of objects
#'    These objects may be anything which the "Payload" function understands.
#'    list with chunkId
#'    named items?
#' @param packages character vector : the names the packages that need to be sent to the worker nodes, in support
#'   of the "payload" function and its dependencies.
#' @param export character vector with the name of the objects to be sent to the worker nodes; the very first element of
#'   this vector must be a function; it is known as the "Payload function" and is the function that is invoked for each
#'   chunk.
#' @param combine character string : the name of the function to use to combine together the return values from
#'    the threads. Defaults to \code{NULL} which returns a list. typical values are \code{rbind}, \code{cbind},
#'    \code{c}, but any function which takes two or more of arguments and appends (in whichever way the application
#'    finds fit) the the subsequent arguments to the first one could be used.
#' @param savePayloadLog Yes/No/FailOnly
#' @param savePayloadLogTo character string : the URL to the S3 folder where the log produced by the "Payload" function,
#'   if any, is to be saved.
#' @param doPar logical : \code{TRUE}, the default, request a parallel processing. \code{FALSE}
#'   results in a sequential processing; this can be useful for debugging purposes.
#' @export
ParallelDoCall <- function(awsConfig, appConfig, chunks, packages, export, combine=NULL,  savePayloadLog="No", savePayloadLogTo=NULL, doPar=TRUE) {
  logOrig <- "DoCallInParallel()"  # string used for origin argument to Log() calls.

  # *** Basic validation of the arguments ***
  if (is.null(awsConfig)) {
    stop(sprintf("Invalid argument 'awsConfig'. The %s function requires an awsConfig such as one produced by ConfigureAWS()",
                 logOrig))
  }
  if (is.null(chunks) || !is.list(chunks) || length(chunks) == 0) {
    stop(sprintf("Invalid argument 'chunks'. The %s function expects a list, non-empty",  logOrig))
  }
  if (is.null(export) || !is.character(export) || length(export) < 1) {
    stop(sprintf("Invalid argument 'export'. The %s function expects a character vector with at least one element",  logOrig))
  }
  if(!is.function(get0(export[1], ifnotfound=""))) {
    stop(sprintf("Invalid argument. '%s' is not found or not a function! The %s function expects that the first element of its 'export' argument be the name of a function.",
                 export[1], logOrig))
  }
  if (is.null(combine)) {
    combine <- function(a, ...) { c(a, list(...))}
  } else if (!is.character(combine) || length(combine) != 1) {
    stop(sprintf("Invalid argument. 'combine' must be NULL or a character string with the name of a function like 'c', 'rbind', 'cbind' and the like."))
  }

  if (savePayloadLog != "No" && (is.null(savePayloadLogTo) || !grepl("^s3://.+/$", savePayloadLogTo))) {
    stop("Invalid argument. 'savePayloadLogTo' must be supplied.  Also, it must start with 's3://' and end with '/' when the 'savePayloadLog' argument is not 'No'")
  }

  if (doPar) {
    doVerb <- "%dopar%"
    procType <- "parallel"
  } else {
    doVerb <- "%do%"
    procType <- "sequential"
  }

  DoLog(sprintf("Starting %s processing of %d chunks thru %s() function.", procType, length(chunks), export[1]),
        "MILESTONE", logOrig)

  # The following is the very equivalent of a plain "foreach(xxx) %do/dopar% {expression}"; it is just written as
  # a function call of the %do% or %dopar% operator so that the operator can be parametrized (sometimes using %do%,
  # sometimes %dopar%)
  retVal <-
    do.call(doVerb,
            list(
              foreach(chunk=iter(chunks),
                    .packages=packages,
                    .export=export),
#                    .combine=combine),
              quote(
              {
              # *** Configure the Worker thread so it can work (each worker is a separate preoces)
              if (doPar) {
                AACloudTools::ConfigureSnowWorker(appConfig$rootDir, awsConfig)
              }
              # Provide a Logger for it;  the logger has two outputs
              #    - one relatively terse, to the console (by way of ECHO command so this can be forwarded back to the master node)
              #    - the other quite verbose going to a file (which is local to the worker node)
              # Also, note the PID as Origin to help keeping log events from a given thread together.
              chunkId <- InferChunkId(chunk)  # the ChunkId is used for naming the log etc.
              Logger <- AALogger()
              Logger$Origin <- sprintf("PID=%d ChnkId=%s", Sys.getpid(), chunkId)
              Logger$SetLogLevel("MILESTONE")
              Logger$SetLogFile(NULL)
              VerboseLogFileName <- sprintf("Log_%s.log", chunkId)
              Logger$SetLogFile(VerboseLogFileName, logNr=2, overwrite=TRUE)
              Logger$SetLogLevel("DBG", logNr=2)

              chunkOut <- do.call(export[1], list(chunk, appConfig, Logger))

              callStatus <- InferPayloadCc(chunkOut, payloadFctName=export[1])

              # *** Close the "DBG"log file, save it to S3 if applicable and dispose of the local one ***
              Logger$SetLogFile(NULL, logNr=2)
              Logger$SetLogLevel("DISABLE", logNr=2)
              if (savePayloadLog != "No" &&
                  file.exists(VerboseLogFileName) && file.info(VerboseLogFileName)$size > 0) { # Not all Payload fcts produce log info
                if (savePayloadLog == "Yes" || (!is.na(callStatus$code) && callStatus$code < 0)) {
                  UploadFileToS3(VerboseLogFileName, paste0(savePayloadLogTo, VerboseLogFileName))
                }
              }
              if (file.exists(VerboseLogFileName))
                file.remove(VerboseLogFileName)

              chunkOut
              }
            )
            )
    )

  DoLog(sprintf("Done with %s processing of %d chunks thru %s() function.", procType, length(chunks), export[1]),
        "MILESTONE", logOrig)

  retVal
}

