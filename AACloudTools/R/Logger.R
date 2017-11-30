## Logger.R
##   Class for logging various messages relative to the proper or erroneous
##   working of the application.
##
##   Any message, be it basically informational or associated with an error
##   or warning should be sent by way of the Logger's functions.  In this
##   fashion, the particular behavior of the logging is uniform throughout
##   the application and can be modified in a cental location.
##   Furthermore, compared with the ad hoc use of print() or cat() calls
##   to the Logger functions make the logic self-documented.
##
## This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
##
## Author: Marc Veillet 11/02/2016
##    (based on similar but non-packaged logic, c. Feb2015 for TrueSource and other AA projects
##

# TODO: Escape the pipe and redirection characters when using the system("echo ...") ouptut mode.


#' Utility Class for Logging
#'
#' A Reference Class used to Log "events" of various importance. Event messages can either
#' be sent to the console or to a file.  The verbosity of the Log can be controlled to
#' filter-out messages which importance is below a particular threshold.
#'
#' Any message, be it associated with an error, a warning or merely informational, should
#' be sent by way of the Logger's functions.  In this fashion, the particular behavior of
#' the logging is uniform throughout the application (and accross applications!) and can
#' be modified in a centralized fashion (by altering the Logger or using another
#' Logger class with the same API)
#'
#' Furthermore, compared with the \emph{ad hoc} use of \code{print()} or \code{cat()} and other
#' \code{system("echo xxx")} calls, the Logger functions make the logic self-documented.
#' In other words, it makes it explicit that a particular message is effectively
#' \emph{intended} as a Log event (of X importance), rather than maybe being some
#' left-over from a debug session or maybe a data output, or yet something else...
#'
#' @field LogLevels List of short character strings indicative of the importance
#'   of log entries.  This list is \strong{ordered} from the most urgent/severe
#'   message type to the least important.  The concept of Log Level is used to
#'   control the verbosity of the log output (by filtering out events of a lesser
#'   level), and to help searching in the log output for certain types of messages.
#'   The \code{LogLevels} list defaults to
#'   \itemize{
#'     \item "FATAL"   The kind of situation that causes the script/application to abort
#'     \item "ERROR"   A serious anomaly; something rather unexpected (but not fatal)
#'     \item "WARN"    A somewhat anormal/odd situation which should be looked at
#'     \item "MILESTONE" A significant "step" within the normal flow the program
#'     \item "INFO"    Informational message
#'     \item "DETAIL"  A low importance message
#'     \item "DBG"     A message typically introduced for debug purposes only
#'     \item "-"       (a single dash) All kinds of messages with very low relevance and
#'                  which are only actually recorded/logged with the most verbose log setting
#'   }
#' These levels cover most typical implementations, but it is possible to replace the list
#' with different mnemonics and/or include more or fewer levels.
#' @field Origin A short string indicating which part of the code and/or which worker thread
#'   (in the case of parallel processing) is responsible for producing log entries.  This string
#'   is output/recorded along with the rest of the message to allow identifying the origin of a
#'   given log entry.  The application is responsible for setting whichever text it sees fit in this
#'   field; any subsquent Log event is then recorded with this string, until the field is changed
#'   anew.  The \code{Log} method's \code{origin} parameter is a optional text that is appended
#'   to this field when outputting log entries.  This allows for example the \code{$Origin} field
#'   to specify the major feature or the worker ID and the \code{origin} parameter to supply the
#'   specific section of code where the message originated.
#' @field priv PRIVATE field! Do not use.  It is an environment, used to collect all private
#'               variables used internally by the the AALogger instance.
#' @section Methods:
#' \itemize{
#'   \item \code{Log}           Send a message to the log.  see \link{AALogger_Log}
#'   \item \code{SetLogFile}    Change the file where Log messages are output. \link{AALogger_SetLogFile}
#'   \item \code{SetLogLevel}   Set the verbosity of the log.  see \link{AALogger_SetLogLevel}
#' }
#'
#' @section Synopsis and examples:
#' See \link{AALoger_Log}
#' @export AALogger
AALogger <- setRefClass(
  "AALogger_class",
  fields=list(
    LogLevels="character",
    Origin="character",
    priv="environment"
  ),
  methods=list(
    # Most methods are defined thereafter using the class$methods() syntax
    # so that their parameters could be documented

    # Set default values and other initializations.
    initialize=function() {
      priv <<- new.env(parent=emptyenv())
      LogLevels <<- c("FATAL", "ERROR", "WARN", "MILESTONE", "INFO", "DETAIL", "DBG", "-")
      Origin <<- ""
      # Private function used by methods which require a file passed as argument (e.g. SetLogFile())
      # Returns the newFile argument as-is if it is a opened file, and opens a file with exception handling
      # etc. if this argument is a character string.
      .self$priv$getFile <- function(fileArg, overwrite) {
        if (is.character(fileArg)) {
          openMode <- ifelse(overwrite, "wt", "at")
          retVal <- NULL
          # TODO: see maybe about better error handling...
          try({retVal <- file(fileArg, openMode)})
          if (is.null(retVal)) {
            stop(sprintf("Cannot open new log file '%s' for appending."))
          }
        } else {
          retVal <- fileArg
        }
        retVal
      }

      # Convert a LogLevel value as passed in functions like SetLogLevel() to a
      # numeric value as stored in fields like .self$priv$logLevel
      # argument: newLevelArg integer or character string
      #    if integer should be either 0 (= DISABLE) or in the 1-len($LogLevels)
      #    if string should be either "DISABLE" or one of the values in $LogLevels.
      .self$priv$getLogLevel <- function(newLevelArg) {
        if (is.null(newLevelArg))
          newLevelArg <- ""
        if (is.integer(newLevelArg) &&
            newLevelArg >= 0 && newLevelArg <= length(.self$LogLevels)) {
          retVal <- newLevelArg
        } else {
          if (newLevelArg == "DISABLE") {
            retVal <- 0
          } else {
            retVal <- match(newLevelArg, LogLevels, nomatch=999)
          }
        }
        retVal
      }

      .self$SetLogLevel("INFO")
      .self$priv$logFile <- stderr()

      .self$SetLogLevel("DISABLE", logNr=2)
      .self$priv$logFile2 <- NULL
    }
  )
)

#' Send a Message to the Log
#'
#' \code{AALogger$Log} Send a text message to the log, optionally specifying its "level" and its "origin".
#'
#' First this function determine if the message should actually be logged.  If the "level" of the
#' message is less than the value last set with \code{SetLogLevel}, the message is ignored.
#'
#' Next the message is formatted to include the current date and time, the Level and other elements
#' that may be passed explicitly along in the \code{$Log} call, or implicit such as when
#' \code{$Origin} attribute is set.
#'
#' Finally the message is sent to a file, to the console (stderr or stdout) or to other outputs.
#' @name AALogger_Log
#' @param msg character string. Text message that is to be logged. This text is typically constructed with
#'   \code{sprintf} or \code{paste} to include variable values.
#' @param level character string. It should be one of the strings found in \code{$LogLevels}
#'   \code{msg} is effectively logged only if level is as important as the LogLevel currently in place.
#'     (If \code{level} is not found in \code{LogLevels}, however, the message is also logged)
#' @param origin character string. Additional origin text, appended to the \code{$Origin} field in the output.
#' @examples
#'   # instantiate the Logger.  Typically only one logger per app or per significant subsystem.
#'   Logger <- AALogger()
#'
#'   Logger$Log("Hello World")
#'   badDevice <- 321
#'   Logger$Log(sprintf("Major malfunction with flux converter number %d.", badDevice), "FATAL")
#'
#'   # From now on we'll be only recording FATAL and ERROR messages
#'   Logger$SetLogLevel("ERROR")
#'   #Following two should not print/record, but the third one should
#'   Logger$Log("Setup target date to 1900", "INFO")
#'   Logger$Log("Temperature in flux converter is too high", "WARN")
#'   fluxTemp <- "753 degree celsius"
#'   Logger$Log(sprintf("Flux converter is dangerously hot (%s)", fluxTemp), "ERROR")
#'
#'   # Instantiate another Logger to go to file
#'   Logger2 <- AALogger()
#'   myLogFile <- file("./Test.log", "wt")
#'   Logger2$SetLogFile(myLogFile)
#'   Logger2$Log("This msg should go to Test.log file", "INFO")
#'   blownupCompressorId <- 444
#'   Logger2$Log(sprintf("Compressor or tank # %d exploded !!!", blownupCompressorId), "FATAL")
#'   Logger2$SetLogFile(stderr())  # has the effect of closing the file
#'
#'   # the first Logger we created above is still accessible
#'   # we now use it to demonstrate the use of the Origin concept
#'   Logger$Origin <- "SNOW-12"  # 12th partition
#'   Logger$Log("Starting Processing of a chunk", "MILESTONE")
#'   Logger$Log("Flux converter is ready", origin="preflight()")
#'   Logger$Log("Discarding invalid target date", "WARN", origin="Main loop")
#'
#' @family AALogger methods
NULL
AALogger$methods(
Log = function(msg, level="INFO", origin=NULL) {
  msgLogLevel <- match(level, LogLevels, nomatch=1)  # when not found, the message will be output systematically.

  if (msgLogLevel <= priv$logLevel || msgLogLevel <= priv$logLevel2) {
    if (is.null(origin)) {
      origin <- Origin
    } else {
      origin <- paste(Origin, origin, sep="~")
    }
    logEntry <- sprintf("%s [%s] [%s] - %s\n", strftime(Sys.time(), "%Y/%m/%d %H:%M:%S"), level, origin, msg)

    if (msgLogLevel <= priv$logLevel) { # *** Output to "main" Log ***
      if (!is.null(priv$logFile) ) {
        cat(logEntry, file=priv$logFile)
        flush(priv$logFile)
      } else {
        #TODO: ruggedize by removing (escaping?) pipe characters and the like...
        # note the dbl quotes in part of the echo command, to escape special chars the logEntry may contain.
        system(paste0('echo "', logEntry, '"'))
      }
    }

    if (msgLogLevel <= priv$logLevel2) { # *** Output to "main" Log ***
      if (!is.null(priv$logFile2) ) {
        cat(logEntry, file=priv$logFile2)
        flush(priv$logFile2)
      } else {
        #TODO: ruggedize by removing (escaping?) pipe characters and the like...
        # note the dbl quotes in part of the echo command, to escape special chars the logEntry may contain.
        system(paste0('echo "', logEntry, '"'))
      }
    }
  } # test if at least one log has verbosity such that a message will be sent.
}
)

#' Sets the Verbosity of the Log
#'
#' \code{AALogger$SetLogLevel} Sets the "Log Level" in order to control which log entries are effectively recorded/displayed.
#'
#' Any Log "event" submitted with a function such as \code{$Log} is ignored if its 'level' is lower
#' than the current Log Level.  For example, assuming the \code{LogLevels} list is the default one,
#' and if the current value of the Log Level is \code{"MILESTONE"} then calling
#' \code{$Log("Blah blah", "INFO")} will not produce any new entry in the log, however calling
#' \code{$Log("Blah blah", "WARN")} or \code{$Log("Blah blah", "MILESTONE")} will.
#'
#' The \code{logNr} argument is what allows to have \strong{multiple log outputs} (e.g. one to the console and
#' one to a file), with different verbosity.  (e.g. only up to MILESTONE events to the console,  every message
#' to the file; in this fashion relatively sober output to user but possibilty to dig in the file for debugging purposes)
#'
#' @name AALogger_SetLogLevel
#' @param newLevel character string : the Level below which log events do not get recorded.
#'   It should be one of the strings found in \code{$LogLevels}, or the special value "DISABLE" which
#'   effectively prevents any output of log messages regardless of their Log Level.
#' @param logNr integer : The log Number for which we are setting verbosity.  Defaults to 1, i.e. the
#'   main Log.  Currently accepted values are 1 and 2, other values are processed as for value 1.
#' @return character string. The Log Level \emph{previously} applicable.  This may be useful if
#'   some logic wants to temporarilly alter the log level and re-establish it to its original
#'   value at a later time.
#' @family AALogger methods
NULL
AALogger$methods(
SetLogLevel = function(newLevel, logNr=1) {
  if (logNr != 2) {
    retVal <- priv$logLevel
    .self$priv$logLevel <- .self$priv$getLogLevel(newLevel)
  } else {
    retVal <- priv$logLevel2
    .self$priv$logLevel2 <- .self$priv$getLogLevel(newLevel)
  }
  retVal
}
)

#' Configure the File where Log Entries are sent to
#'
#' \code{AALogger$SetLogFile} Specify a file/connection where the Log messages are displayed or recorded
#'   for a given Log.
#'
#' By default the logger will log to stderr().  This function allows designating any open file instead.
#'
#' The function automatically closes the file/connection previously associated with the
#' Logger (unless it was \code{stdout()} or \code{stderr()} or \code{NULL})
#'
#' When the \code{newFile} argument is a character string, the function opens this file, in append mode,
#' hence creating a new file if none existed or appending to an existing file otherwise.
#'
#' The \code{logNr} argument is what allows having \strong{multiple log outputs} (e.g. one to the console and
#' one to a file), optionally with different verbosity.  By default there is only one log output, to stderr(),
#' by using \code{$SetLogFile()} and \code{$SetLogLevel}, with a \code{logNr} value of 2, one can designate
#' an additional output log and optionally set a different LogLevel for it.  When one is done using the
#' extra output, it can be disabled by setting its verbosity (\code{SetLogLevel()} to "DISABLE"); the file
#' itself can either be close explicitly or by calling \code{SetLogFile()} for another file ('though more
#' typically for NULL.  (Attention setting the file to NULL as the effect of using the Session's console, by
#' way of ECHO commands, so merely setting the file to NULL will not disable output, only displace it).
#'
#' At this time, a maximum of two outputs are supported, though this limit could be increased if it proved to be
#' desirable.
#'
#'
#' @name AALogger_SetLogFile
#' @param newFile Either a character string with the path/name of a file or an \emph{opened} connection
#'   such as \code{stderr()} or the result of something like \code{file("myFile.log", "wt", blocking=FALSE)}.
#'   This can also be \code{NULL} to use a direct output   to the console, \emph{by way of} the system's ECHO
#'   command (this approach is useful when logging from SNOW worker nodes, because the stderr and stdout output
#'   are not sent back to the master node, whereby ECHO-produced output is).
#' @param logNr integer : The log Number for which we are setting the output.  Defaults to 1, i.e. the
#'   main Log.  Currently accepted values are 1 and 2, other values are processed as for value 1.
#' @param overwrite : logical \code{TRUE} for creating a new log file; \code{FALSE} for appending to an existing one.
#'
#' @family AALogger methods
AALogger$methods(
SetLogFile = function(newFile, logNr=1, overwrite=FALSE) {
  if (logNr != 2) {
    currFile <- .self$priv$logFile
  } else {
    currFile <- .self$priv$logFile2
  }

  if (!is.null(currFile) && !isatty(currFile) && currFile != stdout() && currFile != stderr()) {
    close(currFile)
  }

  if (logNr != 2) {
    .self$priv$logFile <- .self$priv$getFile(newFile, overwrite)
  } else {
    .self$priv$logFile2 <- .self$priv$getFile(newFile, overwrite)
  }
}
)


#' Safely invoke Logger$Log()
#'
#' \code{DoLog} Assert that an AALogger class named \code{Logger} exists and calls
#'   its \code{Log()} method with the provided arguments.
#'
#' The arguments for this function are the same as for \link{AALogger_Log}
#'
#' By convention, many IHSMarkit applications create and configure a global \code{AALogger} variable
#'   named \code{Logger} so that anywhere in the application's logic, all that is required to record
#'   log messages is something like \code{Logger$Log("Hello world", "INFO")}.   Such applications should
#'   continue invoking Logger functions in this fashion.
#'
#' \code{DoLog()} is a convenience function intended for "Logger-aware" libraries. It allows these
#' libraries' logic to use a one-liner idiom to record log messages, without having to worry if the
#' \code{Logger} is effectively available.
#'
#' \strong{In short}:
#' \enumerate{
#'    \item Application logic should use \code{Logger$Log(etc)}
#'    \item Library logic should use \code{DoLog{etc}}
#' }
#'
#' Alternatively, a library could either make the \emph{explicit} requirement that applications using
#' it should configure a \code{Logger} global variable, or assert for the existence of said variable
#' themselves.
#'
#' @examples
#' DoLog("The model 'myFancyModel' cannot be found; using default model", "WARN")
#' # If a suitable 'Logger' variable exists,
#' #   the above message gets sent to the log (assuming current verbosity setting of the log allows it)
#' # otherwise, the above snippet is just like a "No-Op"
#'
#' @family AALogger methods
#' @export
DoLog <- function(...) {
  if (exists("Logger") && class(get("Logger")) == "AALogger_class") {
    Logger$Log(...)
  }
}

# Test / Sample use for the Logger class
TestLogger <- function()
{
  # instantiate the Logger.  Typically only one logger per app or per significant subsystem.
  Logger <- AALogger()

  Logger$Log("Hello World", "INFO")
  badDevice <- 321
  Logger$Log(sprintf("Major malfunction with flux converter number %d.", badDevice), "FATAL")

  # From now on we'll be only recording FATAL and ERROR messages
  Logger$SetLogLevel("ERROR")
  # Following two should not print/record, but the thrid one should
  Logger$Log("Setup target date to 1900", "INFO")
  Logger$Log("Temperature in flux converter is too high", "WARN")
  fluxTemp <- "753 degree celsius"
  Logger$Log(sprintf("Flux converter is dangerously hot (%s)", fluxTemp), "ERROR")

  Logger$SetLogLevel("INFO")
  # instantiate another Logger to go to file
  Logger2 <- AALogger()
  myLogFile <- file("Test.log", "wt")
  Logger2$SetLogFile(myLogFile)

  Logger2$Log("This msg should go to Test.log file", "INFO")
  blownupCompressorId <- 444
  Logger2$Log(sprintf("Compressor or tank # %d exploded !!!", blownupCompressorId), "FATAL")

  Logger2$SetLogFile("Test_TWO.log")
  Logger2$Log("This msg should go to Test_TWO.log file", "WARN")

  Logger2$SetLogFile(stderr())  # has the effect of closing the file

  # Demo the "Origin" feature
  Logger$Origin <- "SNOW-12"  # 12th partition
  Logger$Log("Starting Processing of a chunk", "MILESTONE")
  Logger$Log("Flux converter is ready", origin="preflight()")
  Logger$Log("Discarding invalid target date", "WARN", origin="Main loop")

  # Demo of an extra output file
  Logger$SetLogFile("Test_Three.log", logNr=2)
  Logger$SetLogLevel("INFO", logNr=2)

  Logger$Log("This msg should go to console and to Test_Three.log file", "INFO")

  # close the file and prevent any output to 2nd log
  Logger$SetLogFile(NULL, logNr=2)
  Logger$SetLogLevel("DISABLE", logNr=1)

  # Test the DoLog() method
  DoLog("Hello", "INFO")
  Logger <- "whatever"
  DoLog("Hello", "INFO")

}

#TestLogger()
