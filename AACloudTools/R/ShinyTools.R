## ShinyTools.R
##
##   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016-2017
##
##   Author:  Marc Veillet
##

# idea: add log of every succesful (or failed?) publishing

# Extract the "Site Folder", i.e. the root where all Shiny apps are published
# @param verbose logical : when TRUE, error messages are sent to the console
# @return the site folder (a character string), or a negative number if the function failed
#     -1 : No config file: the current host is not a Shiny Server (typical when users are trying to publish on their own workstation)
#     -2 : Error while reading config file (plausibly permission errors)
#     -3 : Cannot find the entry where site_dir is defined.
GetShinySiteFolder <- function(verbose=FALSE) {
  CONFIG_FILE <- "/etc/shiny-server/shiny-server.conf"

  if (!file.exists(CONFIG_FILE)) {
    if (verbose)
      message("Error: this host is not a known Shiny Server!")
    return(-1)
  }

  tryCatch(
    {shinyConf <- readLines(CONFIG_FILE)},
    warning = function(w) {
      if (verbose)
        message(sprintf("%s"), w)
    },
    error = function(e) {
      shinyConf  <<- NULL
      if (verbose)
        message(sprintf("Error while reading Shiny config file: %s", e))
      return(-2)
    }
  )

  parsedConf <- stringr::str_match(shinyConf, "^([:blank:]*site_dir[:blank:]+)([^;]+)")
  retVal <- parsedConf[!is.na(parsedConf[, 3]), ][3]
  if (is.na(retVal)) {
    if (verbose)
      message(sprintf("Cannot find 'site_dir' entry in Shiny Config file"))
    retVal <- -3
  }

  retVal
}

GetShinyServerIpAddress <- function() {
  if (.Platform$OS.type == "unix") {
    retVal <- str_trim(system("hostname -I", intern = TRUE)[1])
  } else {
    stop("GetShinyServerIpAddress() is only meant to be called on Linux server.")
  }

  retVal
}


#' Publish a Shiny App to Shiny Server
#'
#' \code{PublishShinyApp} Publish a Shiny application from the development area to the location
#'   where it can be served from.
#'
#' This initial version of the function only allows publishing applications which are readily on
#' the Shiny server; in other words, one cannot publish \emph{directly} from, say, one's workstation to
#' the Shiny server, but instead one must bring the shiny application (typically in the context of
#' a whole RStudio project) to a "development" area on the Shiny Server, and publish it from there.
#'
#' Essentially the publishing is little more than the copying of the Shiny application's folder and
#' its contents to a subfolder somewhere below the "root" of all Shiny applications.
#'
#' Using the default values for the \code{extraPath} and \code{appName} arguments, the application gets
#' published so that the URL to use the applicatoin is
#'
#' \code{http://<server_ip>:3838/app/<basename_of_SrcFolder>}
#'
#' for example if the server is 10.45.89.34 and the \code{srcFolder} is passed as \code{"VisualTools/ModelListExplorer"}
#'
#' the URL is \code{http://10.45.89.34:3838/app/ModelLiftExplorer}
#'
#' Alternatively, if the \code{extraPath} was passed as \code{"JeffStuff/beta"}
#'
#' the URL would be \code{http://10.45.89.34:3838/app/JeffStuff/beta/ModelListExplorer}
#'
#' or, if the \code{appName} was passed as \code{"LiftExp_v1"} then the
#'
#' URL would be \code{http://10.45.89.34:3838/app/LiftExp_v1}
#'
#' @param srcFolder character string : the folder where the Shiny app is found.
#' @param extraPath character string : optional path, below the "root" of all published Shiny applications
#'   where the application should be published.
#' @param appName character string : the name of the application. This defaults to the basename of the
#'   folder where the application is copied from, but it can be used to provide alternative names, such as
#'   application nicknames or also names which indicate the version number.
#' @return logical: \code{TRUE} or {FALSE} depending if the publishing was successful or not, respectively.
#'   In case of errors, the function outputs messages indicating the nature of the problem and ways to
#'   remedy them.
#'
#' @examples
#' \dontrun{
#'    PublishShinyApp("Development/ShinyDev/timeseries_viz", extraPath="mjvTests", appName="Viz2")
#'    # [1] "The URL to the newly published Shiny application is"
#'    # [1] "  http:<server_ip>:3838/app/mjvTests/Viz2"
#'    # [1] TRUE
#'    #
#'    PublishShinyApp("Development/ShinyDev/FluxCapacitorTuner")
#'    # [1] "The URL to the newly published Shiny application is"
#'    # [1] "  http:<server_ip>:3838/app/FluxCapacitorTuner"
#'    # [1] TRUE
#' }
#' @export
PublishShinyApp <- function(srcFolder=".", extraPath=NULL, appName=NULL) {

  if (.Platform$OS.type != "unix") {
    shinyFolder <- -1  # not a valid Shiny server
  } else {
    shinyFolder <- GetShinySiteFolder(verbose = FALSE)
  }

  if (!is.character(shinyFolder)) {
    errMsg <-
    switch(as.character(shinyFolder),
           "-1" = c("The current host is not a valid Shiny server.",
                  "PublishShinyApp() must be run from a Shiny server such as the 10.45.89.34 host.",
                  "The recipe:",
                  "  1. Optionally, perform initial development of Shiny app locally; commit project to Git",
                  "  2. Open a session on the RStudio/Shiny server (e.g. http://10.45.89.34:8787 )",
                  "  3. Git pull the project with Shiny app; test and finalize development as needed",
                  "  4. call PublishShinyApp()"),
           "-2" = "Error while reading Shiny configuration file.",
           "-3" = "Invalid Shiny configuration file.",
           "Unexpected error.")

    writeLines(errMsg)
    writeLines("If you are sure you are invoking this function from a valid Shiny server, contact the AA Support team")

    stop("Fatal error, could not proceed (attempt to publish to a host that isn't a valid Shiny server or othr issue.")
  }

  if (srcFolder == ".")
    srcFolder <- basename(getwd())
  if (is.null(appName) || appName == "")
    appName <- basename(srcFolder)

  destSubFolder <- "/app"
  if (!is.null(extraPath) && extraPath != "") {
    destSubFolder <- paste0(destSubFolder, "/", extraPath)
  }

  if (!stringr::str_detect(destSubFolder, "/$"))
    destSubFolder <- paste0(destSubFolder, "/")
  destSubFolder <- paste0(destSubFolder, appName)

  destFullPath <- paste0(shinyFolder, destSubFolder)

  # ensure we start from an empty directory, lest we risk leaving some files from a previous publishing
  if (dir.exists(destFullPath)) {
    unlink(destFullPath, recursive = TRUE, force = TRUE)
  }
  dir.create(destFullPath, recursive = TRUE)

  # print(sprintf("Copying from %s ...", srcFolder))
  # print(sprintf("       to    %s", destFullPath))

  retVal  <- TRUE
  tryCatch(
    {file.copy(from = list.files(srcFolder, full.names = TRUE), to = destFullPath, overwrite = TRUE, recursive = TRUE)},
    error = function(e) {
      retVal  <<- FALSE
      message(sprintf("Error while copying Shiny applicaition: %s", e))
    }
  )

  if (retVal) {
    print("The URL to the newly published Shiny application is")
    print(sprintf("  http://%s:3838%s", GetShinyServerIpAddress(), destSubFolder))
  }

  retVal
}
