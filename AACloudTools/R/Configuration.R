###############################################################################
##
## Configuration.R
##
## This source file is part of Advanced Analytics AWS Cloud Tools
##
## Authors:  Christopher Lewis and Marc Veillet
##
## License: IHSMarkit (c)2016-2017- not to be shared outside the company
##
###############################################################################

# Note about namespace management (@import, Depends sections and all that...)
# AACloud tools uses extensively various stringr::* functions, hence it is
# appropriate to just @import this package (rather than using explicit :: notation)
# or @importFrom with a subset of the stringr functions.
#' @import stringr
NULL

# Locate the User's R Environment file
# R specification provide 3 possible locations for this file.  See, for e.g.
#    http://rviews.rstudio.com/2017/04/19/r-for-enterprise-understanding-r-s-startup/
#
#  @param createIfNeeded logical : used when a file cannot be found; when TRUE, this
#         function creates a small "empty" file. (except if the system-level environmental var
#         R_ENVIRON_USER is not empty, as it seems illogical to have this explicit location
#         pointing to a non existing file);  a file gets created however if neither of the
#         to other locations (implicit ones) do not yield a file.
# @return a character string with the full path of the R Environment file.
#
GetUserRenvironPath <- function(createIfNeeded=TRUE) {
  # First possible location: file pointed by R_ENVIRON_USER
  retVal <- path.expand(Sys.getenv("R_ENVIRON_USER"))
  if (retVal != "") {
    if (!file.exist(retVal))
      warn(sprintf("User 'Renviron' file', %s, is set to a non existing file", retVal))
    return(retVal)
  }

  # Second possible location : .Renviron file in current directory
  retVal <- file.path(getwd(), ".Renviron")
  if (file.exists(retVal)) {
    return(retVal)
  }

  # Thrid possible location : .Renviron file in user's home directory
  retVal <- file.path(path.expand("~"), ".Renviron")
  if (!file.exists(retVal)) {
    if (createIfNeeded) {
      writeLines(c("## .Renviron file",
                   "## Environmental variables for all R sessions started for this user",
                   ""),
                 con=retVal)
    } else {
      retVal <- ""
    }

  }
  return(retVal)
}

# Sets Key-Value pair(s) in a R Environment (or more broadly a typical KVP text file such
# as these use in Linux config files).
#   @param kvp named character vector : the list of the values with the names of the vector element
#     as the key.
#   @param fileName character string : the full path of the file where to add (or edit) the
#     key values pairs.  This path is often one provided by a function like GetUserRenvironPath()
#     but could well be that of any arbitrary file in any arbitrary location.  When NULL, defaults to
#     the User R Profile file (as per GetUserRenvironPath()).
#   @return integer : the number of key-values added/modified.
SetRenviron <- function(kvp, fileName=NULL) {
  # *** cursory argument validation
  if (is.null(kvp) || !is.character(kvp) || length(kvp) == 0 ||
      is.null(names(kvp))) {
    stop("Invalid 'kvp' argument : expecting a named character vector with at least 1 element.")
  }
  if (is.null(fileName) || fileName == "")
    fileName <- GetUserRenvironPath(createIfNeeded=TRUE)

  if (file.exists(fileName))
    envirLines <- readLines(fileName)
  else
    envirLines <- c("## environmental variables for R session", "")

  setOneEnvirVar <- function(envirLines, key, value) {
    keyEqVal <- sprintf("%s=%s", key, value)
    retVal <- envirLines
    lineToAlter <- which(str_detect(envirLines, sprintf("^[ ]*%s[ ]*=", key)))
    if (length(lineToAlter) == 0) {  # key is not readilly in file : append line
      retVal <- c(retVal, keyEqVal)
    } else {
      if (length(lineToAlter) > 1) {
        warning(sprintf("Found multiple instances of key '%s' in environment file; only changed the first occurrence.", key))
        lineToAlter <- lineToAlter[1]
      }
      retVal[lineToAlter] <- keyEqVal
    }
    retVal
  }

  for(i in seq_along(kvp)) {
    key <- names(kvp)[i]
    val <- kvp[i]
    envirLines <- setOneEnvirVar(envirLines, key, val)
  }

  writeLines(envirLines, con=fileName)

  length(kvp)
}

# Read all the Key-Value pairs in a REnviron-like file
#   @param fileName character string : the REnviron-like file.
#      Defaults to the User R Environment file, as per GetUserRenvironPath()).
#   @return a named character vector where the names are the keys and the elements'
#     values the values.
ReadRenviron <- function(fileName=NULL) {
  if (is.null(fileName))
    fileName <- GetUserRenvironPath(FALSE)

  if (!file.exists(fileName)) {
    return(character())   # no file : empty vector is the answer!
  }

  envirLines <- readLines(fileName)
  linesWithKvp <- which(str_detect(envirLines, "^[ ]*[^#= ]+[ ]*="))
  envirLines <- envirLines[linesWithKvp]
  parsedLines <- str_match(envirLines, "^([ ]*)([^#= ]+)([ ]*=[ ]*)([^#]+)")

  retVal <- str_trim(parsedLines[, 5])    # values
  names(retVal) <- parsedLines[, 3]       # keys
  retVal
}

# *********************************************************************************
#   Functions related to ConfigureAWS()
# *********************************************************************************
# All these functions start with the awsConf_ prefix
# Some of these function's first argument is `vars` an environment with shared variables and constants
# Splitting ConfigureAWS() into several smaller functions was necessary to improve the readability and the
# testability of this, now, relatively complicated body of logic.

# Create the 'vars' variable, an [R] environment used to bundle all the variables shared by
# ConfigureAWS() and various awsConf_* functions.  Rather than merely creating the container and
# having the other functions add to it as the logic progress, creating the variables here as well serves
# the main purpose of providing a central documentation for 'vars'.
awsConf_InitVars <- function() {
  vars <- new.env(parent=emptyenv())

  # Clear text associated with "location codes" such as used in the fallBackConfigs argument
  vars$LOC_CODES_TEXT <- c(ARG="ConfigJSON argument",
                           USER="User's Global config",
                           GUEST="Guest config")
  # character vector where we collect Human-readable throughout the process flows, as so to
  # provide the user with insight and actionable information in case of failure
  # (This approach allows keeping the ConfigureAWS() function "silent" when all goes well and verbose=FALSE)
  # See also awsConf_AddMsg() function
  vars$info <- character()

  # (not used anymore)
  # These two variables are used to locate the additional  files referred in the config file, in the
  # case that finding the config file itself required some "walking up" the directory structure.
  #  vars$argOrigFolder  <- NULL   # Original dirname() part of configJSON
  #  vars$argFoundFolder <- NULL   # dirname() of the effective path derived from configJSON

  vars$configFromArg <- NULL    # indicates that the config effectively used was derived from configJSON argument
  vars$effectiveConfigFolder <- NULL  # Folder where the "main" (JSON) config file was found.

  # This is the configuration that is eventually the return value of ConfigureAWS
  vars$conf <- list()

  vars
}

# Add message into the 'info' variable; this constitutes a local "log" of sorts, whereby all information
# pertaining to the configuration process is accumulated in the 'info' variable, and available for display
# to the user in case of error etc (or for curious users who run the config with verbose=TRUE)
awsConf_AddMsg <- function(vars, msg) {
  vars$info <- c(vars$info, msg)
}

# returns the path of the Configuration file for one of the alternate configuration "locations".
# @param loCode : one of "User" or "Guest"
awsConf_GetConfigFilePath <- function(locCode) {
  locCode <- toupper(locCode)
  if (locCode == "USER") {
    usrRenviron <- ReadRenviron()
    retVal <- usrRenviron["AACLOUDTOOLS_CONFIG_FILE"]
    if (is.na(retVal))
      retVal <- ""
    return(retVal)
  }

  if (locCode == "GUEST") {
    warning("ConfigureAWS():  future implementation for fallBackConfigs code 'GUEST';  code ignored for now")
    return("")
  }
}

# Locates a config file (whether the config per-se or the password file or the cluster file etc.), when
# it is not found _exactly_ at the path provided in configJSON.
# This function was created to
#   - circuvent a idiosyncrasis of testthat
#   as well as to
#   -  allow several sub-applications to share the application's config file, stored somewhere
#      "above" them in the folder hierarchy.
# @param: filePath character string: the path to the file, including the file name and generally as found.
# @return a character string with the [absolute] path to where the file was found, or NULL when the
#  search failed.
awsConf_FindConfigFilePath <- function(filePath) {
  if (file.exists(filePath)) {
    return(normalizePath(filePath))
  }

  fileName <- basename(filePath)
  path     <- dirname(filePath)
  if (dirname(path) != path) folderName <- basename(path) else folderName = NULL

  path    <- dirname(filePath) # start one-up since the file wasn't found in filePath as-is)

  absPath <- PathSimplify(normalizePath(path, winslash="/", mustWork=FALSE))
  if (PathIsRelative(absPath))    # fail safe on linux where when the path doesn't exist, normalizePath
    absPath <- getwd()            # returns its 'path' argument unchanged.

  nxtPath <- absPath
  curPath <- ""
  retVal <- NULL
  while(nxtPath != curPath) {
    # look in directory...
    curPath <- nxtPath
    retVal <- file.path(curPath, fileName)
    if (file.exists(retVal))
      break
    # look in folder, down from curPath
    if (!is.null(folderName)) {
      retVal <- file.path(curPath, folderName, fileName) # note: the first time around, this is redundant/impossible
      if (file.exists(retVal))
        break
    }

    # continue searching, One level up from directory
    nxtPath <- dirname(curPath)
  }

  if (!file.exists(retVal))
    retVal <- NULL
  retVal
}

# returns the path to the config file that should be used.
# This function locates the config file by searching in the following order:
#  1. the "ARG" location i.e. the one produced from the configJSON argument to ConfigureAWS()
#     This location is not searched at all if the configJSON argument is NULL.
#     Otherwise it is first search "as-is" and if not found, and if the findConfigFolder argument is TRUE,
#     an attempt is made at locating the file "somewhere above" in the directory hierarchy
#     (see awsConf_FindConfigFilePath() )
#     BTW, this kind of search is only applicable to the "ARG" location, as the alternate locations
#     are expected to be "global", defined by an absolute path, and hence it doesn't make sense to look
#     for the config file elsewhere.
#  2. The alternate locations such as
#        "User", i.e. the global AWS configuration for the current user
#        "Guest", i.e. the global AWS configuration used for Guests
#    The specific alternate locations searched and the search order are defined by the fallBackConfigs argument.
awsConf_GetConfigFile <- function(vars, configJSON, fallBackConfigs, findConfigFolder) {
  if (is.null(configJSON)) {
    awsConf_AddMsg(vars, "'configJSON' argument is NULL: skipping search of locations derived from it.")
    configPath <- ""
  } else {
    configPath <- configJSON
    #   Attempt to find the config file by walking "up" the directory structure.
    if (!file.exists(configPath) && findConfigFolder) {
      awsConf_AddMsg(vars, "  Looking for Config Folder by walking up the path...")
      actualpath <- awsConf_FindConfigFilePath(configPath)
      if (is.null(actualpath)) {
        awsConf_AddMsg(vars, "  ... not found.")
      } else {
        # Memorize the fact that we "walked up the directory path" to find the config file.
        # These vars may be used to find the files referenced inside the config file itself.
        # vars$argOrigFolder  <- dirname(configPath)
        # vars$argFoundFolder <- dirname(actualpath)
        configPath <- actualpath
        awsConf_AddMsg(vars, sprintf("  ... found it : %s", configPath))
      }
    }
    configPath <- normalizePath(configPath)
  }

  vars$configFromArg <- file.exists(configPath)
  if (vars$configFromArg) {  # *** Use the 'configJSON' file if it was found (directly of by "walking up" the dir).
    awsConf_AddMsg(vars,
                   sprintf("  File referred by %s exists; Using it!", vars$LOC_CODES_TEXT["ARG"])
    )
  } else {              # *** Try alternate locations if not found based on 'configJSON' argument.
    if (length(fallBackConfigs) == 0) {
      awsConf_AddMsg(vars, c(
        sprintf("  FATAL: File %s not found and no alternative locations suggested ('fallBackConfigs' argument).",
                configPath),
        "SUGGESTIONS to fix this issue :",
        "  - Ensure the current working directory is as expected (if you intend to use a 'local' config)",
        "  - Ensure config file is effectively there",
        "  - Allow looking into alternative global locations by using some codes in the 'fallBackConfigs' argument.",
        "    (you'll also need to ensure that a valid configuration is found at this/these alternate locations if",
        "     that is not readily the case; see AACloudTools::SetGlobalConfigLocation() )"
      )
      )
      print(vars$info)
      stop("ConfigureAWS() failed.")
    }

    if (!is.null(configJSON)) {
      awsConf_AddMsg(vars, "  Could not find config file as per 'configJSON' argument; looking into global locations.")
    }

    # loop through alternative locations until a config file is found there
    while(length(fallBackConfigs) > 0 && !file.exists(configPath)) {
      locationCode <- toupper(fallBackConfigs[1])
      fallBackConfigs <- fallBackConfigs[-1]

      configPath <- awsConf_GetConfigFilePath(locationCode)
      if (configPath == "") {
        awsConf_AddMsg(vars,
                       sprintf("  Alt location %s (%s) is not setup. Skipping this location.",
                               locationCode, vars$LOC_CODES_TEXT[locationCode]))
      } else {
        if (!file.exists(configPath)) {
          awsConf_AddMsg(vars,
                         sprintf("  Alt location %s points to %s but this file does not exist.",
                                 locationCode, configPath))
        }
      }
    }

    if (file.exists(configPath)) {
      awsConf_AddMsg(vars,
                     sprintf("*** Using %s file (%s) ***", vars$LOC_CODES_TEXT[locationCode], configPath))
    } else {
      awsConf_AddMsg(vars, c(
        "FATAL: Exhausted all possibilities; could not find configuration file.",
        "SUGGESTIONS to fix this issue :",
        "  - Produce a config file and its associated files (e.g. copy these",
        "    from AACloudQuickStartTemplate project's Config folder)",
        "  - Edit these files as appropriate, with your personal credentials etc.",
        "  - Ensure that these files are placed in a location that matches the 'configJSON'",
        "    argument to configAWS()  (recommended location is in 'Config' folder just below.",
        "    the current working directory).",
        " OR (if you seek to use an alternate location, shared by multiple applications)",
        "  - Ensure a config file effectively exists as said location",
        "   or, if an alternate config hasn't readily been put in place:",
        "  - Produce a config file and its associated files at said location",
        "  - Invoke AACloudTools::SetGlobalConfigLocation();",
        "      This one-time action will ensure that the configuration is valid and",
        "      will record this globally so that future calls to configureAWS() can make",
        "      use of this location."
      )
      )
      print(vars$info)
      stop("ConfigureAWS() failed.")
    }
  }

  vars$effectiveConfigFolder <- dirname(configPath)
  configPath
}

# Parse the main config file, from JSON file to an R list;
# Returns the R list or otherwise report the error and abort.
# This function could be made more generic to be used in other contexts, however
# it is convenient to have it handle all the "logging/reporting" in a
# ConfigureAWS()-specific fashion.
# @param jsongFile Name+Path of JSON file
# @return a list which replicates the hierachical structure of the JSON file.
awsConf_ParseJson <- function(vars, jsonFile, isJsonTxt) {
  if (isJsonTxt) {
    awsConf_AddMsg(vars, c("'configJSON' argument supplied JSON text rather than a file path.",
                           sprintf("Using this JSON text as-is  (%d) lines.", length(jsonFile)))
    )
  } else {
    if (!file.exists(jsonFile)) {
      awsConf_AddMsg(vars, sprintf("FATAL: Cannot find file %s", jsonFile))
      print(vars$info)
      stop("ConfigureAWS() failed.")
    }
    awsConf_AddMsg(vars, sprintf("  Parsing JSON file %s", jsonFile))
  }
  cfg <- NULL
  tryCatch(
    cfg <- jsonlite::fromJSON(jsonFile),
    warning = function(w) {
      awsConf_AddMsg(vars, sprintf("WARNING while parsing JSON file: %s", w))
    },
    error   = function(e) {
      awsConf_AddMsg(vars, sprintf("ERROR while parsing JSON file: %s", e))
      cfg <- NULL
    }
  )

  if (is.null(cfg)) {
    awsConf_AddMsg(vars, c(
      "FATAL: Could not parse configuration file.",
      "SUGGESTIONS to fix this issue:",
      "  - Edit the file with proper JSON syntax and try again."
    )
    )
    print(vars$info)
    stop("ConfigureAWS() failed.")
  }
  cfg
}

# Parse and validates the S3 elements of the config; add these to the config if they are ok;
# issue error and warning messages as appropriate.
# @param vars         : shared variables; only used for awsConf_AddMsg() or in case of fatal error.
# @param rawConf list : the "raw" config, as parsed from JSON
# @param outConf list : the output config, i.e. that which will eventually be ConfigureAWS()'s output.
awsConf_ParseS3 <- function(vars, rawConf, outConf) {
  s3 <- rawConf$configuration$S3
  if (is.null(s3)) {
    awsConf_AddMsg(vars, "   Note: AWS s3 configuration settings were absent from config file.")
  } else {
    outConf$s3$access_key_id     <- s3$access_key_id
    outConf$s3$secret_access_key <- s3$secret_access_key

    if (strIsNullOrEmpty(outConf$s3$access_key_id) ||
        strIsNullOrEmpty(outConf$s3$secret_access_key)) {
      awsConf_AddMsg(vars,c("FATAL: invalid config values.",
                            "S3$access_key_id and S3$secret_access_key cannot be NULL or empty")
      )
      print(vars$info)
      stop("Invalid configuration.")
    }

    if (is.null(s3$s3TempFolder)) {
      outConf$s3$s3TempFolder <- "s3://ihs-temp/"  # For backward compatibility
    } else {
      outConf$s3$s3TempFolder <- s3$s3TempFolder
    }

    if (is.null(s3$s3DataFolder)) {
      outConf$s3$s3DataFolder <- "s3://ihs-bda-data/projects/" # For backward compatibility
    } else {
      outConf$s3$s3DataFolder <- s3$s3DataFolder
    }
  }

  outConf
}

# Parse and validates the section of the config relevant to Redshift; add this section to the
# overall config if it is ok; issue error and warning messages as appropriate.
# @param vars         : shared variables
# @param rawConf list : the "raw" config, as parsed from JSON
# @param outConf list : the output config, i.e. that which will eventually be ConfigureAWS()'s output.
awsConf_ParseRedshift <- function(vars, rawConf, outConf) {
  rsConfig <- rawConf$configuration$redshift

  if (is.null(rsConfig)) {
    awsConf_AddMsg(vars, "   Note: Redshift configuration settings were absent from config file.")
    return(outConf)
  }

  if (strIsNullOrEmpty(rsConfig$UserPasswordFile)) {
    awsConf_AddMsg(vars, " FATAL: 'UserPasswordFile' is a required field in 'redshift' section of JSON config file.")
    print(vars$info)
    stop("Invalid configuration")
  }

  # Deal with missing "User+Password" file; this is complicated as there are different circumstances
  # each with very distinct appropriate reations.
  if (!file.exists(rsConfig$UserPasswordFile)) {
    # Config provided an bsolute path: end-of-the-road: we cannot/shouldnot attempt to "rescue" abs. paths.
    if (!PathIsRelative(rsConfig$UserPasswordFile)) {
      awsConf_AddMsg(vars,  c(
        "  FATAL: User+Password file was not found",
        "SUGGESTION to fix this issue :",
        "  Ensure that the 'UserPasswordFile' element either ...",
        "    - ... point to a file in the same folder as the config file itself or",
        "    - ... uses an absolute path.",
        " The relative paths are recommended for the configuration sets found within the application",
        " The absolute ones are for when the configuration is shared between applications.")
      )
      print(vars$info)
      stop("Invalid configuration")
    }

    upfFolder <- dirname(rsConfig$UserPasswordFile)
    upfName   <- basename(rsConfig$UserPasswordFile)

    awsConf_AddMsg(vars, c(
      sprintf("  User+Password file not found at %s", rsConfig$UserPasswordFile),
      "    Attempting to this file elsewhere.")
    )

    # Try the simplest -and, frankly, maybe only setup we'll eventually allow. when it comes to relative paths- :
    # is it in same folder as the main config file ?
    defaultAndBestPath <- file.path(vars$effectiveConfigFolder, upfName)
    if (file.exists(defaultAndBestPath)) {
      rsConfig$UserPasswordFile <- defaultAndBestPath
    } else {
      # otherwise try to look for it "up" in the directory tree...
      rsConfig$UserPasswordFile <- awsConf_FindConfigFilePath(rsConfig$UserPasswordFile)
      # ... and if that fails, assume the file does not _yet_ exists and needs to be created, in the same
      # folder as the main config.
      if (is.null(rsConfig$UserPasswordFile))
        rsConfig$UserPasswordFile <- defaultAndBestPath
    }
  }

  if (!file.exists(rsConfig$UserPasswordFile)) {
    # The automatic copy of the template is only applicable when the config file comes form the 'configJSON' argument.
    if (!vars$configFromArg) {
      awsConf_AddMsg(vars, c(
        "  FATAL: User+Password file was not found",
        "SUGGESTION to fix this issue :",
        "  Ensure that the 'UserPasswordFile' element either ...",
        "    - ... point to a file in the same folder as the config file itself or",
        "    - ... uses an absolute path.",
        " The relative paths are recommended for the configuration sets found within the application",
        " The absolute ones are for when the configuration is shared between applications.")
      )
      print(vars$info)
      stop("Invalid configuration")
    }

    # User/password file not found: copy the template of this file so that all the user has to
    # do is fill-in his/her credentials in it.
    namePwdTemplate <- c(
      '{',
      '  "Username": "your_redshift_username"',
      '  "Password": "your_redshift_password"',
      '}'
    )
    writeLines(namePwdTemplate, con=rsConfig$UserPasswordFile)

    awsConf_AddMsg(vars, sprintf("  Redshift Name+Password file is: %s", rsConfig$UserPasswordFile))
  }

  # Get [Redshift] username and password
  #   jsonlite::fromJSON doesn't always understand the 'url' passed to it is effectively a url
  #   hence the two steps: read to string and fromJSON the json text instead.
  tryCatch( {
    jsonText <- readLines(rsConfig$UserPasswordFile)
    rsUsernamePassword <- jsonlite::fromJSON(jsonText)
  },
  warning=function(w) { awsConf_AddMsg(vars, "WARNING while parsing Redshift Name+Password file: %s", w)},
  error=function(e) {
    awsConf_AddMsg(vars, sprintf("ERROR while parsing Name+Password file: %s", e))
    rsUsernamePassword <- NULL
  }
  )
  if (is.null(rsUsernamePassword)) {
    awsConf_AddMsg(vars, c("FATAL: Could not parse configuration file.",
                           "SUGGESTIONS to fix this issue:",
                           sprintf("  - Edit the %s file and try again", rsConfig$UserPasswordFile)
    )
    )
    print(vars$info)
    stop("ConfigureAWS() failed.")
  }

  # detect the fact that the user/password is still a template, not yet filled-in by user
  if (rsUsernamePassword$Username == "your_redshift_username") {
    awsConf_AddMsg(vars, c( "FATAL : the Redshift User+Password file is still a copy of the template.",
                            "SUGGESTIONS to fix this issue:",
                            sprintf("  - Edit the %s file by ", rsConfig$UserPasswordFile),
                            "    replacing the template text with your Redshift login credentials."
    )
    )
    print(vars$info)
    stop("ConfigureAWS() failed.")
  }

  # *** We now have all the elements of the Redshift config; bundling these in the output config
  outConf$redshift$Username    <- rsUsernamePassword$Username
  outConf$redshift$Password    <- rsUsernamePassword$Password

  outConf$redshift$Hostname    <- rsConfig$Hostname
  outConf$redshift$Database    <- rsConfig$Database
  outConf$redshift$Port        <- rsConfig$Port
  outConf$redshift$DriverClass <- rsConfig$DriverClass
  outConf$redshift$UrlToRedshift <-
    paste0("jdbc:redshift://", rsConfig$Hostname, ":", rsConfig$Port, "/", rsConfig$Database)

  # *** Basic validation of this config
  if (strIsNullOrEmpty(outConf$redshift$Hostname) ||
      strIsNullOrEmpty(outConf$redshift$Port)     ||
      strIsNullOrEmpty(outConf$redshift$DriverClass) ||
      strIsNullOrEmpty(outConf$redshift$Username)  ||
      strIsNullOrEmpty(outConf$redshift$Password) ) {
    awsConf_AddMsg(vars, c("FATAL: invalid config values.",
                           "redshift parameters 'Hostname', 'Port', 'DriverClass', UserName and Password",
                           "cannot be NULL or empty")
    )
    print(vars$info)
    stop("Invalid configuration.")
  }

  # todo : maybe a actual test of the Redshift config)

  outConf
}

# Parse and validates the section of the config relevant to the SNOW cluster;
# add this section to the overall config if it is ok; issue error and warning messages as appropriate.
# @param vars         : shared variables; only used for awsConf_AddMsg() or in case of fatal error.
# @param rawConf list : the "raw" config, as parsed from JSON
# @param outConf list : the output config, i.e. that which will eventually be ConfigureAWS()'s output.
awsConf_ParseSnowCluster <- function(vars, rawConf, outConf) {
  clusterConfigFileName <- rawConf$configuration$cluster$configFilename
  if (strIsNullOrEmpty(clusterConfigFileName)) {
    awsConf_AddMsg(vars, c("   Note: SNOW cluster configuration settings were absent from config file.",
                           "         (a valid situation; No SNOW cluster will be available for this application)"))
  } else {
    # TODO: @@@2 here: adjust the clusterConfigFileName if need be:  we'll do once the logic for the password file is tested.
    outConf$configuration$cluster$id_ip <- read.table(file=clusterConfigFileName, sep="," , header=TRUE)
  }
  outConf
}


# Copy some of the parameters from the AWS configuration to OS-level environmental
# variables so that various AACloud tools functions (as well as some AWS commands)
# can find them when they are not passed explicitly.
awsconf_SetSysEnv <- function(configAWS) {
  # S3 stuff
  if (!is.null(configAWS$s3$access_key_id)) {
    Sys.setenv(AWS_ACCESS_KEY_ID     = configAWS$s3$access_key_id,
               AWS_SECRET_ACCESS_KEY = configAWS$s3$secret_access_key,
               AWS_DEFAULT_REGION    = "us-west-2",
               S3TEMPFOLDER          = configAWS$s3$s3TempFolder,
               S3DATAFOLDER          = configAWS$s3$s3DataFolder)
  }
  # Redshift stuff
  if (!is.null(configAWS$redshift$Hostname)) {
    Sys.setenv(PGCLIENTENCODING = "UTF8",
               PGHOSTNAME       = configAWS$redshift$Hostname,
               PGDBNAME         = configAWS$redshift$Database,
               PGPORT           = configAWS$redshift$Port,
               PGDRIVERCLASS    = configAWS$redshift$DriverClass,
               PGURLRS          = configAWS$redshift$UrlToRedshift)

    if (!is.null(configAWS$redshift$Username)) {
      Sys.setenv(PGUSER     = configAWS$redshift$Username,
                 PGPASSWORD = configAWS$redshift$Password)
    }
  }
}

#' Configure the Parameters pertaining to AWS Services.
#'
#' \code{ConfigureAWS} Load the configuration used by various functions of the AACloudTools
#'   package to access AWS services (S3, Redshift, EC2 instances, SNOW cluster...)
#'
#' \code{ConfigureAWS} must be called once, typically during the initialization phase of the
#'   application, so that the underlying parameters (Account IDs, IP addresses, Options...)
#'   are available to the various functions of this package as they provide access to AWS
#'   services.
#'
#' The primary AWS configuration data is held in the \code{config.json} file which is,
#' \emph{by convention}, found in the \code{./Config/} folder, just below the application's
#' working directory.  It optionally references two additional files:
#' \itemize{
#'   \item Redshift credentials file (\code{redshift_username_password.json})
#'   \item SNOW cluster config file
#' }
#' These files are also found in the \code{./Config/} folder and the Redshift Credentials
#' file should be named as indicated, although these conventions can be overridden since
#' the name and path of both files are in the primary config file.  There are many good reasons
#' to adhere to these conventions, including the fact that by default the Redshift Credentials
#' file is excluded from the Source Control (Git), keeping each colleague's credentials private.
#' The name of the SNOW cluster configuration file varies, as the cluster is typically
#' applicaiton-specific and is hence typically named after the application along with
#' the 'snow' keyword and a \code{.dat} extension as in \code{bda_snow_ProjectBravo.dat}.
#'
#' @param configJSON character string: name and path to the configuration file.
#'   Use \code{NULL} to prevent using this location (i.e. to only consider the
#'   \code{fallBackConfigs} locations.)
#' @return A tree structure based on a list of lists where related parameters are
#'   grouped near one another. This return value is required as an argument to
#'   several of the functions in this package, so that these functions can refer to
#'   configuration parameters such as  \code{theConfig$redshift$Hostname} or
#'   \code{theConfig$s3$access_key_id}.  As a side-effect, some configuration parameters
#'   are also copied to Environment Variables so they can be accessed implicitly.
#'
#' @examples
#' # Configuration for the AWS services
#' configAWS <- ConfigureAWS("./Config/config.json")
#' # ...
#' # now we can use such services, for example
#' # here, with a function which requires the 'configAWS' parameter
#' mySnowCluster <- AAStartSnow(local=FALSE, configAWS)
#' # or, here, with a function which gets the config parameters from Environment Variables
#' PizzaCount <- GetRecordCount("RestaurantDB.PizzaRecipes")
#'
#' @export
ConfigureAWS <- function(configJSON="", fallBackConfigs="User", verbose=FALSE, findConfigFolder=TRUE) {

  vars <- awsConf_InitVars()

  # *** Default values ***
  if (is.null(fallBackConfigs))
    fallBackConfigs <- character()
  if (identical(configJSON, ""))
    configPath <- "Config/config.json"
  else
    configPath <- configJSON

  # Detect when configJSON supplied us with immediate JSON text rather a path (or NULL)
  configPath_isJsonTxt <- (is.character(configPath) && any(grepl("{", configPath, fixed=TRUE)))

  if (configPath_isJsonTxt) {
    configPath_Display <- "<Immediate JSON text>"
  } else {
    if (is.null(configPath))
      configPath_Display <- "<NULL>"
    else
      configPath_Display <- configPath
  }

  awsConf_AddMsg(vars,
                 c("ConfigureAWS():",
                   "  function called as: (after default parameter values substitution)",
                   sprintf("    configureAWS(configJSON='%s', fallBackConfigs=%s, verbose=%s)",
                           configPath_Display,
                           paste0("c(", paste0("'", fallBackConfigs, "'"), ")", collapse=", "),
                           verbose
                   ),
                   sprintf("  Current Working Directory is: %s", getwd())
                 )
  )

  # ***** First, (an unless configJSON provided directly JSON text), figure out which file to use ****
  # awsConf_GetConfigFile() tells us where to read the config from (or stops if it
  # cannot find it).
  if (!configPath_isJsonTxt) {
    configPath <- awsConf_GetConfigFile(vars, configPath, fallBackConfigs, findConfigFolder)
    configPath <- normalizePath(configPath, winslash="/", mustWork=FALSE)
  }

  # ***** Second, parse config file and intialize config objects *****
  rawConf <- awsConf_ParseJson(vars, configPath, configPath_isJsonTxt)
  # Cumulate each of the config sections into vars$conf
  vars$conf <- awsConf_ParseS3(vars,          rawConf, vars$conf)
  vars$conf <- awsConf_ParseRedshift(vars,    rawConf, vars$conf)
  vars$conf <- awsConf_ParseSnowCluster(vars, rawConf, vars$conf)

  awsconf_SetSysEnv(vars$conf)

  if (verbose) {
    print(vars$info)
  }

  vars$conf
}

#' Return the Redshift URL as supplied in the AWS configuration.
#'
#' \code{GetRedshifUrl} Get the full Redshift URL (including username and password).
#'
#' The returned value is suitable for use as the \code{url} argument of the
#' \code{SparkR::read.df()} function (for a \code{"com.databricks.spark.redshift"}) source.
#'
#' @param awsConfig object: Configuration object such as returned by the
#'   \code{ConfigureAWS()} funtion.  When \code{NULL}, an attempt is made to use
#'   the environmental variables which are normally set by a call
#'   to \code{ConfigureAWS()}.
#'
#' @return A string with the URL and including the UserName and Password.  This
#'   value can be used as configuration / argument to various functions which
#'   access Redshift (e.g. \code{SparkR::read.df()})
#'
#' @examples
#' # Configuration for the AWS services
#' AwsConfig <- ConfigureAWS("./Config/config.json")
#' # ...
#' rsUrl <- GetRedshifUrl(AwsConfig)
#' # ...
#' sdfMyData <- SparkR::read.df(
#'   path = NULL,
#'   source = "com.databricks.spark.redshift",
#'   tempdir = "s3a://ihs-temp/myProject/temp",
#'   query = "SELECT lrno, vesselname FROM ra.ABSD_SHIP_SEARCH WHERE flag = 'USA'",
#'   schema =  structType(
#'               structField("lrno", "string"),
#'               structField("vesselname", "string")
#'             ),
#'   url = rsUrl)
#'
#' @export
GetRedshifUrl <- function(awsConfig = NULL) {
  if (!is.null(awsConfig) && !is.null(awsConfig$redshift)) {
    url <- awsConfig$redshift$UrlToRedshift
    usr <- awsConfig$redshift$Username
    pwd <- awsConfig$redshift$Password
  } else {
    url <- Sys.getenv("PGURLRS")
    usr <- Sys.getenv("PGUSER")
    pwd <- Sys.getenv("PGPASSWORD")
  }

  if (is.null(url) || url == "" || is.null(usr) || usr == "" || is.null(pwd) || pwd == "") {
    stop("GetRedshifUrl() is missing info.  Have you called ConfigureAWS()?")
  }
  sprintf("%s?user=%s&password=%s", url, usr, pwd)
}

#' Basic Initialization of a SNOW Worker
#'
#' \code{ConfigureSnowWorker} Configure an individual SNOW Worker
#'
#' This function must be called for each SNOW worker, before any other processing is submitted to it.
#' Its purpose is to setup the system so it can use the AWS services, to ensure that the work directory
#' is properly set and generally make the environment on the individual workers compatible with the
#' logic tested on stand-alone hosts.
#' @param workingDirBaseName character string: directory name (not path) typically the application or project name
#' @param configAWS object returned by \code{ConfigureAWS}
#' @examples
#' \dontrun{
#' # ...
#' myResults <- foreach(partition=allPartitions) %dopar% {
#'     ConfigureSnowWorker(workingDirBaseName, configAWS)
#'     PerformSomeWork(SomeParam, partition$start, partition$count, SomeOtherParam)
#'   }
#' }
#' @export
ConfigureSnowWorker <- function(workingDirBaseName, configAWS) {
  awsconf_SetSysEnv(configAWS)

  # kludge to prevent moving the working directory when in a testthat context
  isUnitTesting <- (basename(getwd()) == "testthat")

  if (basename(getwd()) != workingDirBaseName && !isUnitTesting) {
    # By default the first instance of the worker will run in the home directory: ~snow
    # If that is the case, create the working directory if it does not exist and switch to it
    if (!dir.exists(workingDirBaseName))
      dir.create(workingDirBaseName)
    setwd(workingDirBaseName)
  }
}


#' Setup a configuration at a global location
#'
#' \code{SetGlobalConfigLocation} Configure the host so that the AWS configuration files can be shared
#'   by multiple applications.
#'
#' A global configuration is Blah, blah blah..
#'
#' @param globalConfigType character string : \code{"User"} for a global configuration
#'   for the current User; \code{"Guest"} for a global configuration made available to guests
#'   on the current host.  ("Guest" is future implementation.)
#' @param globalConfigFolder character string : the folder where the global configuration file(s)
#'   is (are) to be located. If \code{copyFromFolder} argument is equal \code{globalConfigFolder}, this folder
#'   must exist and contain the necessary files, otherwise, this folder will be created, if necessary,
#'   and the files will be copied from the \code{copyFromFolder} location.
#' @param configFileName character string : the name of the "main" configuration file for this set.
#' @param copyFromFolder character string : the folder where existing config file(s) is (are) to be used
#'   to produce the config file(s) for the global configuration. Should be \code{NULL}, if we
#'   wish to use the \code{CopyFromConfig} argument.
#' @param copyFromConfig character string : a config object, such as the one returned by
#'   \code{ConfigureAWS()} function.  This "in-memory" config gets copied.   @@@ see if useful...
#' @return adfasdf
#'
#' @examples
#' \dontrun{
#' # ...
#' myResults <- foreach(partition=allPartitions) %dopar% {
#'     ConfigureSnowWorker(workingDirBaseName, configAWS)
#'     PerformSomeWork(SomeParam, partition$start, partition$count, SomeOtherParam)
#'   }
#' }
#' @export
SetGlobalConfigLocation <- function(globalConfigType="User",
                                    globalConfigFolder="~/Config",
                                    configFileName="config.json",
                                    copyFromFolder=globalConfigFolder,
                                    copyFromConfig=NULL) {

}

# ***** Directory utilities *****
# These could be
#    a) moved elsewhere
#    b) made public

# Split a path into individual folders and file
# @arg p character string : any path, with or without a file name, a drive name, abolute or relative
# @return a character vector with one element per folder/file/drive name
PathSplit <- function(p) {
  nxtUp <- dirname(p)
  if (nxtUp == p) p else c(PathSplit(nxtUp), basename(p))
}

# Test if path is relative [to current working directory]
# @ arg path character string : [file system] path to be tested
# @ return boolean : TRUE if path is relative, FALSE if not, NA if path
# Note the ambivalent case of say, "D:"   return value is FALSE, which is
# mostly true, as the path is not relative to getwd() but
# also false in a sense that as the path is not absolute but relative to whatever
# the current directory _on_ D: drive is
# Also note that although the function was designed for file system paths, it
# -correctly- returns FALSE for any URL-looking string that have a protocol
# prefix such as ftp:// or sql: etc.
PathIsRelative <- function(path) {
  # Special cases:
  if (is.null(path) || is.na(path))
    return(NA)
  if (path == "")
    return(TRUE)
  if (str_sub(path, 1, 2) == "\\\\")
    return(FALSE)
  if (str_detect(path, ":"))
    return(FALSE)
  # General case:
  PathSplit(path)[1] == "."
}

# Remove elements of a path with some .. (next folder up) references, when possible
# For example:   Alpha/Bravo/../Charlie/Delta/..  --> Alpha/Charlie
# 'path' argument  should not include the file name; only folders and drive if applicable.
PathSimplify <- function(path) {
  if (!str_detect(path, "/\\.\\."))
    return(path)  # no /.. : nothing to simplify !

  pathParts <- PathSplit(path)
  # So long as we find a non ".." preceding a ".." (corresponding to
  # an "xxxx/.."  folders sequence in the path), we remove it and repeat.
  again <- TRUE
  while(again && length(pathParts) > 1) {
    again <- FALSE
    for(i in 2:(length(pathParts))) {
      if (pathParts[i] == ".." && pathParts[i - 1] != "..") {
        pathParts <- pathParts[-c(i - 1, i)]
        again <- TRUE
        break;
      }
    }
  }

  # if first element is the root (or the drive, in Windows), we
  # paste it to the next element, lest the / would be doubled with
  # our subsequent logic to collapse the vector back to a string.
  if (str_detect(pathParts[1], "/$") && length(pathParts) > 1)
    pathParts <- c(paste0(pathParts[1], pathParts[2]), pathParts[-(1:2)])

  paste0(pathParts, collapse="/")
}

# stringr's missing function ;-)
# returns TRUE if string is NULL or emtpy
strIsNullOrEmpty <- function(s) {
  is.null(s) || (s == "")
}

# Check if Host is a _Shared_ RStudio Server
# By design, the implementation is based on list of known RStudio Server within IHS network, rather
# than, say, sensing some deamon or other artefacts which are specific to the RStudio Server platform.
IsHostRStudioServer <- function() {
  if (.Platform$OS.type == "unix") {
    knownRStudioSvr <- c("10.45.89.34",    # AA LDAP-less
                         "10.44.87.171",   # ECR LDAP-less
                         "10.45.89.215")   # EAA web server (accessorily an RStudio Server)
    ipAddr <- stringr::str_trim(system("hostname -I", intern=TRUE)[1])
    retVal <- ipAddr %in% knownRStudioSvr
  } else {
    retVal <- FALSE  # only linux hosts are RStudio Servers
  }

  retVal
}
