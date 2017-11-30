context("Configuration functions")


# ************  Setup utilities ***********************************************

# Return a test config in its  "in-memory"  format, i.e. the same format as that used by ConfigureAWS()
getTestConfig <- function(s3=TRUE, redshift=TRUE, snow=TRUE) {
  retVal <- list()
  if (s3) {
    retVal$s3$access_key_id     <-  "AKIAJYXVBNHTJNSA27EQ"
    retVal$s3$secret_access_key <-  "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"
  }


  if (redshift) {
    retVal$redshift$Username    <- "BogusName"
    retVal$redshift$Password    <- "SesameStayClosed"

    retVal$redshift$Hostname    <- "ihs-lake-doppler.cop6dfpxh7ta.us-west-2.redshift.amazonaws.com"
    retVal$redshift$Database    <- "lake_one"
    retVal$redshift$Port        <- 5439
    retVal$redshift$DriverClass <- "com.amazon.redshift.jdbc41.Driver"
    retVal$redshift$UrlToRedshift <-
      paste0("jdbc:redshift://", retVal$redshift$Hostname, ":", retVal$redshift$Port, "/", retVal$redshift$Database)
  }

  if (snow) {
    retVal$configuration$cluster$id_ip <- data.frame(id=c("i-ID_One", "i-ID_Deux", "i-ID_Drei"), ip=c("10.11.12.88", "10.11.12.89", "10.11.12.90"))
  }

  retVal
}

# Return character vectors corresponding the the text (JSON and otherwise) that corresponds to the config argument.
# These texts are returned in a list, under 3 elements: mainConfigTxt, rsUserPwdTxt and snowTxt.
# @param config list : a configuration object with same structure as these returned by ConfigureAWS() (or getTestConfig())
#    if NULL, it a default test config is supplied.
# @param rsUserPwdFileName character string : file path+name of the Redshift User Password file
# @param snowFileName  character string : file path+name of the SNOW config file.
getTestConfigTexts <- function(config=NULL, rsUserPwdFileName=NULL, snowFileName=NULL) {
  if (is.null(config))
    config <- getTestConfig()
  if (is.null(rsUserPwdFileName))
    rsUserPwdFileName <- "./Config/redshift_username_password.json"
  if (is.null(snowFileName))
    snowFileName <- "./Config/shared_snow_cluster3.dat"

  mainConfig <- '{"configuration": {'
  if (!is.null(config$redshift)) {
    mainConfig <- c(mainConfig,
      '    "redshift": {',
      sprintf('      "DriverClass": "%s",', config$redshift$DriverClass),
      '      "DriverPath": "Obsolete",',
      sprintf('      "Hostname": "%s",', config$redshift$Hostname),
      sprintf('      "Database": "lake_one",', config$redshift$Database),
      sprintf('      "Port": %d,', config$redshift$Port),
      sprintf('      "UserPasswordFile" : "%s"', rsUserPwdFileName),
      '    }'
    )
  }
  if (!is.null(config$s3)) {
    if (length(mainConfig) > 1)
      mainConfig <- c(mainConfig, ",")
    mainConfig <- c(mainConfig,
      '    "S3": {',
      sprintf('      "access_key_id": "%s",',    config$s3$access_key_id),
      sprintf('      "secret_access_key": "%s"', config$s3$secret_access_key),
      '    }'
    )
  }
  if (!is.null(config$configuration$cluster)) {
    if (length(mainConfig) > 1)
      mainConfig <- c(mainConfig, ",")
    mainConfig <- c(mainConfig,
      '    "cluster":{',
      sprintf('      "configFilename": "%s"', snowFileName),
      '    }'
    )
  }
  mainConfig <- c(mainConfig,
      '}}'
  )

  if (is.null(config$redshift$Username)) {
    rsUserPwdTxt <- NULL
  } else {
    rsUserPwdTxt <- c(
      '{',
      sprintf('  "Username": "%s",', config$redshift$Username),
      sprintf('  "Password": "%s"',  config$redshift$Password),
      '}'
    )
  }

  if (is.null(config$configuration$cluster$id_ip)) {
    snowTxt <- NULL
  } else {
    dfIdIp <- config$configuration$cluster$id_ip
    snowTxt <- c("id, ip",
                 paste0(dfIdIp$id, ", ", dfIdIp$ip)
    )
  }

  list(mainConfigTxt=mainConfig, rsUserPwdTxt=rsUserPwdTxt, snowTxt=snowTxt)
}


# Functions and global vars used to keep track of original state of config files and to
# re-instate it after the tests
configTest_OrigFiles <- character()

saveOriginalFile <- function(fileToSave) {
  normPath <- normalizePath(fileToSave, mustWork=FALSE)
  backupName <- paste0(normPath, ".bak.tst")
  if (normPath %in% configTest_OrigFiles) {
    if (!file.exists(backupName))
      stop(sprintf("Very odd: file %s said to be readily saved, yet no .bak.txt file exists ???", normPath))
    return(FALSE)  # nothing to do: file already backed-up
  }

  if (!file.exists(normPath))
    return(FALSE)  # nothing to backup; file doesn't exists!
  if (file.exists(backupName))
    stop(sprintf("File %s cannot be backed-up since its .bak.txt file readily exists", normPath))
  file.rename(normPath, backupName)
  configTest_OrigFiles <<- c(configTest_OrigFiles, normPath)
  return(TRUE)
}

reestablishOriginalFiles <- function(filesToReInstate=NULL) {
  if (is.null(filesToReInstate))
    filesToReInstate <- configTest_OrigFiles
  else
    filesToReInstate <- normalizePath(filesToReInstate, mustWork=FALSE)

  filesNotSaved <- setdiff(filesToReInstate, configTest_OrigFiles)
  if (length(filesNotSaved) > 0)
    warning(sprintf("Files %s were not backed-up; cannot re-instate them!", paste(basename(filesNotSaved), collapse=", ")))

  filesToReInstate <- intersect(configTest_OrigFiles, filesToReInstate)

  for (origFile in filesToReInstate) {
    if (file.exists(origFile))    # delete the _test_ file (with the original name) if it exists.
      file.remove(origFile)
    file.rename(paste0(origFile, ".bak.tst"), origFile)
  }
  configTest_OrigFiles <<- setdiff(configTest_OrigFiles, filesToReInstate)
}

# Test of the above:
#  saveOriginalFile("../AACloudTools/Config/config.json")
#  saveOriginalFile("./Config/redshift_username_password_template.json")
#  saveOriginalFile("Config/config.json")
#  reestablishOriginalFiles(c("./Config/redshift_username_password_template.json", "bozo.txt"))
#  reestablishOriginalFiles()
#  configTest_OrigFiles


# Setup a "working" set of config file(s) in 'configFolder' folder
# This function also handles the the backing up of possible files of same name at said location
# To ease with recognizing the config (for e.g. when testing behavior of ConfigureAWS() when multiple
# config locations are possibe) the Redshift User and Password are set to the configTag and to the configFolder
# respectively.
setupWorkingConfig <- function(configFolder=".",
                                    configTag=configFolder,
                                    mainConfFileName="config.json",
                                    rsUserPwdFileName="redshift_username_password.json",
                                    snowFileName="snow_cluster.dat") {
  if (is.null(configFolder))
    configFolder <- "."
  gsub("/$", "", configFolder)
  configFiles <- c(file.path(configFolder, mainConfFileName),
                   file.path(configFolder, rsUserPwdFileName),
                   file.path(configFolder, snowFileName))

  dbg <- sapply(configFiles, saveOriginalFile)

  theConf <- getTestConfig()
  theConf$redshift$Username <- configTag     # <<< That's how we'll recognize which config was used.
  theConf$redshift$Password <- configFolder  # <<< as well.
  confTexts <- getTestConfigTexts(theConf, rsUserPwdFileName=configFiles[2], snowFileName=configFiles[3])

  if (!is.null(mainConfFileName))
    writeLines(confTexts$mainConfigTxt, con=configFiles[1])
  if (!is.null(rsUserPwdFileName))
    writeLines(confTexts$rsUserPwdTxt,  con=configFiles[2])
  if (!is.null(snowFileName))
    writeLines(confTexts$snowTxt,       con=configFiles[3])
}

# ******************** ends "setup" functions *********************************


test_that("SetRenviron() and ReadRenviron work", {

  # Create a small test file with mix of comments and key=value entries
  testFile <- "./test_REnviron_fcts_321.txt"
  startText <- c("## Some Commment",
                 "   ## another astute remark ",
                 "  tstBravo = Berries (mixed)  # cum commentum",
                 "",
                 " #Comment=blahblah",
                 " tstDelta= 3 Dates",
                 "tstLima = 4422.11")
  writeLines(startText,
             con=testFile)

  envVarBack <- ReadRenviron(testFile)
  expect_equal(names(envVarBack), c("tstBravo", "tstDelta", "tstLima"))
  names(envVarBack) <- NULL
  expect_equal(envVarBack, c("Berries (mixed)", "3 Dates", "4422.11"))

  kvps <- c(tstAlpha="Apple", tstBravo="Banana", testZulu="1|2|4|8|42")
  cc <- SetRenviron(kvps, testFile)
  expect_equal(cc, length(kvps))
  envVarBack <- ReadRenviron(testFile)

  expect_equal(names(envVarBack), c("tstBravo", "tstDelta", "tstLima", "tstAlpha", "testZulu" ))
  names(envVarBack) <- NULL
  # BTW note how the tstBravo changes from Berries... to Banana
  expect_equal(envVarBack, c("Banana", "3 Dates", "4422.11", "Apple", "1|2|4|8|42" ))

  # Confirm we keep the comments lines and the lines of the unchanged Key-Value pairs as-is
  linesBack <- readLines(testFile)
  expect_true(all(startText[1:2] == linesBack[1:2], startText[4:7] == linesBack[4:7]))

  # clean-up
  file.remove(testFile)
})

test_that("PathSimplify() works", {
  unSimplifyiables <- c("", "C:/", "/", "..", "/ALpha/Bravo/Charlie/Zulu")
  for (p in unSimplifyiables) {
    expect_equal(p, PathSimplify(p))
  }

  expect_equal("/Alpha/Zulu", PathSimplify("/Alpha/Bravo/../Zulu/Xtra/.."))
  expect_equal("/Alpha/Zulu", PathSimplify("/Alpha/Bravo/Charlie/../../Xtra/../Zulu"))
  expect_equal("E:/Alpha/Tango/Kilo", PathSimplify("E:/Alpha/Bravo/Charlie/../../Tango/Kilo/Zulu/.."))

  # and with relative paths not the explicit ./ added.
  expect_equal("./Alpha/Zulu", PathSimplify("Alpha/Bravo/Charlie/../../Zulu"))
  expect_equal("./Alpha/Zulu", PathSimplify("./Alpha/Bravo/Charlie/../../Zulu"))
  expect_equal("./Alpha/Kilo/Lima", PathSimplify("Oscar/../Alpha/Bravo/../Kilo/Lima/"))
})

test_that("PathIsRelative() works", {
  relativeOnes <- c("", "../foo/bar.txt", "../../", "foo", "bar.baz.bar", "foo/")
  for(p in relativeOnes) {
    expect_true(PathIsRelative(p), info=p)
  }
  absoluteOnes <- c("/", "C:/foo/bar/baz.loo", "E:/foo.bar", "/foo bar/", "D:",
                    "\\\\my.server\\boo\\far", "http://foo.bar.net/baz/bing")
  for(p in absoluteOnes) {
    expect_false(PathIsRelative(p), info=p)
  }

  expect_true(is.na(PathIsRelative(NULL)))
  expect_true(is.na(PathIsRelative(NA)))
})

test_that("awsConf_AddMsg() works", {
  ExpectedValueForInfo <- c("A single line ",
                            "A sequence of 3 lines...", "  ... nbr 2", "  ... nbr 3",
                            sprintf("%s last string produced by %s: %d is the answer!", "One", "sprintf", 42)
  )
  vars <- awsConf_InitVars()
  awsConf_AddMsg(vars, ExpectedValueForInfo[1])
  awsConf_AddMsg(vars, ExpectedValueForInfo[2:4])
  awsConf_AddMsg(vars, ExpectedValueForInfo[5])

  expect_equal(vars$info, ExpectedValueForInfo)
})

test_that("awsConf_GetConfigFilePath() works", {
  # We'll be working with the .Renviron file either in the user's home directory or in current directory
  # hence we disable the .Renviron file possibly pointed to by the R_ENVIRON_USER system-level environmental var
  # because this location is the first that is used when the environmental variable is set.
  saved_R_ENVIRON_USER <- Sys.getenv("R_ENVIRON_USER")
  if (saved_R_ENVIRON_USER != "")
    Sys.unsetenv("R_ENVIRON_USER")

  savedRenvironVar <- ReadRenviron(fileName=NULL)["AACLOUDTOOLS_CONFIG_FILE"]

  # We set ourselves in a folder where we know there is no .Renvion file (yet)
  dir.create("test_zone")
  setwd("./test_zone")

  # by using fileName=NULL, the .Renviron file will be that in the user's home directory,
  # since we've disabled the first place where to look for this file (R_ENVIRON_USER system envir. variable)
  # and the second place (the current directory).
  # if there is no .Renviron file in the home directory, an "empty" one will be automatically created, to which
  # the SetRenvion value pair will be added.
  userConfigLoc <- "/somewhere/on/this/host/config.json"
  SetRenviron(c(AACLOUDTOOLS_CONFIG_FILE=userConfigLoc), fileName=NULL)

  configPath <- awsConf_GetConfigFilePath(locCode="user")  # note the lowercase, as we expect the API to be case-insensitive
  expect_equal(as.character(configPath), userConfigLoc)

  #creating a .Renviron file in current directory;  this file will shadow the file in home dir.
  SetRenviron(c(FluxCapacitor="YES", TargetDate="1989-11-09"), ".Renviron")
  configPath <- awsConf_GetConfigFilePath(locCode="USER")
  expect_equal(configPath, "")  # "" means "not found", no user-level "global" AWS config file is found.
  userConfigLoc <- "./myConfig.json" # although "legal", this relative path is typically a bad idea; here, we
                                     # just use this value to "prove" that we get the info from the right file.
  SetRenviron(c(AACLOUDTOOLS_CONFIG_FILE=userConfigLoc), ".Renviron")
  configPath <- awsConf_GetConfigFilePath(locCode="USER")
  expect_equal(as.character(configPath), userConfigLoc)

  setwd("..")
  unlink("test_zone", recursive=TRUE)

  #re-establish the original state  (attention: order matters...)
  if (!is.na(savedRenvironVar))
    SetRenviron(c(AACLOUDTOOLS_CONFIG_FILE=savedRenvironVar), fileName=NULL)
  if (saved_R_ENVIRON_USER != "")
    Sys.setenv(R_ENVIRON_USER, saved_R_ENVIRON_USER)
})

test_that("awsConf_FindConfigFilePath() works", {
  savedWd <- getwd()
  dir.create("./Alpha/Bravo/Charlie/Delta", recursive=TRUE)
  dir.create("./Alpha/Config")
  dir.create("./Alpha/Bravo/Charlie/Config")

  setupWorkingConfig("./Alpha/Config", "Alpha_Conf", "Config321.json")
  firstLocationAbsPath  <- normalizePath("./Alpha/Config", winslash="/")
  secondLocationAbsPath <- normalizePath("./Alpha/Bravo/Charlie", winslash="/")
  thirdLocationAbsPath  <- normalizePath("./Alpha/Bravo", winslash="/")
  setwd("./Alpha/Bravo/Charlie/Delta")

  foundPath <- awsConf_FindConfigFilePath("./Config/Config321.json")
  expect_equal(foundPath, file.path(firstLocationAbsPath, "Config321.json"))

  # now we place one closer to the current working directory an expect this
  # second config to be the one that is selected.
  setupWorkingConfig("../", "Charlie_Conf", "Config321.json")
  foundPath <- awsConf_FindConfigFilePath("./Config/Config321.json")
  expect_equal(foundPath, file.path(secondLocationAbsPath, "Config321.json"))

  # one more config, this time even closer yet, but under bad name
  # we still expect to find the 2nd location
  setupWorkingConfig(".", "Delta_Conf", "config.json")
  foundPath <- awsConf_FindConfigFilePath("./Config/Config321.json")
  expect_equal(foundPath, file.path(secondLocationAbsPath, "Config321.json"))

  # one last one, in Bravo, under the right name
  setupWorkingConfig("../..", "Bravo_Conf", "Config321.json")
  # and removing the Charlie one which would otherwise, prevail
  file.rename(file.path(secondLocationAbsPath, "Config321.json"), file.path(secondLocationAbsPath, "Test99.txt"))
  foundPath <- awsConf_FindConfigFilePath("./Config/Config321.json")
  expect_equal(foundPath, file.path(thirdLocationAbsPath, "Config321.json"))

  foundPath <- awsConf_FindConfigFilePath("./Config/Test99.txt")
  expect_equal(foundPath, file.path(secondLocationAbsPath, "Test99.txt"))

  foundPath <- awsConf_FindConfigFilePath("./Config/Does_Not_Exists.txt")
  expect_null(foundPath)

  # Reestablish original state
  setwd(savedWd)
  unlink("./Alpha", recursive=TRUE)
})

test_that("ConfigureAWS() works", {
  dir.create("test_zone")
  setwd("test_zone")

  setupWorkingConfig(".", "Test_Conf", "config.json")
  awsConf <- ConfigureAWS(configJSON="config.json", fallBackConfigs=NULL, verbose=FALSE, findConfigFolder=FALSE)

  expect_false(is.null(awsConf$s3$access_key_id) || is.null(awsConf$s3$secret_access_key)
            || is.null(awsConf$redshift$DriverClass) || is.null(awsConf$redshift$UrlToRedshift)
            || awsConf$redshift$Username != "Test_Conf"
            || is.null(awsConf$configuration$cluster$id_ip),
            label="Some elements in config are unexpectedly NULL or not as expected.")

  # dbl check it works with legacy config files.
  #awsConf <- ConfigureAWS(configJSON="Config/config.json", fallBackConfigs=NULL, verbose=FALSE, findConfigFolder=TRUE)

  # TODO: test when expecting errors... though these are better seen in the notebook.

  setwd("..")
  cc <- file.remove(list.files("test_zone", full.names=TRUE))
  unlink("test_zone", recursive=TRUE)
})


#readLines(GetUserRenvironPath(FALSE))
#envirLines <- c("# boo","", "Alpha = zzz", " Bravo  =  Zulu!", "", "Delta=123")
