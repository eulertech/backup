###############################################################################
## PackageTools_Commons.R
##
##   This file contains the functions used for downloading and installing
##   R libraries stored in IHSMarkit's package repository in S3.
##
##   Some of these functions are also used at build-time, i.e. for the creation
##   of these packages (however the functions necessary for the build but not
##   for the installation are kept in PackageTools_BuildSupport.R)
##
##   This source file is shared by multiple projects and applications, but is
##   maintained, centrally, in AACloudTools.  It is suggested that eventually
##   these functions be included in the AACloudTools package per se (at this
##   time it is only there in the source code, in the 'Tools' folder, not as
##   exported functions of the libray).  Availing these function in the
##   AACloudTools library (or another library) will avoid the need for
##   duplicating  the source file in all these projects (only leaving open
#    the need for a subset of these functions to be part of some "boostrap"
##   function set used to install the AACloudTools library itself.)
##
##   Until then, all these applications should refrain from modifying their
##   copy but instead send "push request" to the custodian of AACloudTools.
##
##
##   This source file is part of Advanced Analytics AWS Cloud Tools
##     (c) IHS 2016-2017
##
##   Author:  Christopher Lewis and Marc Veillet
###############################################################################


# Remove the functions/objects created by source()-ing this very script
#
# It is used to keep the environment tidy once we are done using these functions.
#
Cleanup_PackageToolsCommons <- function() {
  rm(InstallDependentPackages,
     GetPackageS3Folder,
     GetLatestVersionFromS3Location,
     InstallPackage,
     ParsePackageList,
     InstallPackagesOnCurrentHost,
     InstallPackagesEverywhere,

     Cleanup_PackageToolsCommons,  # even remove this very function!
     envir = parent.frame()
  )
}


# Install Dependent Packages
#
# @param reqPackages character vector : the list of packages required by the current package/application.
#   NOTE: these are the "CRAN" packages only, i.e. excludes the IHSMarkit packages such as AACloudTools, AASpectre etc.
#         as these come from S3 with special installation logic rather than from the standard CRAN-based repository process.
# @param repos character string : the R package repository to use.  Defaults to the "repos" Option.  Example value =
#    "http://cran.us.r-project.org"
# @param fatalIfMissingPackages logical : whether the program should stop if some libraries cannot be installed.
# @param verbose logical : whether install.packages() should be called with verbose option
# @return TRUE is succesful, FALSE otherwise.
InstallDependentPackages <- function(reqPackages, repos = getOption("repos"), fatalIfMissingPackages = TRUE, verbose = FALSE) {
  retVal <- TRUE

  lapply(reqPackages, function(x) {
                       if (!requireNamespace(x))
                         install.packages(x, repos = repos, dependencies = TRUE, verbose = verbose)
  })
  libIsOk <- sapply(reqPackages, function(x) requireNamespace(x))
  if (!all(libIsOk)) {
    missingLibs <- paste(reqPackages[!libIsOk], collapse = ", ")
    if (fatalIfMissingPackages)
      stop(sprintf("CANNOT CONTINUE: The following libraries are missing : %s", missingLibs))
    print(sprintf("WARNING: the following libraries are missing : %s", missingLibs))
    retVal <- FALSE
  }
  retVal
}


# Get the URL of the S3 folder where the package is to be stored or retrieved, given the desired
#   "subLocation" location
#
#
# @param subLocation character string : the "subfolder" where the file should be stored; this may either be a
#   subfolder name to be used as-is or one of the following 3 codes:
#      - NULL (or empty string) : the official location for the current STABLE version
#      - "~Dev"                 : the location for the latest (and plausibly unstable) development version
#      - "~Ver"                 : a folder named after the version of the package
# @param packageName character string : the name of the package (typically the basename of the root folder of the package project)
# @param version a class representing the version of the package (see GetPackageVersion() or base::package_version)
#    This argument is used in the "upload" direction, for defining the ~Ver subFolder, and for asserting that this is not a Dev Version, for
#    the "~Ver" and "stable" cases.   It is not used for the "download" direction, as one doesn't necessarily know the version.  In
#    that case, the default value of "0.0.0" should be be used (also note that, in that case, using "~Ver" doesn't make sense.)
#
GetPackageS3Folder <- function(subLocation, packageName, version = package_version("0.0.0")) {
  if (is.null(subLocation))
    subLocation <- ""
  subLocation <- stringr::str_replace_all(subLocation, "(^/|/$)", "")  # strip leading and/or trailing slashes from the subLocation as we explicitly supply these here.

  # The subfolder is given directly by "subFolder" argument...
  subFolder <- paste0("/", subLocation)
  # ... unless it is one of the following 3 cases
  if (subLocation == "") {
    if (!is.na(version[, 4])) {
      stop("'Latest STABLE' shorthand for S3 destination is not applicable for versions which have a Development Version part.")
    }
    subFolder <- ""
  }
  if (subLocation == "~Dev")
    subFolder <- "/DevVersion"
  if (subLocation == "~Ver") {
    if (!is.na(version[, 4])) {
      stop("'~Ver' shorthand for S3 destination is not applicable for versions which have a Development Version part.")
    }
    subFolder <- paste0("/", version)
  }

  # TODO: eventually move to a less "temp" location, after all, we can only easily re-create the current Development version
  #       hence we should save the versions we may care for in a more permanent location!
  paste0("s3://ihs-temp/packages/", packageName, subFolder, "/")
}

#   Tests / examples
# GetPackageS3Folder("~Dev", "BozoPack", package_version("1.2.3"))
# GetPackageS3Folder("~Ver", "BozoPack", package_version("1.2.3"))
# GetPackageS3Folder("",     "BozoPack", package_version("1.2.3"))
# GetPackageS3Folder(NULL,   "BozoPack", package_version("1.2.3"))
# GetPackageS3Folder("VersionForDemo/Aug19",  "BozoPack", package_version("1.2.3"))
# GetPackageS3Folder("/VersionForDemo/Aug19", "BozoPack", package_version("1.2.3"))
# GetPackageS3Folder("VersionForDemo/Aug19/", "BozoPack", package_version("1.2.3"))

# Finds the latest version of a library _in a given Location_
#
# This function is useful when deciding which specific file to use; as such it is not used
# in this script, but it is kept in this source file for sake of keeping all these related
# functions together.  (It is used, i.e. duplicated, in the various ReInstallPackages.R scripts)
#
# @param subLocation character string : the subfolder where to look; this may be a subfolder
#    name per-se or one of the following two codes:
#       -  NULL (or empty string) = Official location of the latest STABLE version
#       -  "~DEV"                 = Location of the latest (and plausibly unstable) Development version
# @param packageName character string : the name of the package
# @param fatalIfNoPackageFound logical : How the function behaves when the underlying folder doesn't
#   have at least one qualifying package file;  when TRUE, the program is aborted (stop()-ed); when
#   FALSE, a NULL is returned.
# @return a list object with two elements (or NULL as explained above)
#    $Version  the version number (of the latest)
#    $S3Url    the full URL to the corresponding package file in S3
GetLatestVersionFromS3Location <- function(subLocation, packageName, fatalIfNoPackageFound = TRUE) {
  if (is.null(subLocation))
    subLocation <- ""
  subLocation <- stringr::str_replace_all(subLocation, "(^/|/$)", "")  # strip leading and/or trailing slashes from the subLocation as we explicitly supply these here.

  # The subfolder is given directly by "subFolder" argument...
  subFolder <- paste0("/", subLocation)
  # ... unless it is one of the following 2 cases
  if (subLocation == "")
    subFolder <- ""                # Latest STABLE version is in the "root"
  if (subLocation == "~Dev")
    subFolder <- "/DevVersion"     # and that this where the bleeding edge is

  s3Folder <- paste0("s3://ihs-temp/packages/", packageName, subFolder, "/")
  awsCommand <- paste("aws s3 ls", s3Folder)
  rawDirContent <- system(awsCommand, intern=TRUE)
  if (length(rawDirContent) == 0) {
    if (fatalIfNoPackageFound)
      stop(sprintf("The S3 location, %s , is inexistent or empty.", s3Folder))
    return(NULL)
  }

  verStrings <- stringr::str_match(rawDirContent, paste0(packageName, "_(([0-9]+\\.){3}([0-9]+\\.)?)tar\\.gz"))[, 2]
  verStrings <- verStrings[!is.na(verStrings)]
  if (length(verStrings) == 0) {
    if (fatalIfNoPackageFound)
      stop(sprintf("The se location, %s , does not have any qualifying package files", s3Folder))
    return(NULL)
  }
  verNums <-  package_version(stringr::str_replace(verStrings, "\\.$", ""))    # trim the trailing dot + convert to true version numbers
  latestVer <- max(verNums)
  urlToLatestPackage <- paste0(GetPackageS3Folder(subLocation, packageName, latestVer),
                               packageName, "_", as.character(latestVer), ".tar.gz")
  list(Version=latestVer, S3Url=urlToLatestPackage)
}

# Test / examples of the above
#  wrk <- GetLatestVersionFromS3Location("~Dev", "AACloudTools")



# Install ONE R package stored on IHS Markit's S3 repository (as opposed to, say, from CRAN)
#
# The package to install is defined by its "SubLocation", its Name and by the latest (highest)
# version effectively present in corresponding folder in S3
#
# @param subLocation character string : the subfolder where to look; this may be a subfolder
#    name per-se or one of the following two codes:
#       -  NULL (or empty string) = Official location of the latest STABLE version
#       -  "~DEV"                 = Location of the latest (and plausibly unstable) Development version
# @param packageName character string : the name of the package e.g "AACloudTools" or "AASpectre"
#
InstallPackage <- function(subLocation, packageName) {
  ConfigureS3()
  s3Folder   <- GetPackageS3Folder(subLocation, packageName)
  latestVer  <- GetLatestVersionFromS3Location(subLocation, packageName, fatalIfNoPackageFound = TRUE)
  pkgFile <- paste0(packageName, ".tar.gz")  # local file name

  DownloadFileFromS3(latestVer$S3Url, pkgFile)

  pkgFullname <- paste0("package:", packageName)
  if (pkgFullname %in% search())                                            # Is package currently attached ?
    detach(pkgFullname, unload = TRUE, character.only = TRUE, force = TRUE) # Yes: detach it first

  install.packages(pkgFile, repos = NULL, INSTALL_opts = "--no-multiarch")
  library(packageName, character.only = TRUE)

  file.remove(pkgFile)
}

# Parse and Check a Package List such as the packagesToInstall argument of the
#   InstallPackagesOnCurrentHost() and InstallPackagesEverywhere() functions.
#
# Such arguments can either be a character vector or a data.frame
#
# If a character vector, it must contain the Package name optionally preceded by the "SubLocation";
# when both Package name and SubLocation are supplied these must be separated by a | character; it is
# also ok to have just the Package name with a | as its first character.
# examples :
#    # 1st and 3rd elements have no "SubLocation" (i.e. they point to the official Latest STABLE version)
#    c("|AACloudTools", "~Dev|AASpectre", "AABaysian")
#    # 2nd element is invalid (Location "Bozo" but no Package name)
#    # 3rd element is valid (package named "Bozo")
#    c("AACloudTools", "Bozo|", "Bozo")
#
# If a data.frame it must have two columns, $SubLocation and $PackageName
#
# @param packagesList character vector or data.frame (see details)
# @return a data.frame
ParsePackageList <- function(packagesList) {
  if (is.character(packagesList)) {
    parsedPkgList <- stringr::str_match(packagesList, "^(([^|]*)(\\|))?([^|]+)$")  #
    parsedPkgList <- parsedPkgList[, c(3, 5)]
    parsedPkgList[is.na(parsedPkgList)] <- ""
    retVal <- data.frame(matrix(parsedPkgList, ncol = 2), stringsAsFactors = FALSE)
    colnames(retVal) <- c("SubLocation", "PackageName")
  } else {
    if (!is.data.frame(packagesList)) {
      stop("Invalid 'packagesList' argument: expecting either a character vector or a dataframe")
    }
    retVal <- packagesList
    if (length(setdiff(colnames(retVal), c("SubLocation", "PackageName"))) != 0) {
      stop("Invalid 'packagesList' argument: its column names should be 'SubLocation' and 'PackageName'")
    }
  }

  if (nrow(retVal) == 0) {
    stop("Invalid packagesList, it is empty!'")
  }

  missingPkgNameIx <- which(retVal$PackageName == "")
  if (length(missingPkgNameIx) > 0){
    msg <- sprintf("Invalid elements in packagesList: the element(s) # (%s) have an empty/missing PackageName value",
                   paste(missingPkgNameIx, collapse = ", "))
    stop(msg)
  }
  retVal
}
# Test / example
#   df <- ParsePackageList(c("AACloudTools", "~Dev|AASpectre", "Bozo|GLM"))
#   df <- ParsePackageList(c("AACloudTools", "~Dev|AASpectre", "Bozo|"))  # expect error


# Install a list of (IHS/AA) packages on the current host
#
# This function can be used, sequentially, to install packages on the "main" (aka "edge node") host
# or on individual worker nodes of a SNOW cluster.
#
# @param ihsPackagesToInstall character vector or data.frame (see ParsePackageList's packagesList argument) : the list
#                                 of S3-stored packages to install  (IHS's R package repository)
# @param stdPackagesToInstall  character vector : the list of "standard" (CRAN-based) packages to install
# @param repos character string : the [CRAN-like] repository to use for downloading the "standard" packages.
# @param fatalIfMissingPackages logical : whether the program should stop if some libraries cannot be installed.
#
InstallPackagesOnCurrentHost <- function(ihsPackagesToInstall, stdPackagesToInstall,
                                         repos = getOption("repos"), fatalIfMissingPackages = TRUE, verbose = FALSE) {
  InstallDependentPackages(stdPackagesToInstall, repos = repos, fatalIfMissingPackages = fatalIfMissingPackages, verbose = verbose)

  ihsPackagesToInstall <- ParsePackageList(ihsPackagesToInstall)

  for (i in seq(nrow(ihsPackagesToInstall))) {
    pkgToInstall <- ihsPackagesToInstall[i, ]
    InstallPackage(pkgToInstall$SubLocation , pkgToInstall$PackageName)
  }
}

# Install [IHS / AA] packages on the current host _and_ on the SNOW cluster
# associated with the application if applicable.
#
# @param ihsPackagesToInstall  character vector or data.frame (see ParsePackageList's packagesList argument) : the list
#                                 of S3-stored packages to install  (IHS's R package repository)
# @param stdPackagesToInstall  character vector : the list of "standard" (CRAN-based) packages to install
# @param repos character string : the [CRAN-like] repository to use for downloading the "standard" packages.
# @param fatalIfMissingPackages logical : whether the program should stop if some libraries cannot be installed.
#
# @return an integer code : -1 = error,  0 = OK but installed only on main host  1 = OK, installed on the cluster as well.
InstallPackagesEverywhere <- function(ihsPackagesToInstall, stdPackagesToInstall,
                                      repos = getOption("repos"), fatalIfMissingPackages = TRUE, verbose = FALSE) {

  # Systematically install the packages on the master/edge node
  InstallPackagesOnCurrentHost(ihsPackagesToInstall, stdPackagesToInstall,
                               repos = repos, fatalIfMissingPackages = fatalIfMissingPackages, verbose = verbose)
  message(Sys.time(), " - Completed packages installation on 'main' host.")

  if (!require("AACloudTools", character.only=TRUE))   # also maybe quietly=TRUE ?
    return(0)  # normal exit:  no AACloudTools => no SNOW => done!

  # Configuration Settings
  configAWS <- ConfigureAWS("./Config/config.json")

  # Install packages on the snow cluster
  if (!AreEC2InstancesRunning(configAWS))
    return(0) # normal exit : EC2 instances of the SNOW cluster are not started => done !

  if (.Platform$OS.type != "unix") {
    message("SNOW worker nodes can only be updated by running this code on the RStudio Server instance in the IHS cloud.")
    return(0) # normal exit : Can't work with snow workers from local host => done !
  }

  # Only 1 process on each node is needed to perform the library installation
  cl <- AAStartSnow(local = FALSE, configAWS, coresPerNode = 1)
  message(Sys.time(), " - Installing packages on the SNOW worker nodes.  Please wait...")
  workerNodes <- nrow(configAWS$configuration$cluster$id_ip)
  results <- foreach(i = 1:workerNodes,
                     .export=c("InstallDependentPackages", "DownloadFileFromS3", "ConfigureS3", "InstallPackage", "InstallPackagesOnCurrentHost",
                               "ParsePackageList", "GetPackageS3Folder", "GetLatestVersionFromS3Location")) %dopar% {
                       InstallPackagesOnCurrentHost(ihsPackagesToInstall, stdPackagesToInstall,
                                                    repos = repos, fatalIfMissingPackages = fatalIfMissingPackages, verbose = verbose)
                     }
  message(Sys.time(), " - Done. Completed packages installation on SNOW worker nodes.")
  AAStopSnow(cl, configAWS)
  return(1)
}
