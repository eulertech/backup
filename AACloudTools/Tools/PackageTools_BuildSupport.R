###############################################################################
## PackageTools_BuildSupport.R
##
##   Functions used by the package builder logic.
##
##   While the building logic per-se is customized for each library, this
##   set of functions is shared by multiple library projects (AACloudTools,
##   AASpectre ...) and maintained centrally in AACloudTools.
##
##   In addition to the AACloudTools "pilot version", this file is duplicated
##   in the following other projects:  (? non exhaustive list)
##
##      - AASpectre    (Francisco Marco Serrano)
##
##   These projects should _not_ modify the file but rather send "push requests"
##   of sorts so that the latest and unique version of it remains in
##   AACloudTools.
##   New projects which use this file are requested to "register" with the
##   the AACloudTools custodians (Marc Veillet / Christopher Lewis) so that
##   they can receive updates.
##
##     (c) IHS 2016-2017
##
##   Author:  Christopher Lewis
###############################################################################


# Remove the functions/objects created by source()-ing this very script
#
# It is used to keep the environment tidy once we are done using these functions.
#
Cleanup_PackageToolsBuildSupport <- function() {
  rm(GetPackageVersion,
     CreateUploadPackageToS3,

     Cleanup_PackageToolsBuildSupport,  # even remove this very function!
     envir=parent.frame()
  )
}


# Find the version of the package and optionally increment its Development Version
#
# @param incrementDevVersion logical : When TRUE the Development Version part of the version
#    (i.e. the 4th numeric part of the version string) is incremented.  Results in a fatal error if
#    TRUE and if the version does not have a Development version.
# @return a object representing a "Package Version" (see package_version).
#
GetPackageVersion <- function(incrementDevVersion=FALSE) {
  versionLineRegex <- "^Version:(.+)"
  description <- readLines("DESCRIPTION")
  vLine <- stringr::str_subset(description, versionLineRegex)
  if (length(vLine) != 1)
    stop("Found more than one 'Version:' lines in DESCRIPTION file !")

  vString <- stringr::str_trim(stringr::str_match(vLine, "^Version:(.+)")[2])
  retVal <- package_version(vString)

  if (incrementDevVersion) {
    devVersion <- retVal[, 4]
    if (is.na(devVersion)) {
      stop(paste("The 'incrementDevVersion' option of CreateUploadPackageToS3()/GetPackageVersion() is",
                 "only applicable if the version in DESCRIPTION file has a Development Version part!"))
    }
    retVal[, 4] <- as.integer(devVersion) + 1
    vLineNr <- stringr::str_which(description, versionLineRegex)
    description[vLineNr] <- sprintf("Version: %s", retVal)

    writeLines(description, "DESCRIPTION")
  }

  retVal
}


# Create the package and loads it to S3 storage so that it is available for users to download and install.
#
# @param packageName character string : the name of the package (typically the basename of the the root folder of
#    the package project)
# @param incrementDevVersion  logical : when TRUE, the Development Version part of the version number (the 4th
#    numerical part) is incremented. Note: Results in a fatal error if TRUE and the version doesn't have a
#    Development Version
# @copyTo character string : the subfolder in S3 where the package is to be copied.
#    This can be one of three pre-determined places:
#        NULL / empty string = the "Current" version folder = the default place where users get the most recent BUT STABLE version
#        "~Dev"               = the development folder      = the default place where the developpers store the LASTEST but probably unstable version.
#        "~Ver"               = the version folder, i.e. a subfolder named after the version _excluding_ the Dev Version part.
#    or the name of the desired subfolder, taken at face value (e.g. "SpecialVersionForDemoX")
#
CreateUploadPackageToS3 <- function(packageName, incrementDevVersion=FALSE, copyTo="~Dev") {
  pkgVer   <- GetPackageVersion(incrementDevVersion=incrementDevVersion)
  s3Folder <- GetPackageS3Folder(copyTo, packageName, pkgVer)

  devtools::document()  # produce the documentation's .Rd files from roxygen comments amid the R source files
  # unsure if this is necessary with manual=TRUE parameter below...

  commandLine <- paste0("R CMD INSTALL --preclean --no-multiarch --with-keep.source ", getwd())
  system(commandLine)

  # Note: The --resvave-data=gzip is redundant with default behavior of R CMD build (which is the command invoked when binary=FALSW),
  # but this a) makes it explicit and b) provides a placeholder/reminder that the args= argument is where we can add options
  pkgFile <- devtools::build(binary=FALSE, manual=TRUE, vignettes=TRUE, args=c("--resave-data=gzip"))

  pkgFullname <- paste0('package:', packageName)
  isAttached <- pkgFullname %in% search()
  if (isAttached)
    detach(pkgFullname, unload=TRUE, character.only=TRUE)

  install.packages(pkgFile, repos=NULL, INSTALL_opts='--no-multiarch')
  library(packageName, character.only=TRUE)

  # ***** Copy the package file to S3 storage *****
  configAWS <- ConfigureAWS('./Config/config.json')
  UploadFileToS3(pkgFile, paste0(s3Folder, basename(pkgFile)))
}
