###############################################################################
## PackageTools.R
##
##   Utility to rebuild the package and upload it to S3 repository
##
##   This source file is part of Advanced Analytics AWS Cloud Tools
##   It can also be used as template (with very few, if any, changes) for the
##   build of other packages.
##
##     (c) IHS 2016-2017
##
##   Author:  Christopher Lewis and Marc Veillet
##
###############################################################################

# HOW TO...  : Instructions on how to use this script are found at the end of this file.

# ***** Snippets to alter the paramaters used ******

#  incrementDevVersion_ <- TRUE    # Request to get the Version's 4th numeric part incremented

#                              Upload completed package file to ...
# copyTo_ <-  NULL            # ...  the subfolder where the STABLE versions are collected
# copyTo_ <-  "~Ver"          # ...  a subfolder named after the current version number
# copyTo_ <-  "UsedInDemos"   # ...  the subfolder named "UsedInDemo"  (allegedly where you collect the binaries which which are use in some demo)
# copyTo_ <-  "SubFolder_of_your_choice"     # <<<<  cut and paste this, edit in console or elsewhere and run to have a subfolder of your choice.

# unitTestFilePattern_ <- ""  # use this to prevent all tests
# unitTestFilePattern_ <- "(RedshiftTools|GetSeXxxUrl)"  # example only run a few few tests


# ****** PackageTools.R logic starts in earnest here ******

# Tabula _almost_ rasa!  Ensure repeatability by starting the build process with a mostly empty working environment
# The only variables which are not deleted are the ones used for setting the parameters of the call to
# the CreateUploadPackageToS3() function.
# The intent is to allow developpers to set these variables (typically but not necessarily) by running some
# of the commented-out snippets found in the script, near the function call, and then to source() the
# script in its entirety, without having to modify and save it.
rm(list=(setdiff(ls(all.names=TRUE), c("copyTo_", "incrementDevVersion_", "unitTestFilePattern_"))))
gc()
cat("\014") # Clear output window

source("Tools/PackageTools_Commons.R")
source("Tools/PackageTools_BuildSupport.R")

# Ensure the following R libraries  are installed and loaded, installing them if necessary,
# because they are needed by the package installation process itself and by AACloudTool  (and beyond as well)
InstallDependentPackages(c("foreach", "doSNOW", "jsonlite", "RJDBC", "data.table", "stringr", "knitr", "rmarkdown", "devtools"),
                         fatalIfMissingPackages=TRUE)


# ****** If we are saving under a "Release Version Number" (3 parts), we run the Unit Tests.
# Note that user can limit the extend of these tests with the unitTestFilePattern_  variable.
if (is.na(GetPackageVersion()[, 4])) {  # no 4th part : this is a "Release Version Number"
  WaitForUserInput <- function() {
    userInput <- readline(prompt="Review the Unit Test resuls and press 'Y' [enter] to proceed, any other input to abort.")
    if (userInput != "Y" && userInput != 'y')
      stop("User requested abort of the build process.")
    cat("Unit Test resuls were accepted; proceeding with the Package build process.")
  }

  if (!exists("unitTestFilePattern_"))
    unitTestFilePattern_ <- NULL                # = Run all the tests

  if (is.null(unitTestFilePattern_) || unitTestFilePattern_ != "") {
    devtools::test(filter=unitTestFilePattern_)
    WaitForUserInput()
  }
  rm(WaitForUserInput)
}

# ****** Package Build process per-se ******
#  This boils down to simply calling CreateUploadPackageToS3() with the set of parameters that is appropriate
#  for the situation.
#

# Use default parameter values when the corresponding variable is not set
if (!exists("copyTo_"))
  copyTo_ <- "~Dev"                 # Destination subfolder is "DevVersion"
if (!exists("incrementDevVersion_"))
  incrementDevVersion_ <- FALSE     # Version number's dev_number is not bumped up

CreateUploadPackageToS3(packageName=basename(getwd()), incrementDevVersion=incrementDevVersion_, copyTo=copyTo_)

message(sprintf("Created and saved package file; Version Number = %s", GetPackageVersion()))

# ****** Clean-up ******
Cleanup_PackageToolsCommons()
Cleanup_PackageToolsBuildSupport()

# in particular we get rid of the variables used to set the parameters of the CreateUploadPackageToS3() call
# so that next time around the default values for these wil be used (unless these are explicitly set anew)
varsToDelete <- ls(pattern="^(copyTo_|incrementDevVersion_|unitTestFilePattern_)$")
if (length(varsToDelete) > 0)
  rm(list=varsToDelete)
rm(varsToDelete)



# How to use this script:    (also seeo the 'Managing Package Versions' vignette for a primer on Versionning etc.)
# ----------------------
#
#  Initial setup: you may need to edit the list of the required library as it pertains to your package project.
#  (see call to InstallDependentPackages() a bit further down in this script.)
#
#  Other than this initial setup,  the script is designed so thatit doesn't need to be modified and saved.
#  By default it has very safe behavior of copying the new package file it creates to the "development" folder.
#  Hence, in most cases ...
#   1. Simply source() this script, as-is and its entirety,  and be done!
#
#  Alternatively, if you need to change the Version Number or to copy the package to a folder other than
#  the "development" folder.
#
#  1. Run selected snippets from the commented-out section below
#     Simply: Hightlight snippet and press "Ctrl + enter"
#     (or duplicate some of these snippets, uncommented, in another script if you use them frequently or
#      if you need to type in a specific folder name for the copyTo_ variable)
#     Note that the modified variables only apply to the current running of the script; by design, these
#     variables are deleted at the end of the build process, so the script reverts to the default behavior
#     in subsequent runs (unless you re-apply, explicitly, the parameter changes)
#  2. (if applicable)
#     Edit and save the DESCRIPTION file, to change the Version Number as desired.
#  3. source() this file (again, as is and its entirety; the file itself should "never" be changed)
#  4. (typically) Edit and save the Version Number in the DESCRIPTION file again so that going
#     forward, the series of code changes is tied to a new version number.  At a minimum, after saving the
#     package to the "Stable versions" folder (or more generally after saving under the package
#     under a "Release" Version Number) you'll need to add a dev_number (a 4th numeric part)
#      to the version number.
#  5. If you edited the DESCRIPTION file at all, commit and push it to Git Repository so that
#     other developpers are current with the version number in use.
#
# Unit Tests:
#    When saving the package under a "Release Version Number" (a 3-part number rather than the
#    4-parts "Development Version Number"), the script will automatically launch the tests.
#    Afer the tests complete, the script pauses and prompts the user to enter "Y" to proceed.
#    This gives you an opportunity to assert the success of the tests and allow the build
#    process to continue.  Alternatively, you can enter "N" (or other) to cause the script
#    to abort.
#    The unitTestFilePattern_ variable can be used to limit the extent of the unit tests.  Use this
#    feature -wisely- to prevent any testing at all or to run a subset of the test files.
#
