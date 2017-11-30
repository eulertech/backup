###############################################################################
##  AA_InstallerBootstrap.R
##
##    Functions providing _basic_ access to S3 storage, _in the absence of AACloudTools_.
##
##    Whereby all access to S3 and other Cloud-based resources is typically
##    provided by the AACloudTools package, the function in this script are
##    necessary to initially download the AACloudTools package itself !
##
##  Copyright IHSMarkit 2016-2017
##  This is IHS Markit Proprietary logic. It Cannot be dissiminated outside the enterprise.
##
##  Author : Chritopher Lewis and Marc Veillet
##
###############################################################################


# Remove the functions/objects created by source()-ing this very script
#
# It is used to keep the environment tidy once we are done using these functions.
#
CleanUp_AAInstallerBootstrap <- function() {
  rm(InstallDependentPackages,
     DownloadFileFromS3,
     ConfigureS3,

     CleanUp_AAInstallerBootstrap,   # even remove this very function!
     envir=parent.frame()
  )
}


# Provide a minumum configuration for S3 access
#
# It is necessary to call this function prior to using any function which references S3, such as.
#  DownloadFileFromS3() for example.
ConfigureS3 <- function() {
  Sys.setenv(AWS_ACCESS_KEY_ID='AKIAJYXVBNHTJNSA27EQ')
  Sys.setenv(AWS_SECRET_ACCESS_KEY='Hf/Z57ERxTMBkbB3Yl2OcYWkeYoS7q155wkfW0Y3')
  Sys.setenv(AWS_DEFAULT_REGION='us-west-2')
}

# Copy a file from S3 to a Local file
#
# @param s3FileName    character string : full S# URL, including the 's3://' prefix of the file to copy
# @param locaFileName  character string : path and optional name of the destination
# No return value, however the AWS command produces some output to the console.
DownloadFileFromS3 <- function(s3FileName, localFileName) {
  message(sprintf("%s - Downloading file %s from S3", Sys.time(), s3FileName))
  awsCommand <- paste0("aws s3 cp ", s3FileName, " ", localFileName, " --only-show-errors")
  system(awsCommand)
}
