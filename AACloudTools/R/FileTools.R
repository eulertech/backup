# FileTools.R
#
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
#
#   Author:  Christopher Lewis
#

#' Create a Random File Name.
#'
#' \code{GetRandomFileName} returns a file name based on a supplied prefix and extension and some random text.
#'
#' Although introduced initially for \emph{file names} or also folder names, this function produces
#' random strings which may well be used for other purposes.
#' @param prefix A character string with an optional prefix that is added at the beginning of the file name.
#' @param ext A character string with the file extension, or more generally whatever the end of
#'   the file name should be set to.  Use \code{""} if no extension is desired.
#' @param method A character string which determine how the random portion of the file name is produced.
#'   See the Detail section for a list of valid values and their meaning.
#' @param rndLen Integer indicating the number of characters desired in the Random portion of the file
#'   name (i.e. excluding the prefix and the ext).  This argument is not used for
#'   \code{"guid"} and \code{"guidNoDash"}.  It default to 12 and 10 respectively for \code{"hex"}.
#'   and \code{"alphaNum"} respectively.  Beware of using too small lengths as short random texts
#'   are more likely to produce duplicate file names.
#' @return A character string.
#'
#' @examples
#' GetRandomFileName("D_", ".csv")
#'   [1] "D_2e6baac0-9ff5-11e6-b4fd-635bba25a089.csv"
#'
#' GetRandomFileName("Report", ".txt", method="hex")
#'   [1] "Report4C01BD055133.txt"
#'
#' GetRandomFileName("Tmp_", "BlueList.csv", method="hex", 3)
#'   [1] "Tmp_6B1BlueList.csv"
#'
#' GetRandomFileName("", ".RData", method="alphaNum", 6)
#    [1] "VLYLUG.RData"
#' @family File helper functions
#' @export
GetRandomFileName <- function(prefix="", ext=".txt", method="guid", rndLen=NULL) {

  rndPart <- NULL
  digits <- c("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")

  if (method == "guid" || method == "guidNoDash") {
    rndPart <- uuid::UUIDgenerate(TRUE)
    if (method == "guidNoDash") {
      rndPart <- gsub("-", "", rndPart, fixed=TRUE)
    }
  }
  if (method == "hex") {
    if (is.null(rndLen) || rndLen <= 0)
      rndLen <- 12
    rndPart <- paste0(sample(c(digits, LETTERS[1:6]), rndLen, replace=TRUE), collapse="")
  }
  if (method == "alphaNum") {
    if (is.null(rndLen) || rndLen <= 0)
      rndLen <- 10
    rndPart <- paste0(sample(c(digits, LETTERS), rndLen, replace=TRUE), collapse="")
  }

  if (is.null(rndPart))
    stop("Invalid 'method' argument to GetRandomFileName. Must be 'guid', 'guidNoDash', 'hex' or 'alphaNum'.")

  paste0(prefix, rndPart, ext)
}

# Default is OUTSIDE the project directory so that it does not show up in GIT
# ../<ProjectFolder>_Data
#' @export
ComposeLocalDataFolderPath <- function(workingDirBaseName) {
  localFilePath <- paste0("../", workingDirBaseName, "_Data")
  localFilePath
}

# ../<ProjectFolder>_Data/<TableName>
#' @export
ComposeLocalFilePath <- function(redshiftTable, workingDirBaseName, subdir=NULL) {
  localFilePath <- paste0(ComposeLocalDataFolderPath(workingDirBaseName), "/", redshiftTable)
  if (!is.null(subdir)) localFilePath <- paste0(localFilePath, "/", subdir)
  localFilePath
}

# ../<ProjectFolder>_Data/<TableName>/Data_000.gz
#' @export
ComposeDataFileName <- function(redshiftTable, workingDirBaseName, subdir=NULL) {
  localFilePath <- ComposeLocalFilePath(redshiftTable, workingDirBaseName, subdir)
  paste0(localFilePath, "/Data_000.gz")
}

# ../<ProjectFolder>_Data/<TableName>/ColumnNames.txt
#' @export
ComposeColumnNamesFileName <- function(redshiftTable, workingDirBaseName, subdir=NULL) {
  localFilePath <- ComposeLocalFilePath(redshiftTable, workingDirBaseName, subdir)
  paste0(localFilePath, "/ColumnNames.txt")
}

# ../<ProjectFolder>_Data/Temp
#' @export
GetTempFolder <- function (workingDirBaseName) {
  folderName <- paste0(ComposeLocalDataFolderPath(workingDirBaseName), "/Temp")
  if (!dir.exists(folderName))
    dir.create(folderName)
  folderName
}

# ../<ProjectFolder>_Data/Models
#' @export
GetModelsFolder <- function (destinationFolder) {
  folderName <- paste0(destinationFolder, "/Models")
  if (!dir.exists(folderName))
    dir.create(folderName)
  folderName
}

# ../<ProjectFolder>_Data/Scoring
#' @export
GetScoringFolder <- function (destinationFolder) {
  folderName <- paste0(destinationFolder, "/Scoring")
  if (!dir.exists(folderName))
    dir.create(folderName)
  folderName
}

ComposeParallelDownloadFolderPath <- function(destinationFolder) {
  paste0(destinationFolder, "/Data")
}
