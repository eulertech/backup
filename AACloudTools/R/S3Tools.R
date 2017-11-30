# S3Tools.R
#
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
#
#   Author:  Christopher Lewis
#
# TODO: consider introducing saveRDSToS3() and readRDSFromS3() to complement SaveToS3 and LoadFromS3 functions.

# private function to obtain the "root" folder
GetS3DataDirectory <- function(projectName) {
  paste0(GetS3DataFolder(), projectName)
}

# GetModelsFolderOnS3() soon to be replaced by GetS3ModelUrl()
#' @export
GetModelsFolderOnS3 <- function (redshiftTable, workingDirBaseName) {
  folderName <- paste0(GetS3DataDirectory(workingDirBaseName), "/Models")
  folderName
}

# @@@ GetScoringFolderOnS3() soon to be replaced by GetS3ScoringUrl()
#' @export
GetScoringFolderOnS3 <- function (redshiftTable) {
  folderName <- paste0(GetS3DataDirectory(workingDirBaseName), "/Scoring")
  folderName
}

# Private function used to produce an S3 URL for a particular type of content (Models, Scoring ...)
# Arguments:
#   XyZFolder = character string: "Models" or "Scoring" or whatever other particular content type.
#               This string is part of the URL and is the only differentiator for the various
#               content types that follow this pattern.
#   Other args = same as for GetS3ModelUrl() and GetS3ScoringUrl()
#   Return value = character string: ready-to-use S3 URL pertaining to the content type and the
#                  particular argument values.
GetS3XyzUrl <- function (XyzFolder, projectName, fileName, subFolder) {
  #TODO: Decide if the elements of the URL should not be forced to lowercase to avoid possible difficulties w/ Unix-like systems.

  if (is.null(projectName)) {
    projectName <- basename(getwd())
  }

  # remove leading and/or trailing slashes and spaces in user supplied parameters
  # so that we avoid bad URLs with double slashes and/or spaces.
  projectName <- str_replace(projectName, "^(/| )+", "")
  projectName <- str_replace(projectName, "(/| )+$", "")

  if (!is.null(fileName)) {
    fileName <- str_replace(fileName, "^(/| )+", "")
    fileName <- str_replace(fileName, "(/| )+$", "")
  }

  if (!is.null(subFolder)) {
    subFolder <- str_replace(subFolder, "^(/| )+", "")
    subFolder <- str_replace(subFolder, "(/| )+$", "")
  }

  retVal <- paste0(GetS3DataDirectory(projectName), "/", XyzFolder, "/")
  if (!is.null(subFolder) && nchar(subFolder) > 0)
    retVal <- paste0(retVal, subFolder, "/")
  if (!is.null(fileName) && nchar(fileName) > 0)
    retVal <- paste0(retVal, fileName)
  retVal
}

#' Construct an S3 URL to particular file (or folder) category
#'
#' These functions return a ready-to-use URL to S3 storage. The specific location and name
#' follows an IHSMarkit convention.
#'
#' @param projectName character string: the name of the project or application. That is typically
#'   the base name of the "root" folder a project/application's source code, and since the working
#'   directory normally points to this folder, the project name defaults to \code{basename(getwd())}
#'   when this argument is passed as NULL.
#' @param fileName character string: the file name.  Use NULL to obtain the S3 URL to the folder
#'   rather than to a specific file.
#' @param subFolder character string: an optional folder or path of folders that is
#'      added to the URL
#' @return a character string with a ready-to-use S3 URL, including its \code{s3:://} prefix.
#'    When \code{filename} is NULL, the last character of the URL returned is a slash.
#'
#' @details
#'
#' \code{GetS3ModelUrl} returns URL to S3 file/folder for Model(s)
#'
#' \code{GetS3ScoringUrl} returns URL to S3 file/folder for Scores/Results.
#'
#' These functions help support and enforce IHSMarkit conventions regarding the names and locations
#' of folders and files produced or used by the applications.  The location and name is based on
#'
#' \itemize{
#'   \item The particular type of content (Model files, Scoring results data, Data tables etc.)
#'   \item The project or application name
#'   \item  Optional elements supplied by the user or the application.
#' }
#'
#' In the case of Models or of Scoring results, where there can be very many such files for one
#' given project/application, the convention only covers the location of the folder where such files
#' should be stored; the naming of the specific file and of an optional optional sub-folder path
#' is left to the user.  The \code{fileName} can also be NULL when one wishes to obtain the URL to
#' the folder (for example to enumerate the models therein, to delete them en-block etc.)
#'
#' @family S3 URL functions
#' @name GetS3XxxUrl
NULL

#' @rdname GetS3XxxUrl
#' @export
GetS3ModelUrl <- function (projectName=NULL, fileName=NULL, subFolder=NULL) {
  GetS3XyzUrl("Models", projectName, fileName, subFolder)
}

#' @rdname GetS3XxxUrl
#' @export
GetS3ScoringUrl <- function (projectName=NULL, fileName=NULL, subFolder=NULL) {
  # TODO: consider renaming.  GetS3ScoresUrl ?  GetS3ResultsUrl?
  GetS3XyzUrl("Scoring", projectName, fileName, subFolder)
}

#' Construct an S3 URL pointing to a temporary folder or file.
#'
#' \code{GetS3TempUrl}   Produce an S3 URL for a \emph{temporary} folder or temporary file.
#' @param fileName character string: name of the file.  May be NULL or "" if we wish to obtain
#'        a URL to a "folder" rather than to a specific file.
#' @param subFolder character string: an optional folder or path of folders that is
#'      added to the URL.
#' @param user character string: the user ID or user name; typically this parameter
#'   is ommited and the user ID or name is inferred from the environment variables.
#'   It may be used for example when several colleagues are working in the same "session", for
#'   example allowing someone to peek at the log file produced under a colleague's session.
#' @return a character string such as \code{"s3://ihs-temp/myBigProject/JohnSmith/myFile.txt"}.
#'   This URL points to an S3 bucket explicitly designated for temporary files.  Also,
#'   it includes the name of the project and of the user as so to avoid conflict with others.
#' @details
#'   This function merely create the \emph{URL} (string) which can then be passed to
#'   functions such as \code{UploadFileToS3()}, \code{DownloadFileFromS3} and the like
#'   to effectively read, write or delete file(s) at the underlying location.
#'
#'   Note that any file stored under such a URL should be TEMPORARY in nature. Although one is
#'   responsible for deleting any file he/she created at such location, and although these URLs
#'   are user-specific, one should never use a temporary URL to store assets that cannot be
#'   re-created.  Data assets meant to have a filespan longer than the running of a single
#'   script (or possibly of a sequence of tightly coupled scripts) should be saved to other
#'   locations.
#'
#' @examples
#' # In the following examples,
#' #   BlueLemon is the name of the R project (derived from current directory) and
#' #   ckb12473 is the user ID
#' # both of these are fictuous, for illustration purposes only.
#'
#' GetS3TempUrl("SemiCookedData.csv", "DiamondTrail")
#' # [1] "s3://ihs-temp/BlueLemon/temp/ckb12473/DiamondTrail/SemiCookedData.csv"
#'
#' GetS3TempUrl("debug_log.txt")
#' # [1] "s3://ihs-temp/BlueLemon/temp/ckb12473/debug_log.txt"
#'
#' GetS3TempUrl(NULL, "CitrusCollection")
#' # [1] "s3://ihs-temp/BlueLemon/temp/ckb12473/CitrusCollection/"
#'
#' # Passing the user ID of another colleague.  This is rarely needed.
#' GetS3TempUrl("debug_log.txt", NULL, "rkj23821")
#' # [1] "s3://ihs-temp/BlueLemon/temp/rkj23821/debug_log.txt"
#'
#' @family S3 URL functions
#' @export
GetS3TempUrl <- function(fileName=NULL, subFolder=NULL, user=NULL) {
  if (is.null(user) || nchar(user) == 0) {
    user <- Sys.getenv("USER")
    if (nchar(user) == 0)
      user <- Sys.getenv("USERNAME")
    if (nchar(user) == 0)
      stop(paste("GetS3TempUrl() is missing 'User' text. Ensure that 'USER' or 'USERNAME' environment",
                 "variable is set or that 'user' argument is explicitly passed."))

  }

  retVal <- paste0(GetS3TempFolder(), basename(getwd()), "/temp/", user, "/")
  if (!is.null(subFolder) && nchar(subFolder) > 0)
    retVal <- paste0(retVal, subFolder, "/")
  if (!is.null(fileName) && nchar(fileName) > 0)
    retVal <- paste0(retVal, fileName)

  retVal
}



# Function to download file from S3 to the local drive
DownloadS3FilesForRedshiftPrivate <- function(tempFileInS3BucketFolder, destinationFolder, parallel) {
  message(Sys.time(), " - Downloading file from S3...")

  # Remove existing files or directories from previous downloads
  destinationFile <- paste0(destinationFolder, "/Data_*.gz") # Keep all the data files in the data directory
  unlink(destinationFile)
  paralledDownloadFolder <- ComposeParallelDownloadFolderPath(destinationFolder)
  unlink(paralledDownloadFolder, recursive = TRUE)

  if (parallel) destinationFolder <- ComposeParallelDownloadFolderPath(destinationFolder) # Keep all the data files in the data directory

  command <- paste0("aws s3 cp ", tempFileInS3BucketFolder, " ", destinationFolder, " --recursive --only-show-errors")
  system(command)
}

#' Download file(s) from S3 storage to local drive
#'
#' \code{DownloadFileFromS3()} Copy one or several files from S3 to local storage
#'
#' @param s3FileOrFolder character string : the URL of the targeted file or folder on S3. This must
#'   include the s3:// prefix.  If a file name is provided then only this file name will be
#'   downloaded.
#' @param dataFileOrFolder character string : the destination folder (and optional alternate file name)
#'   on the local storage where the S3 file(s) is (are) to be copied.
#' @param recursive logical : when \code{TRUE} the files found in subfolders of the
#'   \code{s3FileOrFolder} are copied as well, recursively, i.e. their subfolders of these subfolders
#'   are copied as well, etc.  Defaults to \code{FALSE}.
#' @return integer error code. 0 = success,  other values are typically indicative of an error.
#'
#' @examples
#' # **** This example demonstrates both UploadFileToS3() and DownloadFileFromS3()
#' # Setup: (not part of example per-se) Create a couple of local files to copy to S3.
#' #   Note: it is convenient to use load()/save() for this example, but if you effectively
#' #   need to to persist R objects to S3, the AACloudTools functions SaveToS3() and
#' #   LoadFromS3() can do this in one step (making the need of a transient local file unecessary)
#' var1 <- 12
#' var2 <- "Two"
#' save(var1, var2, file="TestFile1328A.RData")
#' var1 <- 33
#' save(var1, var2, file="TestFile1328B.RData")
#'
#' # Access to S3 only requires that Configuration for AWS services has been previously
#' # made available; this is typically done in the application initializatin phase.
#' ConfigureAWS("Config/config.json")
#' # Define where you want to store the file on S3
#' #    GetS3TempUrl()  for temporary files (as here)
#' #    GetS3ModelUrl() for modeling-related files
#' #    GetS3XxxUrl()   etc.
#' s3Location <- GetS3TempUrl(NULL, subFolder="ResultsFiles", user="Tester")
#'
#' UploadFileToS3("TestFile1328A.RData", s3Location)
#' UploadFileToS3("TestFile1328B.RData", s3Location)
#'
#' # to check if these files can be downloaded back;
#' var1 <- 9999
#' var2 <- "bad"
#' DownloadFileFromS3(paste0(s3Location, "TestFile1328A.RData"), "back_1328A.RData")
#' load("back_1328A.RData")
#' var1
#' # [1] 33   !!! it worked...
#' var2
#' # [1] "Two"  Yep. That's what we had in the "A" file.
#' DownloadFileFromS3(paste0(s3Location, "TestFile1328B.RData"), "back_1328B.RData")
#' load("back_1328B.RData")
#' var1
#' # [1] 33    Tah dah!
#'
#' # clean-up...
#' # ... On S3
#' RemoveFileFromS3(paste0(s3Location, "TestFile1328A.RData"))
#' RemoveFileFromS3(paste0(s3Location, "TestFile1328B.RData"))
#' # alternatively: but beware a typo or a few seconds inattention could result in a lot
#' # more "clean-up" than intended...
#' RemoveFolderFromS3Recursive(s3Location)
#' # ... and locally
#' rm(var1, var2)
#' file.remove("TestFile1328A.RData", "TestFile1328B.RData",  "back_1328A.RData",  "back_1328B.RData")
#' @seealso \code{\link{UploadFileToS3}}
#' @family S3 functions
#' @export
DownloadFileFromS3 <- function(s3FileOrFolder, dataFileOrFolder, recursive=FALSE) {
  message(Sys.time(), " - Downloading file from S3...")
  command <- paste0("aws s3 cp ", s3FileOrFolder, " ", dataFileOrFolder, " --only-show-errors")
  if (recursive)
    command <- paste0(command, " --recursive")

  system(command)
}

#' Upload file from local drive to S3 storage
#'
#' \code{UploadFileToS3}   Copy a local file to S3
#' @param dataFileName Path and Name of local file.
#' @param s3FileName   S3 URL indicating the S3 "directory" and optionally the
#'    name of the destination file; must include the "s3://" prefix; if file
#'    name is not supplied, that of the local file is used.
#' @examples
#' # (also see the examples for \code{DownloadFileFromS3})
#'
#' # source: relative path;  destination: under a different file name
#' UploadFileToS3("Results/Report.txt", "s3://ihs-temp/results/report_20160927.txt")
#'
#' # source: relative path;  destination: implicit file name (S3 file will be 'Report.txt')
#' UploadFileToS3("Results/Report.txt", "s3://ihs-temp/results/")
#'
#' # source: absolute path;  destination: implicit file name (S3 file will be 'January.zip')
#' UploadFileToS3("C:/Data/January.zip", "s3://ihs-lake-01/Maritime/AggregateData/")
#' @seealso \code{\link{DownloadFileFromS3}}
#' @family S3 functions
#' @export
UploadFileToS3 <- function(dataFileName, s3FileName) {
  message(Sys.time(), " - Uploading file to S3...")
  command <- paste0("aws s3 cp ", dataFileName, " ", s3FileName, " --only-show-errors")
  system(command)
}

#' Delete file in S3 storage
#'
#' \code{RemoveFileFromS3}  Delete a file from S3
#' @param s3FileName  S3 URL of the file to be deleted; the "S3://" prefix is required
#' @examples
#' # file test12.xls (if if exists) will be permanently removed from jeff folder
#' RemoveFileFromS3("s3://ihs-temp/jeff/test12.xls")
#' @family S3 functions
#' @export
RemoveFileFromS3 <- function(s3FileName) {
  # S3 allows $ signs for names.  Need to put a '' around the string otherwise linux will interpret the $ sign as variable
  command <- paste0("aws s3 rm ", s3FileName, " --only-show-errors")
  system(command)
}

#' Delete recurively folder(s) and files in S3 storage.
#'
#' \code{RemoveFolderFromS3Recursive} Delete, \emph{recurively}, folders and files in S3
#' \strong{Attention!} This command makes it easy to wipe out more out of S3 than one effectively intends to.
#' @param s3Folder S3 bucket where every file or "subfolder" is to be deleted. This
#'    parameter needs to start with the 's3://' prefix.
#' @examples
#' # Again... Sharp tool:  make sure the s3Folder parameter is quite specific
#' # and effectively corresponds to what you want to delete
#' RemoveFolderFromS3Recursive("s3://ihs-temp/mystuff/test22")
#' @family S3 functions
#' @export
RemoveFolderFromS3Recursive <- function(s3Folder) {
  # S3 allows $ signs for names.  Need to put a '' around the string otherwise linux will interpret the $ sign as variable
  command <- paste0("aws s3 rm ", s3Folder, "  --recursive --only-show-errors")
  system(command)
}

#' Save R Objects to S3 Storage.
#'
#' \code{SaveToS3} is just like the \link[base]{save} function in {base} but the resulting RData file is stored to S3.
#'
#' See the  \link[base]{save} function documentation for more detail on the parameters and on the
#' \emph{save} operation per se.
#'
#' The object(s) saved or loaded with \code{SaveToS3} or its reverse function, \code{LoadFromS3}
#' fit in R memory and hence are relatively small; for this reason, and because .RData files
#' are rather atomic and only readable sequentially, the data is saved as a single S3 file (unlike
#' what is done with bigger data tables and such which are split over mulitple files for performance
#' purposes.)
#'
#' @param ... The names of the objects to be saved (as symbols or character strings).
#' @param list A character vector containing the names of objects to be saved.
#' @param s3File A character string with the URL to the S3 file where the serialized object(s) will be saved.
#'   If NULL, the URL to a temporary file name with extension .RData is used; this URL is produced with
#'   \code{GetS3TempUrl()}.  This parameter is the only one which differs from these of
#'   the \code{save()} function.
#' @param ascii logical Should object(s) be serialized as ASCII; defaults to \code{FALSE} which produces
#'   a binary format.
#' @param version For controlling the workspace format version. Typically leave \code{NULL} which
#'   specifies current version.
#' @param envir The environment where to search for the objects to be saved.
#' @param compress To specify whether and how to compress the serialized data; defaults to \code{TRUE}
#'   if \code{ascii} parameter is \code{FALSE}, \code{TRUE} otherwise.
#' @param compression_level The level of compression to be used.
#' @param eval.promises Logical: should objects which are promises be forced before saving?
#' @param precheck Logical: should existence of the objects be checked before saving?
#' @return A character string with the URL of the S3 file where the object(s) were saved.
#'
#' @examples
#' SaveToS3(FittedModel, file="s3://ihs-aa/Mar/Baltimore/model/20160801.RDdata")
#'
#' @seealso \code{\link{LoadFromS3}}
#' @family S3 functions
#' @export
SaveToS3 <- function(..., list=character(),
         s3File=NULL,
         ascii=FALSE, version=NULL, envir=parent.frame(),
         compress=isTRUE(!ascii), compression_level,
         eval.promises=TRUE, precheck=TRUE) {

  if (is.null(s3File) || s3File == "") {
    s3File <- GetS3TempUrl(GetRandomFileName("", ".RData"))
  }

  #TODO: consider using tmp folder rather than project folder
  # Name of local file where the RData file is initially created
  #   The name doesn't really matter: we use that of the target file plus some random
  #   prefix to avoid possible conflict.
  localFile <- paste0("./tmp", paste0(sample(LETTERS, 4, replace=TRUE), collapse=""),
                        "_", basename(s3File))

  objNames <- as.character(substitute(list(...)))[-1L]
  objNames <- c(objNames, list)
  save(list=objNames, file=localFile, ascii=ascii, version=version, envir=envir, compress=compress,
       compression_level=compression_level, eval.promises=eval.promises, precheck=precheck)

  UploadFileToS3(localFile, s3File)
  unlink(localFile)

  s3File
}


#' Reload Saved Dataset from S3 Storage
#'
#' \code{LoadFromS3} is just like the \link[base]{load} function in {base} but the resulting
#'   RData file comes from a file in S3 storage.
#'
#' See the  \link[base]{load} function documentation for more detail on the parameters and on the
#' \emph{load} operation per se.
#'
#' @param s3File A character string: the URL to the S3 file. Must start with "s3://"
#' @param envir  The environment where the data should be loaded.
#' @param verbose Logical: should progress info such as the name of the loaded objects be printed to the console?
#' @return A character vector of the names of the objects created, invisibly.
#'
#' @examples
#' LoadFromS3("s3://ihs-aa/Mar/Baltimore/model/20160801.RDdata")
#'
#' loadedObjs <- LoadFromS3("s3://ihs-temp/Project17/temp/Jeff/PrecookedData.RData")
#'
#' @seealso \code{\link{SaveToS3}}
#' @family S3 functions
#' @export
LoadFromS3 <- function(s3File, envir=parent.frame(), verbose=FALSE) {

  #TODO: consider using tmp folder rather than project folder
  localFile <- paste0("./tmp", paste0(sample(LETTERS, 4, replace=TRUE), collapse=""),
                      "_", basename(s3File))
  DownloadFileFromS3(s3File, localFile)
  retVal <- load(localFile, envir, verbose)
  unlink(localFile)
  retVal
}

#' @export
DownloadModelsFileFromS3 <- function(redshiftTable, workingDirBaseName, fileName) {
  # Get S3 pathname
  s3Folder <- GetModelsFolderOnS3(redshiftTable)
  s3fileName <- paste0(s3Folder, "/", fileName)

  # Get local pathname
  destinationFolder <- ComposeLocalFilePath(redshiftTable, workingDirBaseName)
  localFolder <- GetModelsFolder(destinationFolder)
  localFileName <- paste0(localFolder, "/", fileName)

  DownloadFileFromS3(s3fileName, localFileName)

  localFileName
}

#' @export
DownloadScoringFileFromS3 <- function(redshiftTable, workingDirBaseName, fileName) {
  # Get S3 pathname
  s3Folder <- GetScoringFolderOnS3(redshiftTable)
  s3fileName <- paste0(s3Folder, "/", fileName)

  # Get local pathname
  destinationFolder <- ComposeLocalFilePath(redshiftTable, workingDirBaseName)
  localFolder <- GetScoringFolder(destinationFolder)
  localFileName <- paste0(localFolder, "/", fileName)

  DownloadFileFromS3(s3fileName, localFileName)

  localFileName
}

#' @export
UploadModelsFileToS3 <- function(redshiftTable, workingDirBaseName, fileName) {
  # Get S3 pathname
  s3Folder <- GetModelsFolderOnS3(redshiftTable)
  s3fileName <- paste0(s3Folder, "/", fileName)

  # Get local pathname
  destinationFolder <- ComposeLocalFilePath(redshiftTable, workingDirBaseName)
  localFolder <- GetModelsFolder(destinationFolder)
  localFileName <- paste0(localFolder, "/", fileName)

  UploadFileToS3(localFileName, s3fileName)

  s3fileName
}

#' @export
UploadScoringFileToS3 <- function(redshiftTable, workingDirBaseName, fileName) {
  # Get S3 pathname
  s3Folder <- GetScoringFolderOnS3(redshiftTable)
  s3fileName <- paste0(s3Folder, "/", fileName)

  # Get local pathname
  destinationFolder <- ComposeLocalFilePath(redshiftTable, workingDirBaseName)
  localFolder <- GetScoringFolder(destinationFolder)
  localFileName <- paste0(localFolder, "/", fileName)

  UploadFileToS3(localFileName, s3fileName)

  s3fileName
}

# Location for S3 Temp bucket.  Typically "s3://ihs-temp/" unless using ECR
#' @export
GetS3TempFolder <- function () {
  folderName <- Sys.getenv("S3TEMPFOLDER")
  folderName
}

# Location for S3 Temp bucket.  Typically "s3://ihs-bda-data/projects/" unless using ECR
#' @export
GetS3DataFolder <- function (workingDirBaseName) {
  folderName <- Sys.getenv("S3DATAFOLDER")
  folderName
}

