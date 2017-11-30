# DiagnosticsTools.R
#   A loose collection of functions which can be used for probe / diagnostics purposes.
#   Although most of these functions are typically not included in production software, they can
#   be quite useful in the context of AACloudTools-based logic and beyond.
#
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016-17
#
#   Author:  Marc Veillet
#

#' Test Memory Allocation Limits
#'
#' \code{AADiag_MemAllocTest} Attempt to allocate blocks of memory of a given size a given number of times
#'
#' This function can be used to assert the abilty of allocating memory blocks. By default, the memory used
#' during the test is released, but it is possible to prevent this, for example to call the function multiple
#' times as a way of detecting the effective memory limit etc.
#'
#' \strong{Return value}:
#'
#'  When \code{releaseMem=TRUE} : 0 if successful and the number of allocated blocs if the test failed.
#'
#' When \code{releaseMem=FALSE} : A \code{list} with the allocated blocks (these are numeric vectors) if
#' succesful, \code{NULL} otherwise (the blocks are released by the function in that case.)
#'
#' @param blockSize integer : the size of individual memory blocks to allocate (unit = MBbytes,
#'    i.e. 1,048,576 bytes).
#' @param blockCount integer : The number of blocs to allocate.
#' @param releaseMem logical : Whether the memory allocated during the test should be released.
#'    Defaults to \code{TRUE}
#' @param verbose logical : Whether the function should issue informational messages or error messages
#'   during the test.
#' @return  if \code{releaseMem} is \code{TRUE}, an integer otherwise, a list with the allocated blocks.  See the
#'   'Details' section.
#' @export
AADiag_MemAllocTest <- function(blockSize=500L, blockCount=1L,
                                releaseMem=TRUE, verbose=TRUE) {
  orig <- "AADiag_MemAllocTest"
  # *** Various constants ***
  # NUMERIC_SIZE is size, in bytes, of one numeric in memory  (expect 8 bytes on most R implemtations)
  NUMERIC_SIZE <- as.integer(object.size(vector("numeric", 1)) - 40L)
  MEGA_BYTE <- 1024L * 1024L
  NUMERICS_IN_1MEG <- MEGA_BYTE %/% NUMERIC_SIZE

  if (verbose) {
    gcData <- gc()
    DoLog(sprintf("Before: Total 'VCell' memory used: %f", gcData["Vcells", 2]), "INFO", origin = orig)
  }

  numericsPerBlock <- NUMERICS_IN_1MEG * blockSize
  allocatedBlocks <- list()
  nbAllocatedBlocks <- 0

  for (iBlock in seq(blockCount)) {
    allocOk <- tryCatch({
      allocatedBlocks[[iBlock]] <- vector("numeric", numericsPerBlock)
      TRUE
    }, error = function(e) {
      if (verbose) {
        DoLog(sprintf("Failed to allocate block #%d.  Exception :", iBlock), "ERROR")
        errMsg <- paste0(e)   # not sure needed, just trying to ensure that we get a string suitable for the log
        DoLog(errMsg)
      }
      FALSE
    }
    )

    if (!allocOk) {
      break;
    }
    nbAllocatedBlocks <- nbAllocatedBlocks + 1

    if (verbose) {
      gcData <- gc()
      DoLog(sprintf("Allocated block #%d (%d MBytes). Total 'VCell' memory used: %f",
                    iBlock, blockSize, gcData["Vcells", 2]),
            "DBG")
    }

    gc()
  }

  if (releaseMem || nbAllocatedBlocks != blockCount) {
    allocatedBlocks <- NULL
  }

  if (nbAllocatedBlocks == blockCount) {
    if (releaseMem)
      retVal <- 0
    else
      retVal <- allocatedBlocks
  } else {
    if (releaseMem)
      retVal <- nbAllocatedBlocks
    else
      retVal <- NULL
  }

  if (verbose) {
    gcData <- gc()
    DoLog(sprintf("On exit: Total 'VCell' memory used: %f",  gcData["Vcells", 2]), "INFO", origin = orig)
  }

  retVal
}

#' Automated R Style and xxx Review (Lint)
#'
#' \code{AALint} Utility to flag non-compliant style and suspiious constructs.
#'
#' This utilty reviews the source code in a file a produces a list of errors and warning, labelled by line
#' number, for each instance of non-compliance or otherwise questionnable construct in the file.
#'
#' The file itself is not modified.
#'
#' At this time, the list of conventions and best practices asserted in the automated review
#' process is relatively limited, but this is a good start...  We anticipate this function will
#' become more sophisticated and with many additional assertions by the adding or tailoring of
#' various linters.
#'
#' @param fileName character : name of source file, with an optional full or partial path.
#' @parm maxLineLength integer : the maximum length of lines in the source code.  \code{NULL}
#'    provides the default length; 0 or a negative number results in disabling the line_length
#'    linter.
#' @return  none; this function is called for its side effect of printing the list of places in
#'     the source code where some improvement is required or suggested.
#' @export
AALint <- function(fileName, maxLineLength=NULL) {

  if (is.null(maxLineLength))
    maxLineLength <- 120
  if (maxLineLength <= 0)
    maxLineLength <- 9999   # no real limit

  linters <- lintr::with_defaults(line_length_linter(maxLineLength),
                                  camel_case_linter     = NULL,
                                  commented_code_linter = NULL)

  if (stringr::str_detect(fileName, "\\.r$")) {
    warning(sprintf("Invalid filename: %s ; extension should be '.R' (upperase!)", fileName))
  }

  lintr::lint(fileName, linters = linters)

}
