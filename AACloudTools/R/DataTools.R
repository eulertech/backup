# DataTools.R
#
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
#
#   Author:  Christopher Lewis
#

#' @export
ChunkRecords <- function(nRecords, chunksSize) {
  chunkStart <- seq.int(1, nRecords, chunksSize)
  chunkEnd <- chunkStart + chunksSize -1

  # Fix last element
  chunkEnd[length(chunkEnd)] <- nRecords

  returnValue <- data.frame(start=chunkStart, end=chunkEnd)
  returnValue
}

#' @export
ChunkIds <- function(ids, chunkSize) {
  sortedIDs <- sort(ids)

  recs <- data.table(ChunkRecords(length(sortedIDs), chunkSize))
  recs[, start1:=sortedIDs[start]]
  recs[, end1:=sortedIDs[end]]
  recs[, c("start", "end"):=NULL]
  setnames(recs, c("start", "end"))
  recs
}

# Find rows in df1 that are not in df2
#' @export
RowsInDf1ThatAreNotInDf2  <- function(df1, df2)
{
  # Utility copied from the web
  df1.vec <- apply(df1, 1, paste, collapse = "")
  df2.vec <- apply(df2, 1, paste, collapse = "")

  missingRows <- df1[!df1.vec %in% df2.vec,]
  return (missingRows)
}
