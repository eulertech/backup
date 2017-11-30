# LL_RedShiftTools.R
#   A collection of low-level functions for use with Redshift.
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
#
#   Author:  Tim Kynerd
#

# To do: Write unit test
#' SQL update function (delete and insert)
#'
#' \code{SqlDeleteInsert} Performs an SQL row update by deleting from
#'    and inserting into a table.
#'
#' This function facilitates single-row, or similarly limited, updates to Redshift
#' tables by combining a Redshift table delete and insert into a single function.
#'
#' This function expects, as input, a condition that specifies the row(s) to be
#' deleted, and the VALUES clause of an INSERT statement for the corresponding
#' row(s) to be inserted, with all column values specified for each row.
#' If the row(s) to be inserted is/are contained in a data frame, the
#' \code{GetInsertStmtForDataFrame} function can be used with that data frame
#' to obtain the corresponding VALUES clause.
#'
#' @param conn JDBC connection : An object associated with a JDBC connection to
#'   Redshift. Typically this would be obtained by calling the \code{GetRedshiftConnection}
#'   function.
#' @param table character string : A string identifying the Redshift table to be
#'   updated. This should normally include the schema in which the table is held
#'   (e.g., "ra.absd_ship_search").
#' @param deleteCond character string : A string containing a condition that
#'   uniquely identifies the row(s) to be deleted from the Redshift table.
#' @param insertValues character string : A string containing the VALUES clause
#'   for the row(s) to be inserted into the Redshift table. This includes the word
#'   "VALUES" followed by the values for all columns on each row, with each row's
#'   values enclosed in parentheses. As mentioned above, the \code{GetInsertStmtForDataFrame}
#'   function will automatically generate this string from a data frame.
#' @return Always \code{NULL}.
#'
#' @examples
#' \dontrun{
#' # Obtain Redshift connection
#' myConn <- GetRedshiftConnection()
#' # Generate VALUES clause from (separately prepared) data frame
#' valuesClause <- GetInsertStmtForDataFrame(valuesdf)
#' # Update Redshift table
#' SqlDeleteInsert(myConn, "ra.absd_ship_search", "lrno = 1234567", valuesClause)
#'}
#'
#' @seealso \link{GetInsertStmtForDataFrame} and \link{GetRedshiftConnection}
#' @family Redshift functions

#' @export
SqlDeleteInsert <- function(conn, table, deleteCond, insertValues) {

  dbSendUpdate(conn, paste("DELETE FROM", table, "WHERE", deleteCond))
  dbSendUpdate(conn, paste("INSERT INTO", table, insertValues))

}

# To do: Write unit test
#' Create VALUES clause for SQL INSERT statement from data frame
#'
#' \code{GetInsertStmtForDataFrame} Creates a VALUES clause (for an SQL INSERT statement)
#'    from the values in a data frame.
#'
#' This function uses the contents of a data frame to generate an SQL VALUES clause
#' corresponding to the rows of the data frame. This clause can then be used, for example,
#' in a call to the \code{SqlDeleteInsert} function.
#'
#' This function expects, as input, a data frame.
#'
#' \strong{NOTE:} This function is not intended for use with large data frames. For one thing, the
#' INSERT statement performs poorly when making large numbers of individual row insertions.
#' For another, Redshift limits the length of SQL statements to 16 MB; the VALUES clause
#' for a large data frame could easily exceed this limit, since every individual value in
#' the data frame will be included.
#'
#' @param df data frame : A data frame containing the data intended for insertion
#'   into a Redshift table.
#' @return A character string containing the VALUES clause corresponding to the
#'   contents of the input data frame.
#'
#' @examples
#' \dontrun{
#' # Obtain Redshift connection
#' myConn <- GetRedshiftConnection()
#' # Generate VALUES clause from (separately prepared) data frame
#' valuesClause <- GetInsertStmtForDataFrame(valuesdf)
#' # Update Redshift table
#' SqlDeleteInsert(myConn, "ra.absd_ship_search", "lrno = 1234567", valuesClause)
#'}
#'
#' @seealso \link{SqlDeleteInsert}
#' @family Redshift functions

#' @export
GetInsertStmtForDataFrame <- function(df) {

  mustBeQuoted <- c("character", "date", "Date")
  isQuoted <- sapply(colnames(df), function(colName) class(df[, colName]) %in% mustBeQuoted)

  GetSqlForOneRow <- function(row, isQuoted) {
    rowVals <- sapply(colnames(row), function(colName) ifelse(isQuoted[colName], paste0("'", row[, colName], "'"), row[, colName]))
    paste0("(", paste(rowVals, collapse=", "), ")")
  }

  retVal <- paste0("VALUES ", paste0(sapply(seq(nrow(df)), function(i) GetSqlForOneRow(df[i, ], isQuoted)), collapse=", "))
}
