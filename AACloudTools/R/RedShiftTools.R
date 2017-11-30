# RedShiftTools.R
#
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
#
#   Author:  Christopher Lewis and Marc Veillet
#

#' @export
PSqlExecute <- function(fileName) {
  message(Sys.time(), " - PSQL Processing file: ", fileName)

  hostName <- Sys.getenv("PGHOSTNAME")
  dbName <- Sys.getenv("PGDBNAME")
  port <- Sys.getenv("PGPORT")

  # Ensure psql is in the path.  Don't hard code in file
  command <- paste0("psql -q -v ON_ERROR_STOP=1 -h ", hostName, " -d ", dbName, " -p ", port, " -f ", fileName)
  retVal <- system(command)

  if (retVal!=0)
    stop("\nFailed processing file: ", fileName, "\n")
}


#' Open new connection to Redshift
#'
#' \code{GetRedshiftConnection} Returns a connection object suitable to submit
#'    queries to Redshift.
#'
#' This and related functions provide a relatively low level interface to
#' a Redshift cluster.  Although it is useful and efficient to work at this level, remember
#' that several \code{AACloudTools} functions such as \code{DownloadRedshiftTable} or
#' \code{SqlExecute} typically provide an easier way to perform similar tasks.
#'
#' This function relies, indirectly, almost exclusively on the information contained
#' in the the \code{Config/config.json} file to produce the parameters it needs.
#' Specifically, it obtains these from enviromnental variables which were
#' produced by \code{ConfigureAWS()} on the basis of the JSON configuration
#' file passed to it.  \code{ConfigureAWS("./Config/config.json")} should be called
#' once, early in the initialization process of the application; this then allows
#' subsequent use of Redshift and other Amazon AWS services.
#'
#' Note that in case a connection cannot be obtained, the function returns
#' \code{NULL}, after issuing a error message to the Log, rather than failing
#' then and there.  Failure to connect typically leads to fatal errors, but this
#' graceful exit allows the calling logic to use to test for the error condition
#' and in turn decide between a hard stop and alternatives; the primary use of
#' this is to allow parallel threads (e.g. SNOW processing) to return to the
#' master node with a useful message rather than crashing.
#'
#' It is recommended to explicitly close the connection object when it is not
#' needed anymore, using \code{dbDisconnect()}
#'
#' @param url character string : the URL to the desired Redshift cluster, When
#'   NULL this defaults to \code{Sys.getenv("PGURLRS")}. Its format should be:
#'   \code{jdbc:redshift://<server_dns_name_or_IP_Addr>:<port>/<database>}
#' @return a \code{DBIConnection}-derived object suitable to use with various
#'   \code{DBI} functions such as \code{dbGetQuery()} or \code{dbExistsTable()}.
#'   In case of error, returns \code{NULL}.
#'
#' @examples
#' \dontrun{
#' # Early in application flow, configure the AWS services
#' ConfigureAWS("./Config/config.json")
#' # ...
#' myConn <- GetRedshiftConnection()
#' mydf <- dbGetQuery(myConn, "SELECT TOP 30 * from ra.ABSD_SHIP_SEARCH")
#'}
#'
#' @seealso \link{ConfigureAWS} and DBI package (\link[DBI]{DBIConnection-class})
#' @family Redshift functions
#' @export
GetRedshiftConnection <- function(url=NULL) {
  orig <- "AACloudTools::GetRedshiftConnection()"
  if (is.null(url)) {
    url <- Sys.getenv("PGURLRS")
    if (is.null(url) || url == "") {
      msg <- "Cannot open Redshift connection: remember to call ConfigureAWS() and to optionnaly suppy URL to RS cluster."
      DoLog(msg, "FATAL", origin=orig)
      stop(msg)
    }
  }
  driverPath <- system.file("jars", "RedshiftJDBC41-1.1.7.1007.jar", package="AACloudTools")
  retVal <- NULL
  tryCatch({
    driver <- JDBC(Sys.getenv("PGDRIVERCLASS"), driverPath)
    retVal <- dbConnect(driver, url, Sys.getenv("PGUSER"), Sys.getenv("PGPASSWORD"))
    },
    warning=function(w) {
      DoLog(sprintf("Warning while warning while connecting to [%s]. Message=%s", url, w), "WARN", origin=orig)
      if (is.null(retVal)) {
        DoLog("Failed to obtain connection", "ERROR", origin=orig)
      }
    },
    error=function(e) {
      retVal <- NULL
      DoLog(sprintf("Error while connecting to [%s].  Error=%s", url, e), "ERROR", origin=orig)
    }
  )
  retVal
}

#' Fetch Redshift Data into a data.frame
#'
#' \code{SqlToDf} Returns data.frame with all the data produced by a SQL query targeted at Redshift
#'
#' Aside from "one-lining" queries into Redshift, the key benefit of this function is to provide
#' multiple methods for fetching the data.
#'
#' The \strong{\code{"direct"}} method uses \code{DBI::dbGetQuery()} to submit the query and to retrieve its
#' output directly into a data.frame.  This approach has a very low latency, but become slower when the
#' amount of data returned grows.
#'
#' The \strong{\code{"viaS3"}} method instructs Redshift to "dump" the data to S3 (\code{UNLOAD} SQL command), it
#' then copies the S3 file(s) to the local host and finally load the data to a data.frame using \code{read.csv()}.
#' The implementation also transparently handles the housekeeping associated with these intermediate files.  Although
#' this methos has a higher latency, it becomes more efficient than the \code{"direct"} approach for cases when the
#' amount of data retrieved is significant.
#'
#' An upcoming feature of this function is the \code{"auto"} method which will automatically select the effective
#' method based on the amount of data expected.
#'
#' @param sql character vector : the SQL query.  The elements of this vector are \code{paste}-ed together to produce
#'   the query effectively submitted (Allowing for a vector rather than requesting a single string makes it easier
#'   for the calling logic to break the, often long, query over multiple lines in the source code).  Also,
#'   \code{sql} may be made of a single word, which results in an effective query of \code{"SELECT * FROM <this_word>"};
#'   this makes it convenient to fecth whole tables or views.
#' @param cn connection object : a \code{DBIConnection}-derived object such as one returned by \code{GetRedshiftConnection()}.
#'   When \code{NULL}, the function automatically produces such a connection and dispose of it before returning.
#' @param method character string : one of \code{"direct" or "viaS3"} see Details section.
#'
#' @return a \code{data.frame} with all the data produced by the query, or NULL in case of error.
#'
#' @examples
#' \dontrun{
#' # Early in application flow, configure the AWS services
#' ConfigureAWS("./Config/config.json")
#' # ...
#' mydf <- SqlToDf(c("SELECT TOP 25 *",
#'                   "FROM ra.ABSD_SHIP_SEARCH",
#'                   "WHERE flag = 'PAN' AND statuscode = 'S'",
#'                   "ORDER BY dwt DESC")
#'}
#'
#' @family Redshift functions
#' @export
SqlToDf <- function(sql, cn=NULL, method="direct") {
  cnWasNull <- is.null(cn)
  if (cnWasNull) {
    cn <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cn))
      return(NULL)  # (appropriate messages would readilly been issued by GetRedshiftConnection.)
  }

  if (is.null(sql) || sql == "") {
    msg <- "Invalid argument: 'sql' cannot be NULL or empty."
    stop(msg)
  }

  if (!str_detect(method, "^(direct|viaS3)$")) {
    msg <- "Invalid argument: 'method' must be one of 'direct' or 'viaS3'"
    stop(msg)
  }

  sql <- SqlVectorToString(sql)

  # dispatch the "direct" method at once and return since it is by far the simplest
  if (method == "direct") {
    retVal <- dbGetQuery(cn, sql)
    if (cnWasNull)
      dbDisconnect(cn)
    return(retVal)
  }

  # ****** "viaS3" method *******
  wrk <- paste0("Tmp", str_sub(str_replace_all(uuid::UUIDgenerate(), "-", ""), 1L, 12L))

  stop("viaS3 method = future implementation!")
  # ~ DownloadRedshiftTable() (but with get top 1 to get the names) + not get them as a file ?
  # +
  # retVal <- read.table(ComposeDataFileName(AisDestToLocTable, workingDirBaseName),
  #                            sep="|",
  #                            header = FALSE,
  #                            fill = TRUE,
  #                            stringsAsFactors =FALSE,
  #                            comment.char = '',
  #                            quote = '')
  #
  # varnames <- read.table(ComposeColumnNamesFileName(AisDestToLocTable, workingDirBaseName))
  # names(AisDestToLoc) <- as.character(varnames$V1)


  if (cnWasNull) {
    dbDisconnect(cn)
  }
}

GetRecordCountPrivate <- function(cnRs, redshiftTable, where=NULL) {
  countQuery <- paste0("select count(*) from ", redshiftTable)
  if (!is.null(where)) countQuery <- paste0(countQuery, " where ", where)
  dfCount <- dbGetQuery(cnRs,  countQuery)

  retValue <- list()
  retValue$count <- dfCount$count
  retValue$query <- countQuery

  retValue
}

#' Get the number of records in a Redshift Table.
#'
#' \code{GetRecordCount} Returns the number of records in a Table in Redshift.
#' @param redshiftTable the name of the table for which we want the COUNT(*).  This should
#'   include the Schema name.
#' @return an integer with the number of records found in the table (the result
#'   of \code{SELECT COUNT(*) FROM \emph{redshifttable}})
#' @family Redshift functions
#' @export
GetRecordCount <- function(redshiftTable) {
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  retValue <- GetRecordCountPrivate(cnRs, redshiftTable)
  dbDisconnect(cnRs)

  retValue$count
}

# Function to unload the redshift table to a temporary S3 bucket.  Its faster to unload via S3
DownloadRedshiftTableToS3Private <- function(cnRs, redshiftTable, tempFileInS3BucketFolder, parallel, where) {
  # Delete file before downloading it
  command <- paste0("aws s3 rm ", tempFileInS3BucketFolder, " --recursive --only-show-errors")
  message(Sys.time(), " - Deleting old files on S3...")
  tryCatch(system(command), warning = function(w) {})

  # Remove MANIFEST for now.  Create a single file using: PARALLEL FALSE
  tempFileInS3BucketFiles <- paste0(tempFileInS3BucketFolder, "/", "Data_")

  selectClause <- paste0("select * from ", redshiftTable)
  if (!is.null(where)) {
    where <- gsub("'", "\\\\'", where)
    selectClause <- paste0(selectClause, " where ", where)
  }

  query <- paste0("UNLOAD('", selectClause, "')",
                  "TO '", tempFileInS3BucketFiles, "' ",
                  " WITH CREDENTIALS AS 'aws_access_key_id=", Sys.getenv("AWS_ACCESS_KEY_ID"), ";aws_secret_access_key=", Sys.getenv("AWS_SECRET_ACCESS_KEY"), "'
                  DELIMITER AS '|'
                  GZIP
                  ALLOWOVERWRITE
                  PARALLEL ", parallel, ";")
                  # addquotes escape ;

  retVal <- SqlExecute(query, cn=cnRs, logOrigin="DownloadRedshiftTableToS3Private()", logLevelIfFail="FATAL")

  retVal
}

# TODO: make GetTableColumnNames() obsolecent,  (replace with GetSqlQueryColumnNames)
#   Use a common help topic for all the obsolescent functions...
#' @export
GetTableColumnNames <- function(cnRs, redshiftTable, columnNamesFileName) {
  # Lets Get the column names for the table as well
  schemaAndTable <- unlist(strsplit(redshiftTable, "[.]"))
  query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_schema = '",
                  tolower(schemaAndTable[1]), "' AND table_name = '", tolower(schemaAndTable[2]), "' ORDER BY ordinal_position")
  dfColumnNames <- dbGetQuery(cnRs,  query)

  unlink(columnNamesFileName)
  write.table(dfColumnNames$column_name, file = columnNamesFileName, row.names = F, col.names = F, quote = F)
}


#' Get the list of Column Names corresponding to a SQL query
#'
#' \code{GetSqlQueryColumnNames} Returns the names of the columns that a SQL query would produce
#'
#' The primary use of this function is in relation to the "viaS3" query approach, whereby the
#' exported data end up in a header-less text file and the list of the underlying columns is
#' useful for loading this file to a dataframe or table.
#'
#' This function is meant to replace \code{GetTableColumnNames()} which is less generic as it only
#' works for single tables.   \code{GetSqlQueryColumnNames()} works for arbitrary queries where the list
#' of selected columns may be reordered and/or cherry picked and/or the product of joins into multiple tables
#' etc.
#'
#' @param sql character vector : the SQL query.  The elements of this vector are \code{paste}-ed together to produce
#'   the effective query (Allowing for a vector rather than requesting a single string makes it easier
#'   for the calling logic to break the, sometimes long, query over multiple lines in the source code).  Also,
#'   \code{sql} may be made of a single word, which results in an effective query of \code{"SELECT * FROM <this_word>"};
#'   this makes it convenient to fecth whole tables or views.
#' @param cn connection object : a \code{DBIConnection}-derived object such as one returned by \code{GetRedshiftConnection()}.
#'   When \code{NULL}, the function automatically produces such a connection and dispose of it before returning.
#' @param fileName character string : the path + file name where the list of columns should be saved.  When
#'   \code{NULL}, this info is not saved to file and is therefore only available in the return value.
#' @return character vector with the ordered list of the columns produced by the SQL query.
#' @family Redshift functions
#' @export
GetSqlQueryColumnNames <- function(sql, cn=NULL, fileName=NULL) {
  sql <- SqlVectorToString(sql)
  if (sql == "") {
    msg <- "Invalid argument: 'sql' was NULL or empty string"
    DoLog(msg, "WARN", origin="GetSqlQueryColumnNames()")
    return(NULL)
  }

  cnWasNull <- is.null(cn)
  if (cnWasNull) {
    cn <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cn))
      return(NULL)  # (appropriate messages would readilly been issued by GetRedshiftConnection.)
  }

  dfFirstRow <- dbGetQuery(cn, sprintf("SELECT TOP 1 * FROM (%s)", sql))
  retVal <- colnames(dfFirstRow)

  if (cnWasNull) {
    dbDisconnect(cn)
  }

  if (!is.null(fileName)) {
    write.table(retVal, file=fileName, row.names=FALSE, col.names=FALSE, quote=FALSE)
  }

  retVal
}

# TODO: make obsolescent with DownloadRedshiftQuery()
#' @export
DownloadRedshiftTable <- function(redshiftTable, destinationFolder, columnNamesFileName, parallel=FALSE, where=NULL) {
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  retValue <- GetRecordCountPrivate(cnRs, redshiftTable, where)
  if (retValue$count == 0)
    stop("\n0 RECORDS RETURNED using count test: ", retValue$query,
         "\nFix DownloadRedshiftTable function's input parameters and try again.\n")
  else
    message("\n", Sys.time(), " - ", retValue$count, " records found.")

  user <- Sys.getenv("USER")
  if (nchar(user)==0) user <- Sys.getenv("USERNAME")

  # Construct temporary file name based on user
  tempFileInS3BucketFolder <- paste0(GetS3TempFolder(), basename(getwd()), "/temp/", user, "/temp_redshift/", basename(destinationFolder))

  retVal <- DownloadRedshiftTableToS3Private(cnRs, redshiftTable, tempFileInS3BucketFolder, parallel, where)
  if (retVal) {
    DownloadS3FilesForRedshiftPrivate(tempFileInS3BucketFolder, destinationFolder, parallel)
    GetTableColumnNames(cnRs, redshiftTable, columnNamesFileName)
    RemoveFolderFromS3Recursive(tempFileInS3BucketFolder)

    message(Sys.time(), " - DONE.  ", retValue$count, " records saved to: ", destinationFolder, "\n")
  }
  dbDisconnect(cnRs)

  retVal
}

# @@@ Continue here: this the the replacement for DownloadRedshiftTable
#' Download Redshift data to local files
#'
#' \code{DownloadRedshiftQuery} Submit a query to Redshift and download the results to local files
#'
#' This function is useful when one needs to download Redshift data "Once" and process it multiple times, without having
#' to fetch it anew each time on Redshift.  It is also useful to download Redshift data in a way that it is partitioned
#' over muliple local files so that each partition can be processed individually (in paralllel or otherwise).
#'
#' If neither of these use cases apply, the \code{SqlToDf()} function is preferrable since it gets Redshift data
#' directly into a R dataframe, saving the need of explicitly managing local files and importing them to a dataframe.
#' (Depending on the \code{method} argument passed to \code{SqlToDf()}, the data may still transit through S3 and local
#' files, but all that file management is transparent with \code{SqlToDf()})
#'
#' When \code{parallel} is \code{TRUE} -and when the data retrieved is sufficiently volumnous- the data is split over
#' multiple output files.  This expedites [somewhat] the download process and produce partitions of the data suitable
#' for being processed individually (sequentially or in parallel, as the downtream logic may see fit).  Beware, however,
#' that there is relatively little control as to how the partitionning is produced; it is driven by the built-in
#' "partitionning" of the \code{UNLOAD} SQL command in Redshift, i.e. providing no effective control over the size of
#' the partitions nor over possible partitionning criteria.
#'
#' @param sql character vector : the SQL query.  The elements of this vector are \code{paste}-ed together to produce
#'   the effective query (Allowing for a vector rather than requesting a single string makes it easier
#'   for the calling logic to break the, sometimes long, query over multiple lines in the source code).  Also,
#'   \code{sql} may be made of a single word, which results in an effective query of \code{"SELECT * FROM <this_word>"};
#'   this makes it convenient to fecth whole tables or views.
#' @param destinationFolder character string : @@@ See about name...
#' @param columnNamesFileName character string : the path + file name where the list of columns should be saved.  When
#'   \code{NULL}, this info is not saved to file and is therefore only available in the return value.
#' @param parallel logical : whether the data should be split over multiple files. See Details.
#' @param cn connection object : a \code{DBIConnection}-derived object such as one returned by \code{GetRedshiftConnection()}.
#'   When \code{NULL}, the function automatically produces such a connection and dispose of it before returning.
#' @return integer @@@ TBD
#'
#' @seealso \code{\link{SqlToDf}}
#'
#' @family Redshift functions
#' @export
DownloadRedshiftQuery <- function(sql, destinationFolder, columnNamesFileName, parallel=FALSE, cn=NULL) {
  cnWasNull <- is.null(cn)
  if (cnWasNull) {
    cn <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cn))
      return(NULL)  # (appropriate messages would readilly been issued by GetRedshiftConnection.)
  }

  # @@@ WIP :
  # 1. DownloadRedshiftQueryToS3  (sql->S3)
  # 2. DownloadS3FilesForRedshiftPrivate  s3 - > local
  # 3. GetSqlQueryColumnNames()

  if (cnWasNull) {
    dbDisconnect(cn)
  }
}


# replaces DownloadRedshiftTableToS3 and DownloadRedshiftTableToS3Private
# @param s3Folder should _not_ have a trailing "/".
# @param colNamesSuffix when NULL => no export of the colname;  + on the fence about having a boolean instead and use an always-the-same suffix.
#' @export
DownloadRedshiftQueryToS3 <- function(sql, s3Folder, colNamesSuffix=NULL, parallel=FALSE, cn=NULL) {
  logOrig <- "DownloadRedshiftQueryToS3()"

  cnWasNull <- is.null(cn)
  if (cnWasNull) {
    cn <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cn))
      return(NULL)  # (appropriate messages would readilly been issued by GetRedshiftConnection.)
  }

  # @@@ See how to deal with the 0-count
  # remmember to add many timing DBG statements to log

  # @@@ here ADAPT !!!!

  # Delete file before downloading it
  awsCommand <- paste0("aws s3 rm ", s3Folder, " --recursive --only-show-errors")
  DoLog("Deleting old files on S3", "DETAIL", origin=logOrig)

  tryCatch(system(command), warning = function(w) {})

  # Remove MANIFEST for now.  Create a single file using: PARALLEL FALSE
  s3BucketFiles <- paste0(s3Folder, "/", "Data_")

  sql <- SqlVectorToString(sql)
  sql <- sprintf("SELECT * FROM (%s)", sql)   # in case the query contains a LIMIT / SELECT TOP n, as UNLOAD doesn't work for such queries.
  # TODO: the above wrapping of the query could be made conditional to findind TOP/LIMIT keywords

  query <- paste0("UNLOAD('", sql, "')",
                  "TO '", s3BucketFiles, "' ",
                  " WITH CREDENTIALS AS 'aws_access_key_id=", Sys.getenv("AWS_ACCESS_KEY_ID"), ";aws_secret_access_key=", Sys.getenv("AWS_SECRET_ACCESS_KEY"), "'",
                  "DELIMITER AS '|'
                  GZIP
                  ALLOWOVERWRITE
                  PARALLEL ", parallel, ";")
  # @@@ see about addquotes escape

  # dbSendQuery executes but does not return a value so we get an exception.  Instead
  # check the error status.  If it contains "The query was executed" then we are good.
  message(Sys.time(), " - Executing RedShift unload query to S3...")
  retVal <<- TRUE
  tryCatch(dbSendQuery(cnRs, query),
           warning = function(w) {print(w)},
           error   = function(e) {
             msg <- 'The query was executed' # This is the message in the return results we are looking for.
             pos = grep(msg, e$message)
             if (length(pos)!=0) {
               #print(e) # Uncomment for debugging
               message(Sys.time(), " - Query executed SUCCESSFULLY.")
             } else {
               retVal <<- FALSE
               print(e)
             }
           })



  if (cnWasNull) {
    dbDisconnect(cn)
  }
}

#' @export
DownloadRedshiftTableToS3 <- function(redshiftTable, s3Folder, columnNamesFileName, parallel=FALSE, where=NULL) {
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  retValue <- GetRecordCountPrivate(cnRs, redshiftTable, where)
  if (retValue$count == 0)
    stop("\n0 RECORDS RETURNED using count test: ", retValue$query,
         "\nFix DownloadRedshiftTable function's input parameters and try again.\n")
  else
    message("\n", Sys.time(), " - ", retValue$count, " records found.")

  retVal <- DownloadRedshiftTableToS3Private(cnRs, redshiftTable, s3Folder, parallel, where)
  if (retVal) {
    GetTableColumnNames(cnRs, redshiftTable, columnNamesFileName)
    message(Sys.time(), " - DONE.  ", retValue$count, " records saved to: ", s3Folder, "\n")
  }
  dbDisconnect(cnRs)

  retVal
}



#' Execute a SQL statement on Redshift.
#'
#' \code{SqlExecute} Execute arbitrary SQL command(s) on Redshift
#' @param sql character vector : SQL statement to execute.  This is typically a command which
#'    does not return any data (\code{CREATE, INSERT, DELETE} and the like) since
#'    \code{SqlExecute} doesn't fetch any data.  It may contain multiple statements,
#'    separated by a semi-column.  The elements of this vector are \code{paste}-ed together to produce
#'   the effective query (Allowing for a vector rather than requesting a single string makes it easier
#'   for the calling logic to break the, sometimes long, query over multiple lines in the source code).
#'   Note however that the function produces only ONE call to Redshift (the vector argument is meant for readabilty, not for
#'   containing one discrete SQL command per element.)
#' @param cn connection object : a \code{DBIConnection}-derived object such as one returned by \code{GetRedshiftConnection()}.
#'   When \code{NULL}, the function automatically produces such a connection and dispose of it before returning.
#' @param logOrigin character string : the string used as the \code{origin=} argument to the DoLog() calls.  This is used to
#'   identify the function/module which produced the SQL statement, typically this should be the name of the function which is
#'   calling \code{SqlExecute}, although it could point to a broader module.
#' @param logLevelIfOk character string : the LogLevel to use for logging events pertaining to the normal / OK exectution of this
#'   function.  Defaults to \code{NULL} which results in \emph{not} logging any message when all is OK.  Suggested values are
#'   \code{"INFO", "DETAIL", "DBG"} or code{"-"}.
#' @param logLevelIfFail character string : the LogLevel to use for logging error conditions.  Defaults to \code{"WARN"}.
#'   When \code{"WARN"} is used, a warning is issued to the R output (in addition to the Logger); when \code{"FATAL"} the
#'   exectution of the program is interrupted, with a \code{stop()} command. Suggested values are \code{"WARN", "FATAL"} or
#'   lesser levels such as \code{"INFO"} or \code{"-"} if somehow SQL-level are expected.
#' @return logical. \code{TRUE} if the command completed without error,
#'    \code{FALSE} otherwise.  In either case informative messages may be issued to the Log, as required by
#'    the \code{logLevelIfxxx} arguments
#' @examples
#'   \dontrun{SqlExecute("CREATE TABLE myProject_tmp.WordIndex (Word VARCHAR(40) DISTKEY, RefCount INTEGER) SORTKEY(Word)")}
#'
#'   \dontrun{SqlExecute("DELETE myProject_tmp.WordIndex WHERE RefCount < 100")}
#' @family Redshift functions
#' @seealso \code{\link{SqlExecuteFromFile}} which can be more convenient for long SQL commands.
#'     \code{\link{CreateTableInRedShift}} and \code{\link{DropTableInRedShift}} for specific commands.
#' @export
SqlExecute <- function(sql, cn=NULL, logOrigin=NULL, logLevelIfOk=NULL, logLevelIfFail="WARN") {

  sql <- SqlVectorToString(sql)

  if (is.null(logOrigin))
    logOrigin <- "SqlExecute()"

  fatalIfFail <- (!is.null(logLevelIfFail)) && (logLevelIfFail == "FATAL")

  cnWasNull <- is.null(cn)
  if (cnWasNull) {
    cn <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cn)) {
      # Although appropriate messages would readilly been issued by GetRedshiftConnection, we log as requested in the arguments.
      if (!is.null(logLevelIfFail))
        DoLog("Cannot obtain a connection to Redshift.", logLevelIfFail, origin=logOrigin)
      if (fatalIfFail)
        stop("FATAL ERROR: Could not obtain a connection to Redshift")
      return(FALSE)
    }
  }

  # dbSendQuery() systematically produces an exception, even when all is OK.
  # The way we assert that all is well is by verifying that the exception message contains
  # the text "The query was executed".
  retVal <- TRUE
  errMsg <- NULL
  tryCatch(dbSendQuery(cn, sql),
           warning = function(w) {
             if (!is.null(logLevelIfFail) || !is.null(logLevelIfOk)) {
               DoLog(sprintf("dbSendQuery() resulted in warning:", w), "WARN", logOrigin)
             }
           },
           error   = function(e) {
             if (str_detect(e$message, "The query was executed")) {
               if (!is.null(logLevelIfOk)) {
                 DoLog(sprintf("Successful SQL command: %s", sql), logLevelIfOk, logOrigin)
               }
             } else {
               # The SQL command failed!
               retVal <<- FALSE
               errMsg <<- e$message
               if (!is.null(logLevelIfFail))
                 DoLog(sprintf("Error while running SQL command %s.  Redshift error= %s", sql, e),
                       logLevelIfFail, origin=logOrigin)

             }
           }
  )

  if (cnWasNull) {
    dbDisconnect(cn)
  }

  if (!retVal && fatalIfFail)
      stop(errMsg)

  retVal
}

#' Execute a SQL statement on Redshift.
#'
#' \code{SqlExecute} Execute arbitrary SQL command(s) on Redshift
#' @param fileName The name of the file containing the SQL statement to execute.  This is typically a command which
#'    does not return any data (\code{CREATE, INSERT, DELETE} and the like) since
#'    \code{SqlExecute} doesn't fetch any data. It may contain multiple statements,
#'    separated by a semi-column.
#' @return logical. \code{TRUE} if the command completed without error,
#'    \code{FALSE} otherwise.  In case of error an informative message is sent
#'    to the console.
#' @examples
#'   \dontrun{SqlExecuteFromFile("ZipCodesOfInterest.sql")}
#' @family Redshift functions
#' @seealso \code{\link{SqlExecute}}, \code{\link{CreateTableInRedShift}} and \code{\link{DropTableInRedShift}}
#' @export
SqlExecuteFromFile <- function(fileName) {
  lines <- readLines(fileName)
  query <- paste(lines, collapse = '')
  SqlExecute(query)
}

# Drop all the records from the specified table
TruncateRedshiftTablePrivate <- function(cnRs, redshiftTable) {
  # Note TRUNCATE TABLE does not work if user is not the owner
  query <- paste0("DELETE FROM  ", redshiftTable, ";")

  retVal <- SqlExecute(query, cnRs)
  retVal
}


#' Empty a Table in Redshift.
#'
#' \code{TruncateRedshiftTable} Empty a Table in Redshift  (aka SQL TRUNCATE)
#' @param redshiftTable Name of table to truncate. This should include the name of the Schema.
#' @return \code{TRUE} if the truncation was successful, \code{FALSE} otherwise.  When
#'    successful, a confirmation message is sent to the console.
#' @examples
#' TruncateRedshiftTable("codezero_temp.ValidationScores")
#' # The ValidationScores table is now completely empty.
#' @family Redshift functions.
#' @export
TruncateRedshiftTable <- function(redshiftTable) {
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  retVal <- TruncateRedshiftTablePrivate(cnRs, redshiftTable)
  if (retVal)
    message(Sys.time(), " - Table truncated successfully.")
  dbDisconnect(cnRs)

  retVal
}

# Function to upload an S3 file to redshift.  Its faster to load via S3
UploadS3ToRedshiftTable <- function(cnRs, s3FileName, redshiftTable) {
  query <- paste0("COPY ", redshiftTable,
                  " FROM '", s3FileName, "' ",
                  " WITH CREDENTIALS AS 'aws_access_key_id=", Sys.getenv("AWS_ACCESS_KEY_ID"), ";aws_secret_access_key=", Sys.getenv("AWS_SECRET_ACCESS_KEY"), "'
                  DELIMITER AS '|'
                  ;")
  #  GZIP

  # dbSendQuery executes but does not return a value so we get an exception.  Instead
  # check the error status.  If it contains "The query was executed" then we are good.
  message(Sys.time(), " - Executing RedShift upload query from S3...")
  retVal <- SqlExecute(query, cnRs)
  if (retVal)
    message(Sys.time(), " - Query executed SUCCESSFULLY.")

  retVal
}

#' @export
UploadFileToRedshift <- function(dataFileName, redshiftTable, truncate) {
  user <- Sys.getenv("USER")
  if (nchar(user)==0) user <- Sys.getenv("USERNAME")

  # Construct temporary S3 file name based on user
  s3FileName <- paste0(GetS3TempFolder(), basename(getwd()), "/temp/", user, "/", basename(dataFileName))

  retVal <- FALSE
  errorCode <- UploadFileToS3(dataFileName, s3FileName)
  if (errorCode==0) {
    cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (truncate)
      TruncateRedshiftTablePrivate(cnRs, redshiftTable)

    retVal <- UploadS3ToRedshiftTable(cnRs, s3FileName, redshiftTable)
    if (retVal) message(Sys.time(), " - DONE.  Data loaded to table: ", redshiftTable)
    dbDisconnect(cnRs)
  }

  RemoveFileFromS3(s3FileName)

  retVal
}



#' Upload R data to a Redshift table.
#'
#' \code{UploadTableToRedshift} Upload R data (data.frame, data.table, matrix) to an existing table in Redshift.
#' @param dataTable a data.frame (or data.table or matrix) with the data to be uploaded
#' @param redshiftTable the name of the Redshift table where the data is to be copied.  The
#'    name should include the Schema. The table must exists in Redshift (\code{UploadTableToRedshift} does
#'    not create tables in Redshift).
#' @param truncate a logical value. If \code{TRUE}, the table is first emptied before
#'    receiving \code{dataTable}'s data, hence completely overwritting data previously found in there;
#'    if \code{FALSE}, \code{dataTable}'s records are appended to whatever existing content the
#'    table holds.
#' @examples
#' # *** setup ***
#' # configure AWS access (Redshift, S3 etc.)
#' ConfigureAWS("Config/config.json")
#' # create empty table
#' CreateTableInRedShift("codezero_temp.funWithCars", mtcars, TRUE)
#' #  2016-10-20 23:29:18 - Creating table: codezero_temp.funWithCars. Please wait...
#' #  [1] TRUE
#'
#' UploadTableToRedshift(mtcars, "codezero_temp.funWithCars", TRUE)
#' #  2016-10-20 23:33:09 - Writing: 32 records to temp file...
#' #  2016-10-20 23:33:09 - Uploading file to S3...
#' #  2016-10-20 23:33:13 - Executing RedShift upload query from S3...
#' #  2016-10-20 23:33:20 - Query executed SUCCESSFULLY.
#' #  2016-10-20 23:33:20 - DONE.  Data loaded to table: codezero_temp.funWithCars
#' #  [1] TRUE
#'
#' # *** Clean-up ***
#' DropTableInRedShift("codezero_temp.funWithCars")
#' #  [1] TRUE
#'
#' @family Redshift functions
#' @export
UploadTableToRedshift <- function(dataTable, redshiftTable, truncate=FALSE) {
  # Do NOT use tempdir() for the temporary directory.  tempdir() works well for main rsession.  However,
  # when running multiple threads using the doParallel library, each thread gets its own tempdir() which
  # in some cases does not exist.  One option is to create the tmpdir if it does not exist or just create
  # a temp dir in the data folder.
  dataFileName <- tempfile(pattern = "temp_redshift", tmpdir = GetTempFolder(basename(getwd())), fileext = ".csv")

  message("\n", Sys.time(), " - Writing: ", nrow(dataTable), " records to temp file...")
  write.table(dataTable, file=dataFileName, row.names=FALSE, col.names=FALSE, quote=FALSE, sep="|", na="")
  UploadFileToRedshift(dataFileName, redshiftTable, truncate)
  file.remove(dataFileName)
}

DropTableInRedShiftPrivate <- function(cnRs, redshiftTable) {
  query <- paste0("DROP TABLE IF EXISTS ", redshiftTable)
  SqlExecute(query, cnRs)
}

#' Drop a Table in Redshift.
#'
#' Drop (delete permanently) a SQL table in Redshift
#' @param redshiftTable the name of the SQL table to be dropped. Should include the Schema name.
#' @examples
#' \dontrun{DropTableInRedshift("Inventory.SummerCatalogItems")}
#' @family Redshift functions
#' @export
DropTableInRedShift <- function(redshiftTable) {
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  DropTableInRedShiftPrivate(cnRs, redshiftTable)
  dbDisconnect(cnRs)
}

#' Create a Table in Redshift.
#'
#' \code{CreateTableInRedShift} Create a new SQL table in Redshift.  The new table is
#'    templated on a dataframe supplied as argument.
#' @param redshiftTable the name of the table to create. This should include the Schema.
#' @param df a data.frame used used to define the field names and type of the new table.
#'   This dataframe cannot be empty, it needs to have at least one row. Although the
#'   table is based on this dataframe, it is left empty; it is \emph{not} loaded with the
#'   data from the dataframe.
#' @param recreate a boolean indicating whether the table should be dropped (if it exists).
#'   When the table exists and this parameter is \code{FALSE}, the existing table and the
#'   \code{df} argument must have the same variables names and types.
#' @examples
#'   \dontrun{CreateTableInRedShift("myProject_temp.OctoberSales", dfSales, TRUE)}
#' @family Redshift functions
#' @export
CreateTableInRedShift <- function(redshiftTable, df, recreate=FALSE) {
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  if (recreate)
    DropTableInRedShiftPrivate(cnRs, redshiftTable)

  # Use first 5 rows to create the table.  Using the full table takes a really long time.
  df1Row <- head(df, 5)

  message(Sys.time(), " - Creating table: ", redshiftTable, ". Please wait...")
  dbWriteTable(cnRs, redshiftTable, df1Row)
  TruncateRedshiftTablePrivate(cnRs, redshiftTable)

  dbDisconnect(cnRs)
}

#' @export
LoadRedshiftTableToDataFrame <- function(workingDirBaseName, redshiftTable, whereClause=NULL) {
  destinationFolder   <- ComposeLocalFilePath(redshiftTable, workingDirBaseName)
  columnNamesFileName <- ComposeColumnNamesFileName(redshiftTable, workingDirBaseName)
  retVal              <- DownloadRedshiftTable(redshiftTable, destinationFolder, columnNamesFileName,
                                               parallel=FALSE, where=whereClause)

  # Load the actual data.  Note this data is extracted in bulk without the meta data
  dataFileName <- ComposeDataFileName(redshiftTable, workingDirBaseName)
  dataframe <- read.table(dataFileName, header=FALSE, quote="\"", sep="|",
                          comment.char='', stringsAsFactors=FALSE)

  # Configure the meta data
  columnNames     <- read.table(columnNamesFileName)
  names(dataframe) <- as.character(columnNames$V1)

  dataframe
}


# Converts a character vector with multiple parts of the SQL query to one long string.
# This function also converts "single word" queries to "SELECT * FROM <said_word>"
#
# Several functions receive a SQL statement optionally split over muiltiple "lines" and passed
# as a character vector; this format is very convenient as it allows the queries to be nicely
# laid-out, in a more readable form, in the source code.   This functions simply
# collapse this format into a single string suitable for being passed to dbGetQuery() and the like.
SqlVectorToString <- function(sql) {
  retVal <- paste(sql, collapse=" ")

  # Is the query a single word (optionally with leading and/or trailing whitespace) ?
  if (str_detect(retVal, "^\\s*[A-Za-z0-9._]+\\s*$"))
    retVal <- paste("SELECT * FROM", sql)

  retVal
}



# ******** Old stuff *********

# This private method was made obsolute by SqlExecute().   All 4 calls to the old API were replaced
# SqlExecutePrivate <- function(cnRs, query) {
#   retVal <<- TRUE
#   tryCatch(dbSendQuery(cnRs, query),
#            warning = function(w) {print(w)},
#            error   = function(e) {
#              pos = grep("The query was executed", e$message)
#              if (length(pos) == 0) {
#                retVal <<- FALSE
#                print(e)
#              }
#            })
#
#   retVal
# }
