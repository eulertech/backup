## RedshiftBackupRestore.R
##   Functions used to Backup and Restore whole tables to/from Redshift/S3
##
##   Aside from formalizing some backup process, the purpose behind these
##   functions is to enable easy "shuttling" of data tables between their
##   "live" form in Redshift and their "on ice" form in S3.
##
## This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2017
##
## Author: Marc Veillet 11/22/2016
##


#' Backup a table in Redshift to S3
#'
#' \code{BackupReshiftTableToS3} Backup a table to S3
#'
#'
#'
#' @param schema character string : the name of the Redshift database schema where the table is to be found
#' @param table character string : the name of the Redshift table
#' @param sortOrder character string: the optional \code{ORDER BY} clause for exporting the data, without the
#'   'ORDER BY' keyword.
#' @param parallel logical : when \code{FALSE} the export is performed sequentially, which is somewhat slower but
#'   producing the completely ordered output, whereas \code{TRUE} produces more files in a less precise order; it also
#'   tends to produces more files.  Use NULL to let the function decide, based on the table size (parallel output for
#'   bigger table, sequential for smaller ones.)
#' @param cnRs connection object : a \code{DBIConnection}-derived object such as one returned by \code{GetRedshiftConnection()}.
#'   When \code{NULL}, the function automatically produces such a connection and dispose of it before returning.
#' @param verbose logical : Should informational text messages describing the backup process be issued to the console?
#' @export
BackupReshiftTableToS3 <- function(schema, table, sortOrder = NULL, parallel = NULL, cnRs = NULL, verbose = FALSE) {
  #implementation note: copied in great parts from DownloadRedshiftQueryToS3()  ==> Look to share code etc.

  DATALAKE_BACKUP_S3_BUCKET <- "s3://ihs-bda-redshift-backup/"
  DEFAULT_REDSHIFT_DATABASE <- "lake01"

  logOrig <- "BackupReshiftTableToS3()"

  awsAccessKey <- Sys.getenv("AWS_ACCESS_KEY_ID")
  awsSecretKey <- Sys.getenv("AWS_SECRET_ACCESS_KEY")

  if (awsAccessKey == "" || awsSecretKey == "") {
    stop("AWS access keys are not available in environmental variables; Have you forgotten to call ConfureAWS() ?")
  }

  if (is.null(schema) || schema == "" || is.null(table) || table == "") {
    stop("Invalid Arguments: 'schema' and 'table' must be a non-empty character string.")
  }

  s3Folder <- paste0(DATALAKE_BACKUP_S3_BUCKET, DEFAULT_REDSHIFT_DATABASE, "/", schema, "/", table)

  cnWasNull <- is.null(cnRs)
  if (cnWasNull) {
    cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cnRs)) {
      stop("FATAL: Could not obtain a connection to Redshift !")
    }
  }

  if (is.null(parallel)) {
    # TODO : get the table size etc. and decide on parallel vs. sequential export accoridingly
    # Tables under 20 odd gig should probably be better handled sequentially
    #    SELECT diststyle, sortkey1, size, tbl_rows FROM SVV_TABLE_INFO WHERE "table" = 'absd_ship_search__bak' AND schema = 'ra'
    parallel <- FALSE
  }

  # Delete all files from a previous backup
  # TODO: introduce feature to "move" the old backup to a "backup - 1" folder
  if (verbose)
    message("Deleting previous backup (if any) at ", s3Folder)
  awsCommand <- paste0("aws s3 rm ", s3Folder, " --recursive --only-show-errors")
  tryCatch(system(awsCommand),
           warning = function(w) {},   # @@@ not sure why warning are hidden
           error   = function(e) {
             message(e)
             stop("FATAL error: could not prepare s3 bucket for new backup; aborting.")
           }
  )

  s3BucketFiles <- paste0(s3Folder, "/", "Data_")

  if (!is.null(sortOrder) && sortOrder != "")
    sortOrder <- paste0(" ORDER BY ", sortOrder)
  sql <- paste0("SELECT * FROM ", schema, ".", table, sortOrder)

  query <- paste0("UNLOAD('", sql, "') ",
                  "TO '", s3BucketFiles, "' ",
                  "WITH CREDENTIALS AS 'aws_access_key_id=", awsAccessKey, ";aws_secret_access_key=", awsSecretKey, "' ",
                  "MANIFEST ",
                  "DELIMITER AS '|' ",
                  "ESCAPE ",
                  "GZIP ",
                  "ALLOWOVERWRITE ",
                  "PARALLEL ", parallel, ";")

  # @@@ see about ENCRYPTED
  # @@@ also see behavior with nulls (which may come back as "" in the re-loaded table)

  if (verbose) {
    message("SQL query = ", sql)
    message(Sys.time(), " - Starting the UNLOAD command...")
  }


  # dbSendQuery executes but does not return a value so we get an exception.  Instead
  # check the error status.  If it contains "The query was executed" then we are good.
  retVal <- s3Folder
  tryCatch(dbSendQuery(cnRs, query),
           warning = function(w) {print(w)},
           error   = function(e) {
             msg <- 'The query was executed' # This is the message in the return results we are looking for.
             pos = grep(msg, e$message)
             if (length(pos)!=0) {
               if (verbose) {
                 message(Sys.time(), " - UNLOAD executed SUCCESSFULLY.")
               }
             } else {
               retVal <<- NULL
               print(e)
             }
           }
  )

  if (cnWasNull) {
    dbDisconnect(cnRs)
  }

  return(retVal)
}

# for memo: each 6.3Gib file has ~ 103,244,356 records.
# hence for tblAllAisFiles' 3,364,928,359 recs, we need ~ 33 files,
# it took ~ 7h20' for the first 17, hence ~ 15 hours for the whole download !!!
# Also this tok ~ 45% of the cluster's CPU...
# next : try with // on

RestoreReshiftTableFromS3 <- function(schema, table, restoreToTable = NULL,  cnRs = NULL, verbose = FALSE) {
  DATALAKE_BACKUP_S3_BUCKET <- "s3://ihs-bda-redshift-backup/"
  DEFAULT_REDSHIFT_DATABASE <- "lake01"

  logOrig <- "RestoreReshiftTableFromS3()"

  awsAccessKey <- Sys.getenv("AWS_ACCESS_KEY_ID")
  awsSecretKey <- Sys.getenv("AWS_SECRET_ACCESS_KEY")

  if (awsAccessKey == "" || awsSecretKey == "") {
    stop("AWS access keys are not available in environmental variables; Have you forgotten to call ConfureAWS() ?")
  }

  if (is.null(schema) || schema == "" || is.null(table) || table == "") {
    stop("Invalid Arguments: 'schema' and 'table' must be a non-empty character string.")
  }

  if (is.null(restoreToTable) || restoreToTable == "")
    restoreToTable <- paste0(schema, ".", table)

  s3Folder <- paste0(DATALAKE_BACKUP_S3_BUCKET, DEFAULT_REDSHIFT_DATABASE, "/", schema, "/", table)

  # TODO: see about using the manifest, instead
  s3BucketFiles <- paste0(s3Folder, "/", "Data_")

  # dbgTime:
  s3BucketFiles <- paste0(s3Folder, "/", "Data_000.gz")

  cnWasNull <- is.null(cnRs)
  if (cnWasNull) {
    cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
    if (is.null(cnRs)) {
      stop("FATAL: Could not obtain a connection to Redshift !")
    }
  }

  query <- paste0("COPY ", restoreToTable, " ",
                  "FROM '", s3BucketFiles, "' ",
                  "WITH CREDENTIALS AS 'aws_access_key_id=", awsAccessKey, ";aws_secret_access_key=", awsSecretKey, "' ",
                  "DELIMITER AS '|' ",
                  "ESCAPE ",
                  "GZIP ",
                  # BLANKSASNULL / EMPTYASNULL  / NULL AS 'null_string' tbd
                  "MAXERROR 5 ",  # @@@ debug time...
                  ";")

  tryCatch(dbSendQuery(cnRs, query),
           warning = function(w) {print(w)},
                  error   = function(e) {
                    msg <- 'The query was executed' # This is the message in the return results we are looking for.
                    pos = grep(msg, e$message)
                    if (length(pos)!=0) {
                      if (verbose) {
                        message(Sys.time(), " - COPY executed SUCCESSFULLY.")
                      }
                    } else {
                      retVal <<- NULL
                      print(e)
                    }
                  }
  )

  if (cnWasNull) {
    dbDisconnect(cnRs)
  }

  return(retVal)
}

