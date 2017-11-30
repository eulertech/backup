context("RedshiftTools functions")


#  Do_ConfigureAWS_forTestContext() IS NOW OBSOLETE, because
#      AACloudTools::ConfigureAWS now handles the "walking up" the directory tree
#      to locate the Configuration folder.
#  We leave the function here for documentary purposes.
#
# Call ConfigureAWS() with an added trick to deal with testthat's idiosyncrasis about
# the current working directory
Do_ConfigureAWS_forTestContext <- function() {
  # Work-around the fact that when run under devtools::test(), the working directory is
  # the testthat directory, not the home directory of the project.
  # Because the config file has references to other files, in a relative path, it is
  # easier if not necessary to cheat temporarilly on the WD.
  testthat_wd <- getwd()
  setwd("../..")
  ConfigureAWS(readLines("Config/config.json"))
  setwd(testthat_wd)
}

test_that("GetRedshiftConnection() works", {

  ConfigureAWS("Config/config.json")    # formerly:   Do_ConfigureAWS_forTestContext()

  cat("\n----- The following java.* exception are expected !  -----")
  expect_null(GetRedshiftConnection("bad_url"))
  cat("----- end of the java.* error messages -----\n")

  cnRs <- GetRedshiftConnection()
  expect_is(cnRs, "JDBCConnection")

  # This code is commented out because it was replaced with the round-trip test below.
  # myDf <- dbGetQuery(cnRs, "SELECT TOP 20 * from ra.ABSD_SHIP_SEARCH")
  # expect_equal(nrow(myDf), 20)
  # expect_gt(ncol(myDf), 50)   # actual is ~ 92 cols
  # expect_true(sum(c("lrno", "vesselname") %in% colnames(myDf)) == 2)

  # Round-trip test
  testTableName <- "codezero_temp.RSTest"
  # 1. Create small data frame with random numbers
  originDf <- data.frame(col1=rnorm(3), col2=rnorm(3), col3=rnorm(3), col4=rnorm(3))
  # 2. Create corresponding table in Redshift, and write data to the table
  retCd <- dbWriteTable(cnRs, testTableName, originDf)
  # 3. Read table back from Redshift and perform tests
  resultDf <- dbGetQuery(cnRs, paste0("SELECT * FROM ", testTableName))
  expect_equal(nrow(resultDf), nrow(originDf))
  expect_equal(ncol(resultDf), ncol(originDf))
  expect_equal(colnames(resultDf), tolower(colnames(originDf)))
  # 4. Drop table in Redshift
  retCd <- dbSendUpdate(cnRs, sprintf("DROP TABLE %s", testTableName))

  wrk <- dbDisconnect(cnRs)
})

test_that("SqlToDf() works", {
  ConfigureAWS("Config/config.json")

  sql <- c("SELECT TOP 25 * FROM ra.ABSD_SHIP_SEARCH",
           "WHERE flag = 'USA'")

  folder <-paste0("Tmp", str_sub(str_replace_all(uuid::UUIDgenerate(), "-", ""), 1L, 12L))

  wrk <- GetS3TempUrl(fileName="data.csv", subFolder=folder, user=NULL)

  #wrk <- SqlToDf(sql)
  # TODO: Complete the test_that test for SqlToDf()
})

test_that("GetSqlQueryColumnNames() works", {
  sql <- c("(SELECT 21 AS Bozo, lrno, flag",
           "FROM ra.ABSD_SHIP_SEARCH)")
  columns <- GetSqlQueryColumnNames(sql)
  expect_equal(columns, c("bozo", "lrno", "flag"))

  theFile <- "test_GetSqlQueryColumnNames.txt"

  cnRs <- GetRedshiftConnection()
  columns <- GetSqlQueryColumnNames("ra.ABSD_SHIP_SEARCH", cnRs, theFile)
  expect_gt(length(columns), 50)  # this table has in excess of 90 rows
  colsFromFile <- read.table(theFile, stringsAsFactors=FALSE)$V1
  expect_equal(columns, colsFromFile)
  unlink(theFile)
})

test_that("SqlExecute() works", {

  # BAS SQL statement, no logging
  cc <- SqlExecute("Bogus Syntax", logLevelIfFail=NULL);
  expect_false(cc)

  # Syntactically Valid statement but reference to inexistant table;  Fatal
  sql <- "DROP TABLE myBogusSchema.myPhantomTable"
  expect_error(SqlExecute(sql, logLevelIfFail="FATAL"))
  cc <- SqlExecute(sql, logLevelIfFail="WARN")
  expect_false(cc)

  # Valid and expectedly successful statements  also with our provided connection
  cnRs <- GetRedshiftConnection(Sys.getenv("PGURLRS"))
  testTblName <- "spectre.Tmp_SqlExectute_Test_9848"
  sql <- c(sprintf("DROP TABLE IF EXISTS %s;", testTblName),
           sprintf("CREATE TABLE %s (", testTblName),
           "ZeId       INT IDENTITY(1,1)            ENCODE RAW NOT NULL,",
           "AlphaText  VARCHAR(40)                  ENCODE RAW NOT NULL,",
           "ZuluDate   TIMESTAMP DEFAULT GETDATE()  ENCODE RAW",
           ")",
           "DISTSTYLE ALL"
          )
  cc <- SqlExecute(sql, cn=cnRs)
  expect_true(cc)
  sql <- c(sprintf("INSERT INTO %s", testTblName),
           "(AlphaText) VALUES ('Hello ! Wassup?')"
          )
  cc <- SqlExecute(sql, cn=cnRs)
  expect_true(cc)

  sql <- sprintf("DROP TABLE %s", testTblName)
  cc <- SqlExecute(sql, cn=cnRs)
  expect_true(cc)
  # but tis one should fail, since the table doesn't exist anymore
  cc <- SqlExecute(sql, cn=cnRs)
  expect_false(cc)

  dbDisconnect(cnRs)

  # Generally good enough!  The only feature not tested is the logging
})
