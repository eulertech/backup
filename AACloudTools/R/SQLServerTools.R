# SQLServerTools.R
#
#   This source file is part of Advanced Analytics AWS Cloud Tools (c) IHS 2016
#
#   Author:  Christopher Lewis
#
# UNDER CONSTRUCTIONS

GetSqlServerConnectionPrivate <- function() {
  driverPath <- system.file("jars", "sqljdbc41.jar", package = "AACloudTools")
  driver <- JDBC(Sys.getenv("com.microsoft.sqlserver.jdbc.SQLServerDriver"), driverPath)

  server <- 'jdbc:sqlserver://10.45.88.171'
  dbName    <- 'IDDS03'
  userName  <- 'tugboat'
  password  <- 'trial,246'
  url <- paste0(server, ';databaseName=', dbName)
  cnRs <- dbConnect(driver, url, userName, password)

  cnRs
}

CloseSqlServerConnectionPrivate <- function(cnRs) {
  dbDisconnect(cnRs)
}

#' @export
MSSqlExecute <- function(query) {
  cnRs <- GetSqlServerConnectionPrivate()
  results <- dbGetQuery(cnRs,  query)
  CloseSqlServerConnectionPrivate(cnRs)

  results
}
