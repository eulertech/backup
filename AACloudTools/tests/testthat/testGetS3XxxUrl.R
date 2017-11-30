context("GetS3xxxUrl functions")

# library(stringr)

test_that("GetS3ModelUrl() works", {
  # All GetS3ModelUrl-created URLs should start with the following.
  # This is the s3 bucket which is open to the temp user.
  s3UrlStart <- "s3://ihs-bda-data/projects/"

  url <- GetS3ModelUrl()
  expect_match(url, paste0("^", s3UrlStart, ".+/Models/$"))

  url <- GetS3ModelUrl("myProj", "SummerSales.RData")
  expect_equal(url, paste0(s3UrlStart, "myProj/Models/SummerSales.RData"))

  url <- GetS3ModelUrl("myProj", NULL, "Reference/2016")
  expect_equal(url, paste0(s3UrlStart, "myProj/Models/Reference/2016/"))
})

test_that("GetS3ModelUrl() handles arguments with leading/trailing slashes and spaces", {
  url <- GetS3ModelUrl("myProj", "  July.RData", "IceCream/Strawberry/")
  expect_match(url, "s3://.+/myProj/Models/IceCream/Strawberry/July.RData")

  url <- GetS3ModelUrl(NULL, "August.RData", "/IceCream/Lemon")
  expect_match(url, "s3://.+/Models/IceCream/Lemon/August.RData")
})

test_that("GetS3ScoringUrl() works", {
  # note since GetS3ScoringUrl() and GetS3ModelUrl() are merely a wrapper around the same
  # private function, a limited set of test is sufficient since GetS3ModelUrl() was tested more extensively.
  s3UrlStart <- "s3://ihs-bda-data/projects/"

  url <- GetS3ScoringUrl()
  expect_match(url, paste0("^", s3UrlStart, ".+/Scoring/$"))

  url <- GetS3ScoringUrl("myProj", "Results.csv")
  expect_equal(url, paste0(s3UrlStart, "myProj/Scoring/Results.csv"))

  url <- GetS3ScoringUrl("myProj", NULL, "Europe")
  expect_equal(url, paste0(s3UrlStart, "myProj/Scoring/Europe/"))
})
