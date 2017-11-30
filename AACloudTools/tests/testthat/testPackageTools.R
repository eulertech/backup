context("PackageTools (and _BuildSupport and _Common) functions")


# Gotta source this as the software in the "Tools" folder is not,
# technically, part of the package per se.  (not in the "R" folder)
source("../../Tools/PackageTools_Commons.R")

test_that("ParsePackageList() works", {
  # Assert elegant handling of invalid input values
  expect_error(ParsePackageList(NULL), "Invalid")
  expect_error(ParsePackageList(""), "Invalid")
  expect_error(ParsePackageList(c("AACloudTools", "~Dev|AASpectre", "Bozo|")), "Invalid")  # empty/missing PackageName

  df <- ParsePackageList(c("AACloudTools", "~Dev|AASpectre", "Bozo|GLM"))
  expect_equal(nrow(df), 3)
  expect_equal(ncol(df), 2)
  expect_equal(colnames(df), c("SubLocation", "PackageName"))
  expect_equal(df[2, "SubLocation"], "~Dev")

  # also check that it works with only 1 package
  df <- ParsePackageList("AACloudTools")
  expect_equal(df, data.frame(SubLocation="", PackageName="AACloudTools", stringsAsFactors=FALSE))

})
