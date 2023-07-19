json_resources_dir <- function() {
  test_path("data", "resources", "R4", "json")
}

bundles_test_data_dir <- function() {
  test_path("data", "bundles", "R4", "json")
}

parquet_test_data_dir <- function() {
  test_path("data", "parquet")
}

ndjson_test_data_dir <- json_resources_dir


delta_test_data_dir <- function() {
  test_path("data", "delta")
}


temp_ndjson_dir <- function() {
  file.path(tempdir(), "ndjson")
}

temp_parquet_dir <- function() {
  file.path(tempdir(), "parquet")
}

ndjson_query <- function(data_source) {
  data_source %>% ds_aggregate(
      "Patient",
      aggregations = list(count = "reverseResolve(Condition.subject).count()")
  )
}

bundles_query <- function(data_source) {
  data_source %>% ds_aggregate(
      "Patient",
      aggregations = list(count = "count()")
  )
}

parquet_query <- ndjson_query
delta_query <- ndjson_query

test_that("datasource read ndjson", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  patients <- read_ndjson(pc, ndjson_test_data_dir()) %>% ds_read("Patient")
  expect_equal(patients %>% sdf_nrow(), 9)
})

test_that("datasource ndjson", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  ds <- read_ndjson(pc, ndjson_test_data_dir())

  # TODO: Uncomment when writing is fixed
  # ds_write_ndjson(ds, temp_ndjson_dir())
  # data_source <- read_ndjson(pc, temp_ndjson_dir())
  data_source <- ds

  result <- ndjson_query(data_source)
  expect_equal(names(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

# TODO: Uncomment when file_name_mapper is implemented
#
# test_that("datasource ndjson mapper", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   ds <- read_ndjson(pc, ndjson_test_data_dir())
#   write_ndjson(ds, temp_ndjson_dir(), file_name_mapper = function(x) paste0("Custom", x))
#   data_source <- read_ndjson(pc, temp_ndjson_dir(), file_name_mapper = function(x) str_replace(x, "\\^Custom", ""))
#
#   result <- ndjson_query(data_source)
#   expect_equal(names(result), "count")
#   expect_equal(collect(result), tibble::tibble(count = 71))
# })

test_that("datasource bundles", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  data_source <- read_bundles(pc, bundles_test_data_dir(), c("Patient", "Condition"))

  result <- bundles_query(data_source)
  expect_equal(names(result), "count")
  # TODO: (should be 10): Unify the test data
  expect_equal(collect(result), tibble::tibble(count = 5))
})

test_that("datasource datasets", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  data_source <- read_datasets(pc, list(
      "Patient" = ptl_encode(pc, spark_read_text(spark, file.path(ndjson_test_data_dir(), "Patient.ndjson")), "Patient"),
      "Condition" = ptl_encode(pc, spark_read_text(spark, file.path(ndjson_test_data_dir(), "Condition.ndjson")), "Condition")
  ))

  result <- ndjson_query(data_source)
  expect_equal(names(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource parquet", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  ds <- read_parquet(pc, parquet_test_data_dir())

  # TODO: Uncomment when writing is fixed
  # ds %>% ds_write_parquet(temp_parquet_dir())
  # data_source <- read_parquet(pc, temp_parquet_dir())
  data_source <- ds

  result <- parquet_query(data_source)
  expect_equal(names(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

# TODO: Uncomment when delta is fixed
# test_that("datasource delta", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   # TODO: Uncomment when writing is fixed
#   # read_delta(pc, delta_test_data_dir()) %>% write_delta(temp_delta_dir())
#   data_source <- read_delta(pc, delta_test_data_dir())
#
#   result <- delta_query(data_source)
#   expect_equal(names(result), "count")
#   expect_equal(collect(result), tibble::tibble(count = 71))
# })
#
# test_that("datasource delta merge", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   # TODO: Uncomment when writing is fixed
#   # read_delta(pc, delta_test_data_dir()) %>% write_delta(temp_delta_dir(), import_mode = "merge")
#   # data_source <- read_delta(pc, temp_delta_dir())
#   data_source <- read_delta(pc, delta_test_data_dir())
#
#   result <- delta_query(data_source)
#   expect_equal(names(result), "count")
#   expect_equal(collect(result), tibble::tibble(count = 71))
# })

# TODO: Uncomment when writing is fixed
# test_that("datasource tables", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   read_ndjson(pc, ndjson_test_data_dir()) %>% write_tables()
#
#   data_source <- read_tables(pc)
#   result <- ndjson_query(data_source)
#   expect_equal(names(result), "count")
#   expect_equal(collect(result), tibble::tibble(count = 71))
# })
#
# test_that("datasource tables schema", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   read_ndjson(pc, ndjson_test_data_dir()) %>% write_tables(schema = "test")
#
#   data_source <- read_tables(pc, schema = "test")
#   result <- ndjson_query(data_source)
#   expect_equal(names(result), "count")
#   expect_equal(collect(result), tibble::tibble(count = 71))
# })
