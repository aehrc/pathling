ndjson_test_data_dir <- function() {
  test_path("testdata", "ndjson")
}

bundles_test_data_dir <- function() {
  test_path("testdata", "bundles")
}

parquet_test_data_dir <- function() {
  test_path("testdata", "parquet")
}

delta_test_data_dir <- function() {
  test_path("testdata", "delta")
}


temp_ndjson_dir <- function() {
  file.path(tempdir(), "ndjson")
}

temp_parquet_dir <- function() {
  file.path(tempdir(), "parquet")
}

temp_delta_dir <- function() {
  file.path(tempdir(), "delta")
}


ndjson_query <- function(data_source) {
  data_source %>%
      ds_view(
          "Condition",
          select = list(
              list(
                  column = list(
                      list(
                          path = "id",
                          name = "id"
                      )
                  )
              )
          )
      ) %>%
      count(name = "count")
}

bundles_query <- function(data_source) {
  data_source %>%
      ds_view(
          "Patient",
          select = list(
              list(
                  column = list(
                      list(
                          path = "id",
                          name = "id"
                      )
                  )
              )
          )
      ) %>%
      count(name = "count")
}

parquet_query <- ndjson_query
delta_query <- ndjson_query

test_that("datasource read ndjson", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  patients <- pathling_read_ndjson(pc, ndjson_test_data_dir()) %>% ds_read("Patient")
  expect_equal(patients %>% sdf_nrow(), 9)
})

test_that("datasource ndjson", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  pc %>%
      pathling_read_ndjson(ndjson_test_data_dir()) %>%
      ds_write_ndjson(temp_ndjson_dir())

  data_source <- pc %>% pathling_read_ndjson(temp_ndjson_dir())

  result <- ndjson_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource bundles", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  data_source <- pathling_read_bundles(pc, bundles_test_data_dir(), c("Patient", "Condition"))

  result <- bundles_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 10))
})

test_that("datasource datasets", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  data_source <- pathling_read_datasets(pc, list(
      "Patient" = pathling_encode(pc, spark_read_text(spark, file.path(ndjson_test_data_dir(), "Patient.ndjson")), "Patient"),
      "Condition" = pathling_encode(pc, spark_read_text(spark, file.path(ndjson_test_data_dir(), "Condition.ndjson")), "Condition")
  ))

  result <- ndjson_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource parquet", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  pc %>%
      pathling_read_parquet(parquet_test_data_dir()) %>%
      ds_write_parquet(temp_parquet_dir())

  data_source <- pc %>% pathling_read_parquet(temp_parquet_dir())

  result <- parquet_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource delta", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  pc %>%
      pathling_read_delta(delta_test_data_dir()) %>%
      ds_write_delta(temp_delta_dir())

  data_source <- pc %>% pathling_read_delta(delta_test_data_dir())

  result <- delta_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource delta merge", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  ds <- pc %>% pathling_read_delta(delta_test_data_dir())

  ds %>% ds_write_delta(temp_delta_dir(), save_mode = SaveMode$OVERWRITE)
  ds %>% ds_write_delta(temp_delta_dir(), save_mode = SaveMode$MERGE)

  data_source <- pc %>% pathling_read_delta(delta_test_data_dir())

  result <- delta_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource tables", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  pc %>%
      pathling_read_ndjson(ndjson_test_data_dir()) %>%
      ds_write_tables()
  data_source <- pc %>% pathling_read_tables()

  result <- ndjson_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

test_that("datasource tables with schema", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  pc %>%
      pathling_read_ndjson(ndjson_test_data_dir()) %>%
      ds_write_tables(schema = "test")
  data_source <- pc %>% pathling_read_tables(schema = "test")

  result <- ndjson_query(data_source)
  expect_equal(colnames(result), "count")
  expect_equal(collect(result), tibble::tibble(count = 71))
})

# Tests for write_details return value from write operations.

test_that("ndjson write returns details", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pc %>%
      pathling_read_ndjson(ndjson_test_data_dir()) %>%
      ds_write_ndjson(file.path(tempdir(), "ndjson_details_test"))

  # Verify result is a list with file_infos element.
  expect_true(is.list(result))
  expect_true("file_infos" %in% names(result))
  expect_true(length(result$file_infos) > 0)

  # Verify each file_info has the expected elements.
  for (file_info in result$file_infos) {
    expect_true("fhir_resource_type" %in% names(file_info))
    expect_true("absolute_url" %in% names(file_info))
    expect_false(is.null(file_info$fhir_resource_type))
    expect_false(is.null(file_info$absolute_url))
  }

  # Verify that expected resource types are present.
  resource_types <- sapply(result$file_infos, function(fi) fi$fhir_resource_type)
  expect_true("Patient" %in% resource_types)
  expect_true("Condition" %in% resource_types)
})

test_that("parquet write returns details", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pc %>%
      pathling_read_parquet(parquet_test_data_dir()) %>%
      ds_write_parquet(file.path(tempdir(), "parquet_details_test"))

  # Verify result is a list with file_infos element.
  expect_true(is.list(result))
  expect_true("file_infos" %in% names(result))
  expect_true(length(result$file_infos) > 0)

  # Verify each file_info has the expected elements.
  for (file_info in result$file_infos) {
    expect_true("fhir_resource_type" %in% names(file_info))
    expect_true("absolute_url" %in% names(file_info))
  }
})

test_that("delta write returns details", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pc %>%
      pathling_read_delta(delta_test_data_dir()) %>%
      ds_write_delta(file.path(tempdir(), "delta_details_test"))

  # Verify result is a list with file_infos element.
  expect_true(is.list(result))
  expect_true("file_infos" %in% names(result))
  expect_true(length(result$file_infos) > 0)

  # Verify each file_info has the expected elements.
  for (file_info in result$file_infos) {
    expect_true("fhir_resource_type" %in% names(file_info))
    expect_true("absolute_url" %in% names(file_info))
  }
})

test_that("tables write returns details", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  # Re-use the existing "test" schema to avoid conflicts.
  result <- pc %>%
      pathling_read_ndjson(ndjson_test_data_dir()) %>%
      ds_write_tables(schema = "test")

  # Verify result is a list with file_infos element.
  expect_true(is.list(result))
  expect_true("file_infos" %in% names(result))
  expect_true(length(result$file_infos) > 0)

  # Verify each file_info has the expected elements.
  for (file_info in result$file_infos) {
    expect_true("fhir_resource_type" %in% names(file_info))
    expect_true("absolute_url" %in% names(file_info))
  }
})
