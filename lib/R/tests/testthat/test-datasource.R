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

  ds %>% ds_write_delta(temp_delta_dir(), import_mode = ImportMode$OVERWRITE)
  ds %>% ds_write_delta(temp_delta_dir(), import_mode = ImportMode$MERGE)

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
