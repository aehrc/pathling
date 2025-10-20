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

test_that("datasource delta merge with delete", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  full_data <- pc %>% pathling_read_delta(delta_test_data_dir())

  temp_dir <- tempfile("delta_merge_delete_")
  withr::defer(unlink(temp_dir, recursive = TRUE, force = TRUE))

  full_data %>% ds_write_delta(temp_dir, save_mode = SaveMode$OVERWRITE)

  subset_patients <- full_data %>%
      ds_read("Patient") %>%
      dplyr::filter(id %in% c(
          "8ee183e2-b3c0-4151-be94-b945d6aa8c6d",
          "beff242e-580b-47c0-9844-c1a68c36c5bf",
          "e62e52ae-2d75-4070-a0ae-3cc78d35ed08"
      ))

  subset_data <- pc %>% pathling_read_datasets(list(
      "Patient" = subset_patients,
      "Condition" = full_data %>% ds_read("Condition")
  ))

  subset_data %>% ds_write_delta(temp_dir, save_mode = SaveMode$MERGE, delete_on_merge = TRUE)

  merged_data <- pc %>% pathling_read_delta(temp_dir)
  merged_count <- merged_data %>%
      ds_read("Patient") %>%
      summarise(count = n()) %>%
      collect()

  expect_equal(merged_count$count, 3)
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

test_that("datasource tables merge with delete", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  full_data <- pc %>% pathling_read_ndjson(ndjson_test_data_dir())

  withr::defer(spark %>% sdf_sql("DROP TABLE IF EXISTS test.Patient"))
  withr::defer(spark %>% sdf_sql("DROP TABLE IF EXISTS test.Condition"))

  full_data %>%
      ds_write_tables(schema = "test", save_mode = SaveMode$OVERWRITE, table_format = "delta")

  initial_count <- pc %>%
      pathling_read_tables(schema = "test") %>%
      ds_read("Patient") %>%
      summarise(count = n()) %>%
      collect()
  expect_equal(initial_count$count, 9)

  subset_patients <- full_data %>%
      ds_read("Patient") %>%
      dplyr::filter(id %in% c(
          "8ee183e2-b3c0-4151-be94-b945d6aa8c6d",
          "beff242e-580b-47c0-9844-c1a68c36c5bf",
          "e62e52ae-2d75-4070-a0ae-3cc78d35ed08"
      ))

  subset_data <- pc %>% pathling_read_datasets(list(
      "Patient" = subset_patients,
      "Condition" = full_data %>% ds_read("Condition")
  ))

  subset_data %>% ds_write_tables(
      schema = "test",
      save_mode = SaveMode$MERGE,
      table_format = "delta",
      delete_on_merge = TRUE
  )

  merged_count <- pc %>%
      pathling_read_tables(schema = "test") %>%
      ds_read("Patient") %>%
      summarise(count = n()) %>%
      collect()

  expect_equal(merged_count$count, 3)
})

test_that("ds_write_tables requires schema when table_format specified", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  ds <- pc %>% pathling_read_ndjson(ndjson_test_data_dir())

  expect_error(
      ds %>% ds_write_tables(table_format = "delta"),
      "schema must be provided when table_format is specified"
  )
})
