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

write_divergent_parquet <- function(spark, warehouse) {
  table <- file.path(warehouse, "Patient.parquet")
  first <- sparklyr::copy_to(spark, data.frame(id = "p1", gender = "male"), "first_batch", overwrite = TRUE)
  second <- sparklyr::copy_to(spark, data.frame(id = "p2", birthDate = "1990-01-01"), "second_batch", overwrite = TRUE)
  sparklyr::spark_write_parquet(sparklyr::sdf_coalesce(first, 1), table, mode = "overwrite")
  sparklyr::spark_write_parquet(sparklyr::sdf_coalesce(second, 1), table, mode = "append")
  table
}

test_that("datasource parquet merges divergent files", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  warehouse <- file.path(tempdir(), "parquet-divergent")
  write_divergent_parquet(spark, warehouse)

  patients <- pc %>%
    pathling_read_parquet(warehouse) %>%
    ds_read("Patient")

  expect_true(all(c("id", "gender", "birthDate") %in% colnames(patients)))
  expect_equal(sparklyr::sdf_nrow(patients), 2)
})

test_that("datasource parquet with supplied schemas", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  warehouse <- file.path(tempdir(), "parquet-supplied")
  table <- write_divergent_parquet(spark, warehouse)
  union <- spark %>%
    sparklyr::spark_session() %>%
    sparklyr::invoke("read") %>%
    sparklyr::invoke("format", "parquet") %>%
    sparklyr::invoke("option", "mergeSchema", "true") %>%
    sparklyr::invoke("load", table) %>%
    sparklyr::invoke("schema")

  patients <- pc %>%
    pathling_read_parquet(warehouse, schemas = list(Patient = union %>% sparklyr::invoke("json"))) %>%
    ds_read("Patient")

  expect_equal(
    patients %>% sparklyr::spark_dataframe() %>% sparklyr::invoke("schema") %>% sparklyr::invoke("json"),
    union %>% sparklyr::invoke("json")
  )
  expect_equal(sparklyr::sdf_nrow(patients), 2)
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

# Tests for layout detection and its opt-out.

# Writes the given Patient resources, as JSON strings, to an NDJSON file in a new directory, and
# returns the directory's path.
write_patients <- function(name, patients) {
  directory <- file.path(tempdir(), paste0(name, "-", as.integer(runif(1, 0, 1e9))))
  dir.create(directory)
  writeLines(patients, file.path(directory, "Patient.ndjson"))
  directory
}

# Creates a database of its own for one test, and returns its name.
new_database <- function(spark) {
  database <- paste0("tables_", as.integer(runif(1, 0, 1e9)))
  spark %>%
    sparklyr::spark_session() %>%
    sparklyr::invoke("sql", paste("CREATE DATABASE", database))
  database
}

# Returns the format the catalog records for a table, such as "delta" or "parquet".
table_provider <- function(spark, table) {
  description <- spark %>%
    sdf_sql(paste("DESCRIBE TABLE EXTENDED", table)) %>%
    collect()
  tolower(description$data_type[description$col_name == "Provider"])
}

test_that("datasource tables in delta format take a wider append", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  database <- new_database(spark)
  table <- paste0(database, ".Patient")
  narrow <- write_patients("narrow", '{"resourceType":"Patient","id":"p1","gender":"male"}')
  wide <- write_patients(
    "wide",
    '{"resourceType":"Patient","id":"p2","gender":"female","birthDate":"1990-01-01"}'
  )

  pc %>%
    pathling_read_ndjson(narrow) %>%
    ds_write_tables(schema = database, format = "delta")
  # The first batch's fitted schema lacks the element the second carries.
  expect_false("birthDate" %in% colnames(sdf_sql(spark, paste("SELECT * FROM", table))))
  pc %>%
    pathling_read_ndjson(wide) %>%
    ds_write_tables(schema = database, save_mode = SaveMode$APPEND, format = "delta")

  patients <- sdf_sql(spark, paste("SELECT * FROM", table))
  expect_equal(table_provider(spark, table), "delta")
  expect_true("birthDate" %in% colnames(patients))
  expect_equal(sort(collect(patients)$id), c("p1", "p2"))
})

test_that("datasource tables in delta format take a second merge", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  database <- new_database(spark)
  table <- paste0(database, ".Patient")
  first <- write_patients("first", '{"resourceType":"Patient","id":"p1","gender":"male"}')
  second <- write_patients("second", c(
    '{"resourceType":"Patient","id":"p1","gender":"female"}',
    '{"resourceType":"Patient","id":"p2","gender":"other","birthDate":"1990-01-01"}'
  ))

  pc %>%
    pathling_read_ndjson(first) %>%
    ds_write_tables(schema = database, save_mode = SaveMode$MERGE, format = "delta")
  pc %>%
    pathling_read_ndjson(second) %>%
    ds_write_tables(schema = database, save_mode = SaveMode$MERGE, format = "delta")

  patients <- sdf_sql(spark, paste("SELECT * FROM", table)) %>%
    select(id, gender) %>%
    arrange(id) %>%
    collect()
  expect_equal(table_provider(spark, table), "delta")
  expect_equal(patients, tibble::tibble(id = c("p1", "p2"), gender = c("female", "other")))
})

test_that("datasource tables take a format without a schema", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  database <- new_database(spark)
  patients <- write_patients("patients", '{"resourceType":"Patient","id":"p1","gender":"male"}')
  catalog <- spark %>%
    sparklyr::spark_session() %>%
    sparklyr::invoke("catalog")
  previous <- catalog %>% sparklyr::invoke("currentDatabase")
  catalog %>% sparklyr::invoke("setCurrentDatabase", database)
  on.exit(catalog %>% sparklyr::invoke("setCurrentDatabase", previous), add = TRUE)

  pc %>%
    pathling_read_ndjson(patients) %>%
    ds_write_tables(format = "delta")

  expect_equal(table_provider(spark, paste0(database, ".Patient")), "delta")
})

test_that("datasource tables take the default format when none is given", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  database <- new_database(spark)
  table <- paste0(database, ".Patient")
  patients <- write_patients("patients", '{"resourceType":"Patient","id":"p1","gender":"male"}')

  pc %>%
    pathling_read_ndjson(patients) %>%
    ds_write_tables(schema = database)

  default_format <- spark %>%
    sparklyr::spark_session() %>%
    sparklyr::invoke("conf") %>%
    sparklyr::invoke("get", "spark.sql.sources.default")
  expect_equal(table_provider(spark, table), tolower(default_format))
  expect_equal(sparklyr::sdf_nrow(sdf_sql(spark, paste("SELECT * FROM", table))), 1)
})

previous_layout_refusal <- paste0(
  "The Patient data is stored in the layout written by earlier releases of Pathling"
)

# Writes Patients carrying the root versioned id that the layout of earlier releases stores beside
# every resource id, through the given kind of storage, and returns a function that opens a data
# source over them, taking the layout detection setting.
write_previous_layout <- function(spark, pc, kind) {
  warehouse <- file.path(tempdir(), paste0("previous-", kind, "-", as.integer(runif(1, 0, 1e9))))
  patients <- sparklyr::copy_to(
    spark,
    data.frame(id = "p1", id_versioned = "Patient/p1/_history/1", gender = "female"),
    "previous_patients",
    overwrite = TRUE
  )
  writer <- patients %>%
    sparklyr::spark_dataframe() %>%
    sparklyr::invoke("write")
  table <- file.path(warehouse, "Patient.parquet")
  if (kind == "parquet") {
    writer %>% sparklyr::invoke("parquet", table)
    return(function(detection) pathling_read_parquet(pc, warehouse, layout_detection = detection))
  }
  if (kind == "delta") {
    writer %>%
      sparklyr::invoke("format", "delta") %>%
      sparklyr::invoke("save", table)
    return(function(detection) pathling_read_delta(pc, warehouse, layout_detection = detection))
  }
  database <- paste0("previous_", as.integer(runif(1, 0, 1e9)))
  spark %>%
    sparklyr::spark_session() %>%
    sparklyr::invoke("sql", paste("CREATE DATABASE", database))
  writer %>%
    sparklyr::invoke("format", "delta") %>%
    sparklyr::invoke("saveAsTable", paste0(database, ".Patient"))
  function(detection) pathling_read_tables(pc, schema = database, layout_detection = detection)
}

for (kind in c("parquet", "delta", "tables")) {
  test_that(paste("previous layout is refused from", kind), {
    spark <- def_spark()
    pc <- def_pathling_context(spark)
    open_source <- write_previous_layout(spark, pc, kind)

    expect_error(open_source(TRUE) %>% ds_read("Patient"), previous_layout_refusal)
  })

  test_that(paste("layout detection opt-out permits the read from", kind), {
    spark <- def_spark()
    pc <- def_pathling_context(spark)
    open_source <- write_previous_layout(spark, pc, kind)

    patients <- open_source(FALSE) %>% ds_read("Patient")

    expect_true("id_versioned" %in% colnames(patients))
    expect_equal(sparklyr::sdf_nrow(patients), 1)
  })
}

test_that("layout detection opt-out is scoped to one source", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  open_source <- write_previous_layout(spark, pc, "parquet")
  opted_out <- open_source(FALSE)
  detecting <- open_source(TRUE)

  expect_equal(sparklyr::sdf_nrow(opted_out %>% ds_read("Patient")), 1)
  expect_error(detecting %>% ds_read("Patient"), previous_layout_refusal)
})

for (layout_detection in c(TRUE, FALSE)) {
  test_that(paste("conforming data reads alike with layout detection", layout_detection), {
    spark <- def_spark()
    pc <- def_pathling_context(spark)

    parquet <- pc %>%
      pathling_read_parquet(parquet_test_data_dir(), layout_detection = layout_detection)
    delta <- pc %>%
      pathling_read_delta(delta_test_data_dir(), layout_detection = layout_detection)

    expect_equal(collect(parquet_query(parquet)), tibble::tibble(count = 71))
    expect_equal(collect(delta_query(delta)), tibble::tibble(count = 71))
  })
}

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
