test_that("creates a default pathling context", {
  pc <- pathling_connect()

  expect_s3_class(pc, "spark_jobj")
})

# Helper to create a PathlingContext and encode Patient bundles for search tests.
search_test_setup <- function() {
  spark <- def_spark()
  pc <- pathling_connect(spark)
  json_bundles_dir <- test_path("testdata", "encoders", "bundles", "R4", "json")
  bundles_df <- sparklyr::spark_read_text(spark, json_bundles_dir, whole = TRUE) %>%
    dplyr::select(value = contents)
  patients_df <- pathling_encode_bundle(pc, bundles_df, "Patient")
  list(pc = pc, patients_df = patients_df)
}

test_that("search_to_column returns a spark_jobj for a single parameter", {
  setup <- search_test_setup()
  # A single search parameter should return a JVM Column object.
  filter_col <- pc_search_to_column(setup$pc, "Patient", "gender=male")
  expect_s3_class(filter_col, "spark_jobj")
})

test_that("search_to_column returns a spark_jobj for multiple parameters", {
  setup <- search_test_setup()
  # Multiple parameters combined with & should return a JVM Column object.
  filter_col <- pc_search_to_column(setup$pc, "Patient", "gender=male&active=true")
  expect_s3_class(filter_col, "spark_jobj")
})

test_that("search_to_column returns a spark_jobj for date prefix search", {
  setup <- search_test_setup()
  # Date prefix search should return a JVM Column object.
  filter_col <- pc_search_to_column(setup$pc, "Patient", "birthdate=ge1990-01-01")
  expect_s3_class(filter_col, "spark_jobj")
})

test_that("search_to_column with empty expression matches all resources", {
  setup <- search_test_setup()
  initial_count <- setup$patients_df %>% sparklyr::sdf_nrow()
  # An empty search expression should match all resources.
  empty_filter <- pc_search_to_column(setup$pc, "Patient", "")
  filtered <- sparklyr::spark_dataframe(setup$patients_df) %>%
    j_invoke("filter", empty_filter) %>%
    sparklyr::sdf_register()
  expect_equal(filtered %>% sparklyr::sdf_nrow(), initial_count)
})

test_that("search_to_column filters a DataFrame correctly", {
  setup <- search_test_setup()
  initial_count <- setup$patients_df %>% sparklyr::sdf_nrow()
  # Filtering by gender=male should return fewer or equal rows.
  gender_filter <- pc_search_to_column(setup$pc, "Patient", "gender=male")
  filtered <- sparklyr::spark_dataframe(setup$patients_df) %>%
    j_invoke("filter", gender_filter) %>%
    sparklyr::sdf_register()
  expect_true(filtered %>% sparklyr::sdf_nrow() <= initial_count)
})

test_that("search_to_column raises error for invalid parameter", {
  setup <- search_test_setup()
  # An invalid search parameter should raise an error.
  expect_error(
    pc_search_to_column(setup$pc, "Patient", "invalid-param=value")
  )
})

# ========== pc_fhirpath_to_column tests ==========

test_that("fhirpath_to_column returns a spark_jobj for a boolean expression", {
  setup <- search_test_setup()
  # A boolean FHIRPath expression should return a JVM Column object.
  filter_col <- pc_fhirpath_to_column(setup$pc, "Patient", "gender = 'male'")
  expect_s3_class(filter_col, "spark_jobj")
})

test_that("fhirpath_to_column filters a DataFrame correctly", {
  setup <- search_test_setup()
  initial_count <- setup$patients_df %>% sparklyr::sdf_nrow()
  # Filtering with a boolean FHIRPath expression should return fewer or equal rows.
  gender_filter <- pc_fhirpath_to_column(setup$pc, "Patient", "gender = 'male'")
  filtered <- sparklyr::spark_dataframe(setup$patients_df) %>%
    j_invoke("filter", gender_filter) %>%
    sparklyr::sdf_register()
  expect_true(filtered %>% sparklyr::sdf_nrow() <= initial_count)
})

test_that("fhirpath_to_column returns a spark_jobj for a value expression", {
  setup <- search_test_setup()
  # A value FHIRPath expression should return a JVM Column object.
  name_col <- pc_fhirpath_to_column(setup$pc, "Patient", "name.given.first()")
  expect_s3_class(name_col, "spark_jobj")
})

test_that("fhirpath_to_column raises error for invalid expression", {
  setup <- search_test_setup()
  # An invalid FHIRPath expression should raise an error.
  expect_error(
    pc_fhirpath_to_column(setup$pc, "Patient", "!!invalid!!")
  )
})

# ========== pathling_filter tests (FHIRPath) ==========

test_that("pathling_filter returns a tbl_spark with FHIRPath expression", {
  setup <- search_test_setup()
  # Filtering by gender should return a tbl_spark with fewer rows.
  result <- setup$patients_df %>%
    pathling_filter(setup$pc, "Patient", "gender = 'male'")
  expect_s3_class(result, "tbl_spark")
  initial_count <- setup$patients_df %>% sparklyr::sdf_nrow()
  expect_true(result %>% sparklyr::sdf_nrow() <= initial_count)
  expect_true(result %>% sparklyr::sdf_nrow() > 0)
})

test_that("pathling_filter with combined boolean FHIRPath expression", {
  setup <- search_test_setup()
  # A combined boolean expression should further restrict rows.
  result <- setup$patients_df %>%
    pathling_filter(setup$pc, "Patient", "gender = 'male' and birthDate > @1970-01-01")
  expect_s3_class(result, "tbl_spark")
  expect_true(result %>% sparklyr::sdf_nrow() > 0)
})

test_that("pathling_filter raises error for invalid FHIRPath expression", {
  setup <- search_test_setup()
  # An invalid FHIRPath expression should raise an error.
  expect_error(
    setup$patients_df %>% pathling_filter(setup$pc, "Patient", "!!invalid!!")
  )
})

# ========== pathling_filter tests (search) ==========

test_that("pathling_filter with search type returns a tbl_spark", {
  setup <- search_test_setup()
  # Filtering with search syntax should return a tbl_spark with fewer rows.
  result <- setup$patients_df %>%
    pathling_filter(setup$pc, "Patient", "gender=male", type = "search")
  expect_s3_class(result, "tbl_spark")
  initial_count <- setup$patients_df %>% sparklyr::sdf_nrow()
  expect_true(result %>% sparklyr::sdf_nrow() <= initial_count)
  expect_true(result %>% sparklyr::sdf_nrow() > 0)
})

test_that("pathling_filter with multiple search parameters", {
  setup <- search_test_setup()
  # Multiple search parameters combined with & should filter correctly.
  result <- setup$patients_df %>%
    pathling_filter(setup$pc, "Patient", "gender=male&birthdate=ge1990-01-01", type = "search")
  expect_s3_class(result, "tbl_spark")
})

test_that("pathling_filter with search type raises error for invalid parameter", {
  setup <- search_test_setup()
  # An invalid search parameter should raise an error.
  expect_error(
    setup$patients_df %>%
      pathling_filter(setup$pc, "Patient", "invalid-param=value", type = "search")
  )
})

# ========== pathling_with_column tests ==========

test_that("pathling_with_column adds a named column", {
  setup <- search_test_setup()
  # Adding a column should return a tbl_spark with the new column present.
  result <- setup$patients_df %>%
    pathling_with_column(setup$pc, "Patient", "name.given.first()", column = "given_name")
  expect_s3_class(result, "tbl_spark")
  expect_true("given_name" %in% colnames(result))
  # Row count should be preserved.
  expect_equal(
    result %>% sparklyr::sdf_nrow(),
    setup$patients_df %>% sparklyr::sdf_nrow()
  )
})

test_that("pathling_with_column supports chained calls", {
  setup <- search_test_setup()
  # Chaining multiple pathling_with_column calls should add all columns.
  result <- setup$patients_df %>%
    pathling_with_column(setup$pc, "Patient", "name.given.first()", column = "given_name") %>%
    pathling_with_column(setup$pc, "Patient", "gender", column = "gender_value")
  expect_s3_class(result, "tbl_spark")
  expect_true("given_name" %in% colnames(result))
  expect_true("gender_value" %in% colnames(result))
})

test_that("pathling_with_column raises error for invalid expression", {
  setup <- search_test_setup()
  # An invalid FHIRPath expression should raise an error.
  expect_error(
    setup$patients_df %>%
      pathling_with_column(setup$pc, "Patient", "!!invalid!!", column = "bad")
  )
})

# ========== piped combination tests ==========

test_that("pathling_filter and pathling_with_column work together in a pipe", {
  setup <- search_test_setup()
  # Filtering then adding a column should work in a single pipe.
  result <- setup$patients_df %>%
    pathling_filter(setup$pc, "Patient", "gender = 'male'") %>%
    pathling_with_column(setup$pc, "Patient", "name.given.first()", column = "given") %>%
    dplyr::select(id, given)
  expect_s3_class(result, "tbl_spark")
  expect_equal(colnames(result), c("id", "given"))
  # Should have only male patients.
  expect_true(result %>% sparklyr::sdf_nrow() > 0)
})
