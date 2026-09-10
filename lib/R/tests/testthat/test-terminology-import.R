fhir_fixtures_dir <- function() {
  # The FHIR fixtures live in the sibling terminology module's test resources. The testthat
  # working directory is the test directory, so the repository root is four levels up.
  normalizePath(
    file.path(
      "..", "..", "..", "..",
      "terminology", "src", "test", "resources", "fhir-fixtures", "json"
    ),
    mustWork = FALSE
  )
}

test_that("pathling_import_fhir_terminology enables local member_of", {
  spark <- def_spark()
  store <- file.path(tempdir(), "r-fhir-store")

  # Import through a default (server-mode) context, then query through a local-mode context.
  pc_import <- pathling_connect(spark)
  pathling_import_fhir_terminology(pc_import, fhir_fixtures_dir(), store)

  pc_local <- pathling_connect(
    spark,
    terminology_mode = "local",
    terminology_storage_path = store
  )

  # The column is deliberately not named "code", which to_sdf would treat as a JSON struct column.
  df <- spark %>% to_sdf(
    animal = c("dog", "sparrow")
  )

  result <- df %>%
    select_expr(
      animal,
      is_member = !!tx_member_of(
        !!tx_to_coding(animal, "http://example.org/fhir/CodeSystem/animal-species"),
        "http://example.org/fhir/ValueSet/mammals-enumerated"
      )
    )

  expect_equal(
    sdf_collect(result),
    tibble::tibble(
      animal = c("dog", "sparrow"),
      is_member = c(TRUE, FALSE)
    )
  )

  # Do not disconnect here. This context wraps the shared connection returned by def_spark(),
  # which sparklyr reuses across every test in the suite. Disconnecting it tears down the shared
  # gateway and leaves the following test unable to re-establish its session.
})

test_that("pathling_connect rejects local mode without a storage path", {
  expect_error(
    pathling_connect(terminology_mode = "local"),
    "terminology_storage_path is required"
  )
})

rf2_mini_dir <- function() {
  # The rf2-mini fixture lives in the sibling terminology module's test resources.
  normalizePath(
    file.path(
      "..", "..", "..", "..",
      "terminology", "src", "test", "resources", "rf2-mini", "international-20230601"
    ),
    mustWork = FALSE
  )
}

test_that("pathling_import_snomed honours the named default dialect", {
  spark <- def_spark()
  store <- file.path(tempdir(), "r-snomed-store-gb")

  pc_import <- pathling_connect(spark)
  pathling_import_snomed(
    pc_import, rf2_mini_dir(), store,
    default_dialect = "en-GB"
  )

  pc_local <- pathling_connect(
    spark,
    terminology_mode = "local",
    terminology_storage_path = store
  )

  # PANCREAS_STRUCTURE is one of the three fixture concepts the GB and US English reference sets
  # disagree about, so naming GB English is observable in the stored display.
  df <- spark %>% to_sdf(concept = "1010008")
  result <- df %>%
    select_expr(
      concept,
      term = !!tx_display(!!tx_to_snomed_coding(concept))
    )

  expect_equal(sdf_collect(result)$term, "Structure of pancreas")
})

test_that("pathling_import_snomed rejects a dialect the release does not hold", {
  spark <- def_spark()
  store <- file.path(tempdir(), "r-snomed-store-missing")

  pc_import <- pathling_connect(spark)
  expect_error(
    pathling_import_snomed(
      pc_import, rf2_mini_dir(), store,
      default_dialect = "es"
    )
  )
})

test_that("pathling_connect accepts dialect aliases in local mode", {
  spark <- def_spark()
  store <- file.path(tempdir(), "r-snomed-store-aliased")

  pc_import <- pathling_connect(spark)
  pathling_import_snomed(pc_import, rf2_mini_dir(), store)

  pc_local <- pathling_connect(
    spark,
    terminology_mode = "local",
    terminology_storage_path = store,
    dialect_aliases = c("en-XX" = "900000000000508004")
  )

  # The registered tag names the GB English reference set, so it selects the GB term where the
  # unnamed import stored the US one.
  df <- spark %>% to_sdf(concept = "1010008")
  result <- df %>%
    select_expr(
      concept,
      term = !!tx_display(!!tx_to_snomed_coding(concept), "en-XX")
    )

  expect_equal(sdf_collect(result)$term, "Structure of pancreas")
})
