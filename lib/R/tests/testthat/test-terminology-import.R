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

  pathling_disconnect(pc_local)
})

test_that("pathling_connect rejects local mode without a storage path", {
  expect_error(
    pathling_connect(terminology_mode = "local"),
    "terminology_storage_path is required"
  )
})
