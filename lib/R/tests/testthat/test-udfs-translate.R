test_that("translate", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  df <- spark %>% to_sdf(
    id = c("id-1", "id-2", "id-3"),
    code = c(
      snomed_coding_row("368529001"),
      loinc_coding_row("55915-3"),
      NA
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_translate(code, "http://snomed.info/sct?fhir_cm=100")
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list(snomed_coding_result("368529002")),
        list(),
        NA
      )
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_translate(
        code,
        "http://snomed.info/sct?fhir_cm=100",
        equivalences = c("equivalent", "relatedto")
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list(snomed_coding_result("368529002"), loinc_coding_result("55916-3")),
        list(),
        NA
      )
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_translate(
        code,
        "http://snomed.info/sct?fhir_cm=100",
        equivalences = c("equivalent", "relatedto"),
        target = LOINC_URI
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list(loinc_coding_result("55916-3")),
        list(),
        NA
      )
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_translate(
        code,
        "http://snomed.info/sct?fhir_cm=200",
        equivalences = c("equivalent", "relatedto")
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list(),
        list(),
        NA
      )
    )
  )

  result_df <- df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_translate(
        !!tx_to_coding("55915-3", LOINC_URI),
        "http://snomed.info/sct?fhir_cm=200",
        reverse = TRUE,
        equivalences = "relatedto"
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = "id-1",
      result = list(
        list(snomed_coding_result("368529002"))
      )
    )
  )
})
