test_that("property_of", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  df <- spark %>% to_sdf(
    id = c("id-1", "id-2", "id-3"),
    code = c(
      snomed_coding_row("439319006"),
      loinc_coding_row("55915-3"),
      NA
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_property_of(code, "parent", PropertyType$CODE)
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list("785673007", "74754006"),
        list(),
        NA
      )
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_property_of(code, "inactive", PropertyType$BOOLEAN)
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        logical(0),
        FALSE,
        NA
      )
    )
  )

  result_df <- df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_property_of(
        !!tx_to_coding("55915-3", LOINC_URI),
        "display"
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = "id-1",
      result = list(
        list("Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis")
      )
    )
  )

  result_df <- df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_property_of(
        !!tx_to_coding("55915-3", LOINC_URI),
        "display",
        accept_language = "de"
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = "id-1",
      result = list(
        list("Beta-2-Globulin [Masse/Volumen] in Zerebrospinalflüssigkeit mit Elektrophorese")
      )
    )
  )
})
