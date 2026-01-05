test_that("display", {
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

  expected_result <-
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = c(NA, "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis", NA),
    )


  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_display(code)
    )

  expect_equal(
    sdf_collect(result_df),
    expected_result
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_display(code)
    )

  expect_equal(
    sdf_collect(result_df),
    expected_result
  )

  result_df <- df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_display(!!tx_to_coding("55915-3", LOINC_URI))
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = "id-1",
      result = "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis"
    )
  )

  result_df <- df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_display(!!tx_to_coding("55915-3", LOINC_URI), "fr-FR")
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = "id-1",
      result = "Bêta-2 globulines [Masse/Volume] Liquide céphalorachidien"
    )
  )
})
