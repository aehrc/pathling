test_that("member_of", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  df <- spark %>% to_sdf(
    id = c("code-1", "code-2", "code-3"),
    code = c(
      snomed_coding_row("368529001"),
      loinc_coding_row("55915-3"),
      NA
    )
  )

  result_df_col <- df %>%
    select_expr(
      id,
      is_member = !!tx_member_of(code, "http://snomed.info/sct?fhir_vs=refset/723264001"),
    )

  expect_equal(
    sdf_collect(result_df_col),
    tibble::tibble(
      id = c("code-1", "code-2", "code-3"),
      is_member = c(TRUE, FALSE, NA)
    )
  )

  result_df_str <- df %>%
    select_expr(
      id,
      is_member = !!tx_member_of(code, "http://loinc.org/vs/LP14885-5")
    )

  expect_equal(
    sdf_collect(result_df_str),
    tibble::tibble(
      id = c("code-1", "code-2", "code-3"),
      is_member = c(FALSE, TRUE, NA)
    )
  )

  result_df_coding <- df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_member_of(
        !!tx_to_snomed_coding("368529001"),
        "http://snomed.info/sct?fhir_vs=refset/723264001"
      )
    )

  expect_equal(
    sdf_collect(result_df_coding),
    tibble::tibble(
      id = "code-1",
      result = TRUE
    )
  )
})
