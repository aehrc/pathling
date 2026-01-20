subsumption_df <- function(spark) {
  spark %>%
    to_sdf(
      id = c("id-1", "id-2", "id-3"),
      codeA = c(
        snomed_coding_row("107963000"),
        loinc_coding_row("55915-3"),
        NA
      ),
      codeB1 = c(
        snomed_coding_row("63816008"),
        snomed_coding_row("63816008"),
        snomed_coding_row("107963000")
      ),
      # Note: we duplicate the first coding here as it cannot be NA as in python version
      # Becuse it's needed to infer the schema of the column
      codeB2 = c(
        snomed_coding_row("63816008"),
        loinc_coding_row("55914-3"),
        NA
      )
    ) %>%
    select_expr(id, codeA, codeB = if (is.null(codeB2)) array(codeB1) else array(codeB1, codeB2))
}

test_that("subsumed_by", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  df <- subsumption_df(spark)

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_subsumed_by(codeB, codeA)
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = c(TRUE, FALSE, NA)
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_subsumed_by(
        !!tx_to_coding("63816008", SNOMED_URI),
        codeA
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = c(TRUE, FALSE, NA)
    )
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!tx_subsumed_by(
        codeB,
        !!tx_to_coding("55914-3", LOINC_URI)
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = c(FALSE, TRUE, FALSE)
    )
  )
})
