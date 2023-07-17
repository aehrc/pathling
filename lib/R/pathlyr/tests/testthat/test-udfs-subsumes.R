subsumption_df <- function(spark) {
  df <- spark  %>% sdf_copy_to(
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      codeA = c(
        snomed_coding_row("107963000"),
        loinc_coding_row("55915-3"),
        NA
      ),
      codeB = list(
        list(snomed_coding_row("63816008")),
        list(snomed_coding_row("63816008"), loinc_coding_row("55914-3")),
        list(snomed_coding_row("107963000"))
      )
    ), overwrite = TRUE, struct_columns = c('codeA', 'codeB')
  )

  return(df)
}

#
# TODO: Fix and enable
#
# test_that("subsumes", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   subsumption_df <- subsumption_df(spark)
#
#   result_df <- subsumption_df %>%
#     select_expr(
#       id,
#       result = !!trm_subsumes(codeA, codeB)
#     )
#
#   expect_equal(
#     sdf_collect(result_df),
#     tibble::tibble(
#       id = c("id-1", "id-2", "id-3"),
#       result = list(TRUE, FALSE, NA)
#     )
#   )
#
#   result_df <- subsumption_df %>%
#     select_expr(
#       id,
#       result = !!trm_subsumes(codeA, !!trm_to_coding("63816008", SNOMED_URI))
#     )
#
#   expect_equal(
#     sdf_collect(result_df),
#     tibble::tibble(
#       id = c("id-1", "id-2", "id-3"),
#       result = list(TRUE, FALSE, NA)
#     )
#   )
#
#   result_df <- subsumption_df %>%
#     select_expr(
#       id,
#       result = !!trm_subsumes(
#         !!trm_to_coding("55914-3", LOINC_URI),
#         codeB
#       )
#     )
#
#   expect_equal(
#     sdf_collect(result_df),
#     tibble::tibble(
#       id = c("id-1", "id-2", "id-3"),
#       result = list(FALSE, TRUE, FALSE)
#     )
#   )
# })
#
# test_that("subsumed_by", {
#   spark <- def_spark()
#   pc <- def_ptl_context(spark)
#
#   subsumption_df <- subsumption_df(spark)
#
#   result_df <- subsumption_df %>%
#     select_expr(
#       id,
#       result = !!trm_subsumed_by(codeB, codeA)
#     )
#
#   expect_equal(
#     sdf_collect(result_df),
#     tibble::tibble(
#       id = c("id-1", "id-2", "id-3"),
#       result = list(TRUE, FALSE, NA)
#     )
#   )
#
#   result_df <- subsumption_df %>%
#     select_expr(
#       id,
#       result = !!trm_subsumed_by(
#         !!trm_to_coding("63816008", SNOMED_URI),
#         codeA
#       )
#     )
#
#   expect_equal(
#     sdf_collect(result_df),
#     tibble::tibble(
#       id = c("id-1", "id-2", "id-3"),
#       result = list(TRUE, FALSE, NA)
#     )
#   )
#
#   result_df <- subsumption_df %>%
#     select_expr(
#       id,
#       result = !!trm_subsumed_by(
#         codeB,
#         !!trm_to_coding("55914-3", LOINC_URI)
#       )
#     )
#
#   expect_equal(
#     sdf_collect(result_df),
#     tibble::tibble(
#       id = c("id-1", "id-2", "id-3"),
#       result = list(FALSE, TRUE, FALSE)
#     )
#   )
# })




