spark_session <- function() {
  # Get the shaded JAR for testing purposes.
  spark <- sparklyr::spark_connect(master = "local[2]", config = list(
    #"spark.sql.warehouse.dir" = fs::dir_create_temp(),
    "spark.driver.memory" = "4g"
  ))

  #on.exit(sparklyr::spark_disconnect(spark), add = TRUE)
  spark
}

LOINC_URI <- "http://loinc.org"

snomed_coding_row <- function(code) {
    list(
      id = NA,
      system = SNOMED_URI,
      version = NA,
      code = code,
      display = NA,
      userSelected = NA,
      `_fid` = NA
    )
}

loinc_coding_row <- function(code) {

    list(
      id = NA,
      system = LOINC_URI,
      version = NA,
      code = code,
      display = NA,
      userSelected = NA,
      `_fid` = NA
    )
}


select_expr <-function(...) {
  mutate(..., .keep='none')
}

test_that("member_of", {
  spark <- spark_session()
  pc <- ptl_connect(spark)

  df <- spark  %>% sdf_copy_to(
    tibble::tibble(
        id = c("code-1", "code-2", "code-3"),
        code = list(
          snomed_coding_row("368529001"),
          loinc_coding_row("55915-3"),
          NULL
        )
      )
    )

  result_df_col <- df %>%
    select_expr(
      id,
      is_member = !!trm_member_of(code, "http://snomed.info/sct?fhir_vs=refset/723264001"),
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
      is_member = !!trm_member_of(code, "http://loinc.org/vs/LP14885-5")
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
      result = !!trm_member_of(
        !!trm_to_snomed_coding("368529001"),
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
