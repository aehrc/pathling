USE_DISPLAY <- tx_to_coding(
  "display",
  "http://terminology.hl7.org/CodeSystem/designation-usage"
)

test_that("designation", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  property_df <- spark %>% to_sdf(
    id = c("id-1", "id-2", "id-3"),
    code = c(
      snomed_coding_row("439319006"),
      loinc_coding_row("55915-3"),
      NA
    )
  )

  result_df <- property_df %>%
    select_expr(
      id,
      result = !!tx_designation(code)
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list(
          "Screening for phenothiazine in serum",
          "Screening for phenothiazine in serum (procedure)"
        ),
        list(
          "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
          "Bêta-2 globulines [Masse/Volume] Liquide céphalorachidien",
          "Beta 2 globulin:MCnc:Pt:CSF:Qn:Electrophoresis"
        ),
        NA
      )
    )
  )

  result_df <- property_df %>%
    select_expr(
      id,
      result = !!tx_designation(code, !!USE_DISPLAY)
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(
        list("Screening for phenothiazine in serum"),
        list(
          "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
          "Bêta-2 globulines [Masse/Volume] Liquide céphalorachidien"
        ),
        NA
      )
    )
  )

  result_df <- property_df %>%
    select_expr(
      id,
      result = !!tx_designation(code, !!USE_DISPLAY, "fr-FR")
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      result = list(list(), list("Bêta-2 globulines [Masse/Volume] Liquide céphalorachidien"), NA)
    )
  )

  result_df <- property_df %>%
    head(1) %>%
    select_expr(
      id,
      result = !!tx_designation(
        !!tx_to_coding("439319006", SNOMED_URI),
        !!tx_to_coding("900000000000003001", SNOMED_URI),
        "en"
      )
    )

  expect_equal(
    sdf_collect(result_df),
    tibble::tibble(
      id = "id-1",
      result = list(list("Screening for phenothiazine in serum (procedure)"))
    )
  )
})
