def_spark <- function() {
  # Get the shaded JAR for testing purposes.
  spark <- sparklyr::spark_connect(master = "local[2]", config = list(
    #"spark.sql.warehouse.dir" = fs::dir_create_temp(),
    "spark.driver.memory" = "4g"
  ))

  #on.exit(sparklyr::spark_disconnect(spark), add = TRUE)
  spark
}


def_ptl_context <- function(spark) {

    encoders <- spark %>%
      invoke_static("au.csiro.pathling.encoders.FhirEncoders", "forR4") %>%
      invoke("withMaxNestingLevel", as.integer(0)) %>%
      invoke("withExtensionsEnabled", as.logical(FALSE)) %>%
      invoke("withOpenTypes", invoke_static(spark, "java.util.Collections", "emptySet")) %>%
      invoke("getOrCreate")

    terminology_service_factory <- spark %>%
       invoke_new("au.csiro.pathling.terminology.mock.MockTerminologyServiceFactory")

    spark %>%
      invoke_static("au.csiro.pathling.library.PathlingContext", "create",
                   spark_session(spark), encoders, terminology_service_factory)
}

LOINC_URI <- "http://loinc.org"


coding_row <- function(system, code) {
   coding <- list(
      x0_id = NA,
      x1_system = system ,
      x2_version = NA,
      x3_code = code,
      x4_display = NA,
      x5_userSelected = NA,
      x6__fid = NA
    )
  jsonlite::toJSON(
    as.list(coding),
    na = "null",
    auto_unbox = TRUE,
    digits = NA
  )
}

snomed_coding_row <- function(code) {
  coding_row(SNOMED_URI, code)
}

loinc_coding_row <- function(code) {
  coding_row(LOINC_URI, code)
}


snomed_coding_result <- function(code) {
  list(system = SNOMED_URI, code = code)
}


loinc_coding_result <- function(code) {
  list(system = LOINC_URI, code = code)
}


select_expr <-function(...) {
  mutate(..., .keep='none')
}

test_that("member_of", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  df <- spark  %>% sdf_copy_to(
    tibble::tibble(
        id = c("code-1", "code-2", "code-3"),
        code = c(
          snomed_coding_row("368529001"),
          loinc_coding_row("55915-3"),
          NA)
      ), overwrite = TRUE, struct_columns = 'code'
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


test_that("translate", {
  spark <- def_spark()
  pc <- def_ptl_context(spark)

  df <- spark  %>% sdf_copy_to(
    tibble::tibble(
      id = c("id-1", "id-2", "id-3"),
      code = c(
        snomed_coding_row("368529001"),
        loinc_coding_row("55915-3"),
        NA)
    ), overwrite = TRUE, struct_columns = 'code'
  )

  result_df <- df %>%
    select_expr(
      id,
      result = !!trm_translate(code, "http://snomed.info/sct?fhir_cm=100")
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
      result = !!trm_translate(
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
      result = !!trm_translate(
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
      result = !!trm_translate(
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
      result = !!trm_translate(
        !!trm_to_coding("55915-3", LOINC_URI),
        "http://snomed.info/sct?fhir_cm=200",
        reverse = TRUE,
        equivalences = c("relatedto")
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



