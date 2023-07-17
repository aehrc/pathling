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


to_sdf <- function(spark, ...) {

  tbl <- tibble::tibble(...)
  tbl_columns <- names(tbl)
  tbl_struct_columns <- tbl_columns[grepl("code.*", tbl_columns)]
  spark %>% sdf_copy_to(tbl, overwrite = TRUE, struct_columns = tbl_struct_columns)
}
