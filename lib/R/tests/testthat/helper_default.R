library(dplyr)
library(sparklyr)

def_spark <- function() {

  temp_warehouse_dir <- file.path(tempdir(), 'warehouse')
  spark <- sparklyr::spark_connect(
      master = "local[1]",
      config = list(
          "sparklyr.shell.driver-memory" = "4G",
          "sparklyr.shell.driver-java-options" = '"-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7896"',
          "sparklyr.shell.conf" = c(
              "spark.sql.mapKeyDedupPolicy=LAST_WIN",
              "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
              "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
              "spark.sql.catalogImplementation=hive",
              sprintf("spark.sql.warehouse.dir=\'%s\'", temp_warehouse_dir)
          )
      )
  )

  spark %>% sdf_sql("CREATE DATABASE IF NOT EXISTS test")
  spark
}

def_pathling_context <- function(spark) {

  encoders <- spark %>%
      j_invoke_static("au.csiro.pathling.encoders.FhirEncoders", "forR4") %>%
      j_invoke("withMaxNestingLevel", as.integer(0)) %>%
      j_invoke("withExtensionsEnabled", as.logical(FALSE)) %>%
      j_invoke("withOpenTypes", j_invoke_static(spark, "java.util.Collections", "emptySet")) %>%
      j_invoke("getOrCreate")

  terminology_service_factory <- spark %>%
      j_invoke_new("au.csiro.pathling.terminology.mock.MockTerminologyServiceFactory")

  spark %>%
      j_invoke_static("au.csiro.pathling.library.PathlingContext", "createInternal",
                      spark_session(spark), encoders, terminology_service_factory)
}


coding_row <- function(system, code) {
  coding <- list(
      x0_id = NA,
      x1_system = system,
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


select_expr <- function(...) {
  mutate(..., .keep = 'none')
}


to_sdf <- function(spark, ...) {

  tbl <- tibble::tibble(...)
  tbl_columns <- names(tbl)
  tbl_struct_columns <- tbl_columns[grepl("code.*", tbl_columns)]
  spark %>% sdf_copy_to(tbl, overwrite = TRUE, struct_columns = tbl_struct_columns)
}
