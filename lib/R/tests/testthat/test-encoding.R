spark_session <- function() {
  # # Get the shaded JAR for testing purposes.
  # spark <- sparklyr::spark_connect(master = "local[2]", config = list(
  #   #"spark.sql.warehouse.dir" = fs::dir_create_temp(),
  #   "spark.driver.memory" = "4g"
  # ))
  #
  # #on.exit(sparklyr::spark_disconnect(spark), add = TRUE)
  # spark
  def_spark()
}


encoders_test_data_dir <- function(...) {
  test_path("testdata", "encoders", ...)
}

json_bundles_dir <- function() {
  encoders_test_data_dir("bundles", "R4", "json")
}

json_resources_dir <- function() {
  encoders_test_data_dir("resources", "R4", "json")
}

xml_bundles_dir <- function() {
  encoders_test_data_dir("bundles", "R4", "xml")
}

def_pathling <- function(spark_session) {
  pathling_connect(spark_session)
}

json_resources_df <- function(spark_session, json_resources_dir) {
  sparklyr::spark_read_text(spark_session, json_resources_dir)
}

json_bundles_df <- function(spark_session, json_bundles_dir) {
  # sparklyr::spark_read_text() produces dataframe with two columns (path, contents)
  sparklyr::spark_read_text(spark_session, json_bundles_dir, whole = TRUE) %>%
    select(value = contents)
}

xml_bundles_df <- function(spark_session, xml_bundles_dir) {
  # sparklyr::spark_read_text() produces dataframe with two columns (path, contents)
  sparklyr::spark_read_text(spark_session, xml_bundles_dir, whole = TRUE) %>%
    select(value = contents)
}

test_that("encode_json_bundles", {
  def_pathling <- pathling_connect(spark_session())
  json_bundles_df <- json_bundles_df(spark_session(), json_bundles_dir())

  expect_equal(pathling_encode_bundle(def_pathling, json_bundles_df, "Patient") %>% sdf_nrow(), 5)
  expect_equal(
    pathling_encode_bundle(def_pathling, json_bundles_df, "Condition", column = "value") %>% sdf_nrow(), 107
  )
})

test_that("encode_json_resources", {
  def_pathling <- pathling_connect(spark_session())
  json_resources_df <- json_resources_df(spark_session(), json_resources_dir())

  expect_equal(pathling_encode(def_pathling, json_resources_df, "Patient") %>% sdf_nrow(), 9)
  expect_equal(
    pathling_encode(
      def_pathling,
      json_resources_df,
      "Condition",
      input_type = MimeType$FHIR_JSON
    ) %>% sdf_nrow(),
    71
  )
})

test_that("encode_xml_bundles", {
  def_pathling <- pathling_connect(spark_session())
  xml_bundles_df <- xml_bundles_df(spark_session(), xml_bundles_dir())

  expect_equal(
    pathling_encode_bundle(def_pathling, xml_bundles_df, "Patient", input_type = MimeType$FHIR_XML) %>% sdf_nrow(),
    5
  )
  expect_equal(
    pathling_encode_bundle(
      def_pathling,
      xml_bundles_df,
      "Condition",
      input_type = MimeType$FHIR_XML,
      column = "value"
    ) %>% sdf_nrow(),
    107
  )
})

test_that("element_nesting", {
  spark_session <- spark_session()
  json_resources_df <- json_resources_df(spark_session, json_resources_dir())

  pathling_def <- pathling_connect(spark_session)
  pathling_0 <- pathling_connect(spark_session, max_nesting_level = 0)
  pathling_1 <- pathling_connect(spark_session, max_nesting_level = 1)

  # default nesting level is 3
  quest_def <- pathling_encode(pathling_def, json_resources_df, "Questionnaire") %>%
    head(1) %>%
    sdf_collect()
  expect_true("item" %in% names(quest_def))
  expect_true("item" %in% names(quest_def$item[[1]][[1]]))
  expect_true("item" %in% names(quest_def$item[[1]][[1]]$item[[1]]))
  expect_true("item" %in% names(quest_def$item[[1]][[1]]$item[[1]]$item[[1]]))
  expect_false("item" %in% names(quest_def$item[[1]][[1]]$item[[1]]$item[[1]]$item[[1]]))

  # max nesting level set to 0
  quest_0 <- pathling_encode(pathling_0, json_resources_df, "Questionnaire") %>%
    head(1) %>%
    sdf_collect()
  expect_true("item" %in% names(quest_0))
  expect_false("item" %in% names(quest_0$item[[1]][[1]]))

  # max nesting level set to 1
  quest_1 <- pathling_encode(pathling_1, json_resources_df, "Questionnaire") %>%
    head(1) %>%
    sdf_collect()
  expect_true("item" %in% names(quest_1))
  expect_true("item" %in% names(quest_1$item[[1]][[1]]))
  expect_false("item" %in% names(quest_1$item[[1]][[1]]$item[[1]]))
})

test_that("extension_support", {
  spark_session <- spark_session()
  json_resources_df <- json_resources_df(spark_session, json_resources_dir())

  pathling_def <- pathling_connect(spark_session)
  pathling_ext_off <- pathling_connect(spark_session, enable_extensions = FALSE)
  pathling_ext_on <- pathling_connect(spark_session, enable_extensions = TRUE)

  patient_def <- pathling_encode(pathling_def, json_resources_df, "Patient") %>%
    head(1) %>%
    sdf_collect()
  expect_false("_extension" %in% colnames(patient_def))

  # extensions disabled
  patient_off <- pathling_encode(pathling_ext_off, json_resources_df, "Patient") %>% head(1)
  expect_false("_extension" %in% colnames(patient_off))

  # extensions enabled
  patient_on <- pathling_encode(pathling_ext_on, json_resources_df, "Patient") %>% head(1)
  expect_true("_extension" %in% colnames(patient_on))
})
