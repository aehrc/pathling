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

json_bundles_dir <- function() {
  test_path("data", "bundles", "R4", "json")
}

json_resources_dir <- function() {
  test_path("data", "resources", "R4", "json")
}

xml_bundles_dir <- function() {
  test_path("data", "bundles", "R4", "xml")
}

def_pathling <- function(spark_session) {
  ptl_connect(spark_session)
}

json_resources_df <- function(spark_session, json_resources_dir) {
  sparklyr::spark_read_text(spark_session, json_resources_dir)
}

json_bundles_df <- function(spark_session, json_bundles_dir) {
  # sparklyr::spark_read_text() produces dataframe with two columns (path, contents)
  sparklyr::spark_read_text(spark_session, json_bundles_dir, whole = TRUE) %>%
    select(value=contents)
}

xml_bundles_df <- function(spark_session, xml_bundles_dir) {
  # sparklyr::spark_read_text() produces dataframe with two columns (path, contents)
  sparklyr::spark_read_text(spark_session, xml_bundles_dir, whole = TRUE) %>%
    select(value=contents)
}

test_that("encode_json_bundles", {
  def_pathling <- ptl_connect(spark_session())
  json_bundles_df <- json_bundles_df(spark_session(), json_bundles_dir())

  expect_equal(ptl_encode_bundle(def_pathling, json_bundles_df, "Patient") %>% sdf_nrow(), 5)
  expect_equal(
    ptl_encode_bundle(def_pathling, json_bundles_df, "Condition", column = "value") %>% sdf_nrow(), 107
  )
})

test_that("encode_json_resources", {
  def_pathling <- ptl_connect(spark_session())
  json_resources_df <- json_resources_df(spark_session(), json_resources_dir())

  expect_equal(ptl_encode(def_pathling, json_resources_df, "Patient") %>% sdf_nrow(), 9)
  expect_equal(
    ptl_encode(
      def_pathling,
      json_resources_df,
      "Condition",
      input_type = MimeType$FHIR_JSON
    ) %>% sdf_nrow(),
    71
  )
})

test_that("encode_xml_bundles", {
  def_pathling <- ptl_connect(spark_session())
  xml_bundles_df <- xml_bundles_df(spark_session(), xml_bundles_dir())

  expect_equal(
    ptl_encode_bundle(def_pathling, xml_bundles_df, "Patient", input_type = MimeType$FHIR_XML) %>% sdf_nrow(),
    5
  )
  expect_equal(
    ptl_encode_bundle(
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

  ptl_def <- ptl_connect(spark_session)
  ptl_0 <- ptl_connect(spark_session, max_nesting_level = 0)
  ptl_1 <- ptl_connect(spark_session, max_nesting_level = 1)

  # default nesting level is 3
  quest_def <- ptl_encode(ptl_def, json_resources_df, "Questionnaire") %>% head(1) %>% sdf_collect()
  expect_true(
    "item" %in% names(quest_def) &&
    "item" %in% names(quest_def$item[[1]][[1]]) &&
    "item" %in% names(quest_def$item[[1]][[1]]$item[[1]]) &&
    "item" %in% names(quest_def$item[[1]][[1]]$item[[1]]$item[[1]]) &&
    !("item" %in% names(quest_def$item[[1]][[1]]$item[[1]]$item[[1]]$item[[1]]))
  )

  # max nesting level set to 0
  quest_0 <- ptl_encode(ptl_0, json_resources_df, "Questionnaire") %>% head(1)
  expect_true("item" %in% names(quest_0) && is.null(quest_0$item[[1]]))

  # max nesting level set to 1
  quest_1 <- ptl_encode(ptl_1, json_resources_df, "Questionnaire") %>% head(1) %>% sdf_collect()
  expect_true(
    "item" %in% names(quest_1) &&
    "item" %in% names(quest_1$item[[1]][[1]]) &&
    !("item" %in% names(quest_1$item[[1]][[1]]$item[[1]]))
  )
})

test_that("extension_support", {
  spark_session <- spark_session()
  json_resources_df <- json_resources_df(spark_session, json_resources_dir())

  ptl_def <- ptl_connect(spark_session)
  ptl_ext_off <- ptl_connect(spark_session, enable_extensions = FALSE)
  ptl_ext_on <- ptl_connect(spark_session, enable_extensions = TRUE)

  patient_def <- ptl_encode(ptl_def, json_resources_df, "Patient") %>% head(1) %>% sdf_collect()
  expect_false("_extension" %in% names(patient_def))

  # extensions disabled
  patient_off <- ptl_encode(ptl_ext_off, json_resources_df, "Patient") %>% head(1)
  expect_false("_extension" %in% names(patient_off))

  # extensions enabled
  patient_on <- ptl_encode(ptl_ext_on, json_resources_df, "Patient") %>% head(1)
  expect_true("_extension" %in% names(patient_on))
})
