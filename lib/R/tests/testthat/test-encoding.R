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

# Documents of several types, each one per line, as encode reads them. The Bundle is another type
# and is left out, as the Observation is, when Patients are requested.
mixed_json <- c(
  '{"resourceType":"Patient","id":"p1","gender":"male"}',
  '{"resourceType":"Observation","id":"o1","status":"final","code":{"text":"x"}}',
  '{"resourceType":"Patient","id":"p2","gender":"female"}',
  paste0(
    '{"resourceType":"Bundle","id":"b1","type":"collection","entry":[{"resource":',
    '{"resourceType":"Patient","id":"p3"}}]}'
  )
)

mixed_xml <- c(
  '<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><gender value="male"/></Patient>',
  paste0(
    '<Observation xmlns="http://hl7.org/fhir"><id value="o1"/><status value="final"/>',
    '<code><text value="x"/></code></Observation>'
  ),
  '<Patient xmlns="http://hl7.org/fhir"><id value="p2"/><gender value="female"/></Patient>',
  paste0(
    '<Bundle xmlns="http://hl7.org/fhir"><id value="b1"/><type value="collection"/>',
    '<entry><resource><Patient><id value="p3"/></Patient></resource></entry></Bundle>'
  )
)

encoded_ids <- function(pc, documents, resource_type, input_type) {
  pathling_encode(pc, documents, resource_type, input_type = input_type) %>%
    select(id) %>%
    collect() %>%
    pull(id) %>%
    sort()
}

test_that("encode keeps only the requested type", {
  # Only the documents of the requested type are stored, as before the switch (T070, SC-008).
  spark <- spark_session()
  pc <- pathling_connect(spark)
  for (format in list(list(mixed_json, MimeType$FHIR_JSON), list(mixed_xml, MimeType$FHIR_XML))) {
    documents <- sparklyr::copy_to(
      spark, data.frame(value = format[[1]]),
      name = paste0("mixed_", sample.int(1e6, 1)), overwrite = TRUE
    )
    expect_equal(encoded_ids(pc, documents, "Patient", format[[2]]), c("p1", "p2"))
    expect_equal(encoded_ids(pc, documents, "Observation", format[[2]]), "o1")
    # A type that is absent from the input gives an empty result, not a failure.
    expect_equal(
      pathling_encode(pc, documents, "Condition", input_type = format[[2]]) %>% sdf_nrow(), 0
    )
  }
})

# XML is read and written without HAPI on the runtime jar (ticket 32): through Spark's XML reader and
# writer, with a narrative held to L2 and the references between a bundle's entries resolved.
narrative_xml <- paste0(
  '<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><text><status value="generated"/>',
  '<div xmlns="http://www.w3.org/1999/xhtml"><p>Line one&#xa;line two &amp; more</p></div>',
  '</text><gender value="male"/></Patient>'
)

transaction_xml <- paste0(
  '<Bundle xmlns="http://hl7.org/fhir"><type value="transaction"/>',
  '<entry><fullUrl value="urn:uuid:p1"/><resource><Patient><id value="p1"/></Patient>',
  "</resource></entry>",
  '<entry><fullUrl value="urn:uuid:o1"/><resource><Observation><id value="o1"/>',
  '<status value="final"/><code><text value="x"/></code>',
  '<subject><reference value="urn:uuid:p1"/></subject></Observation></resource></entry>',
  "</Bundle>"
)

test_that("XML is read and written without HAPI", {
  spark <- spark_session()
  pc <- pathling_connect(spark)
  documents <- sparklyr::copy_to(
    spark, data.frame(value = narrative_xml),
    name = paste0("narrative_", sample.int(1e6, 1)), overwrite = TRUE
  )
  encoded <- pathling_encode(pc, documents, "Patient", input_type = MimeType$FHIR_XML)
  decoded <- sparklyr::invoke(
    pc, "decode", sparklyr::spark_dataframe(encoded), "Patient", MimeType$FHIR_XML
  ) %>%
    sparklyr::invoke("toDF") %>%
    sparklyr::sdf_register() %>%
    collect()
  decoded <- decoded[[1]]
  # The line feed in the narrative is kept, and so the document is written as it was read.
  expect_equal(decoded, gsub("&#xa;", "\n", narrative_xml, fixed = TRUE))

  bundles <- sparklyr::copy_to(
    spark, data.frame(value = transaction_xml),
    name = paste0("transaction_", sample.int(1e6, 1)), overwrite = TRUE
  )
  view <- paste0("observations_", sample.int(1e6, 1))
  pathling_encode_bundle(pc, bundles, "Observation", input_type = MimeType$FHIR_XML) %>%
    sparklyr::sdf_register(view)
  subjects <- DBI::dbGetQuery(spark, paste0("SELECT subject.reference AS reference FROM ", view))
  expect_equal(subjects$reference, "Patient/p1")
})

# The nesting and extension options bound the schema of the dense mode only, which does not exist
# yet, so they bound nothing in the fitted schema that encoding stores: depth comes from the data,
# and extensions are stored inline because they are present (contracts/library-api.md, decision
# 69). The options are still accepted.

test_that("element_nesting", {
  spark_session <- spark_session()
  json_resources_df <- json_resources_df(spark_session, json_resources_dir())

  pathling_def <- pathling_connect(spark_session)
  pathling_0 <- pathling_connect(spark_session, max_nesting_level = 0)
  pathling_1 <- pathling_connect(spark_session, max_nesting_level = 1)

  # The levels of items the data carries are kept, whatever the nesting level.
  quest_def_df <- pathling_encode(pathling_def, json_resources_df, "Questionnaire")
  quest_def <- quest_def_df %>%
    head(1) %>%
    sdf_collect()
  expect_true("item" %in% names(quest_def))
  expect_true("item" %in% names(quest_def$item[[1]][[1]]))
  expect_true("item" %in% names(quest_def$item[[1]][[1]]$item[[1]]))
  expect_true("item" %in% names(quest_def$item[[1]][[1]]$item[[1]]$item[[1]]))

  # The options bound nothing: the schema is the one the defaults give.
  for (pathling in list(pathling_0, pathling_1)) {
    quest <- pathling_encode(pathling, json_resources_df, "Questionnaire")
    expect_equal(sdf_schema(quest), sdf_schema(quest_def_df))
    expect_equal(quest %>% head(1) %>% sdf_collect(), quest_def)
  }
})

test_that("extension_support", {
  spark_session <- spark_session()
  json_resources_df <- json_resources_df(spark_session, json_resources_dir())

  pathling_def <- pathling_connect(spark_session)
  pathling_ext_off <- pathling_connect(spark_session, enable_extensions = FALSE)
  pathling_ext_on <- pathling_connect(spark_session, enable_extensions = TRUE)

  # The extensions are stored inline, and there is no extension map.
  patient_def <- pathling_encode(pathling_def, json_resources_df, "Patient")
  expect_false("_extension" %in% colnames(patient_def))
  expect_true("extension" %in% colnames(patient_def))

  for (pathling in list(pathling_ext_off, pathling_ext_on)) {
    patient <- pathling_encode(pathling, json_resources_df, "Patient")
    expect_equal(sdf_schema(patient), sdf_schema(patient_def))
    expect_false("_extension" %in% colnames(patient))
  }
})
