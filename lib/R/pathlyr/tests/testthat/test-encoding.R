test_that("encodes json resources", {

  sc <- spark_connect(master = "local")

  pc <- ptl_connect(sc)
  json_resources_df <- spark_read_text(sc, path=system.file('data','ndjson', package='pathlyr'))

  patient_count <- pc %>% ptl_encode(json_resources_df, 'Patient') %>% sdf_nrow()
  expect_equal(patient_count, 9)

  condition_count <- pc %>% ptl_encode(json_resources_df, 'Condition',
    input_type=MimeType$FHIR_JSON) %>% sdf_nrow()

  expect_equal(condition_count, 71)
})
