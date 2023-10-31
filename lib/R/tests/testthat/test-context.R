test_that("creates a default pathling context", {
  pc <- pathling_connect()

  expect_s3_class(pc, "spark_jobj")
})
