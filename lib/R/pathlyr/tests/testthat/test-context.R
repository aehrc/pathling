test_that("creates a default pathling context", {
  sc <- spark_connect(master = "local")
  pc <- ptl_connect(sc)

  expect_s3_class(pc, "spark_jobj")
})
