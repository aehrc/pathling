json_resources_dir <- function() {
  test_path("testdata", "ndjson")
}

test_data_source <- function() {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  pathling_read_ndjson(pc, json_resources_dir())
}

# test_extract
test_that("test datasource extract", {

  # ... previous setup code ...

  result <- ds_extract(
      test_data_source(),
      "Patient",
      columns = list("id", "gender", condition_code = "reverseResolve(Condition.subject).code.coding.code"),
      filters = list("gender = 'male'")
  )

  expected_result <- tibble::tibble(
      id = c("2b36c1e2-bbe1-45ae-8124-4adad2677702", "2b36c1e2-bbe1-45ae-8124-4adad2677702", "2b36c1e2-bbe1-45ae-8124-4adad2677702", "8ee183e2-b3c0-4151-be94-b945d6aa8c6d", "8ee183e2-b3c0-4151-be94-b945d6aa8c6d"),
      gender = c("male", "male", "male", "male", "male"),
      condition_code = c("10509002", "38341003", "65363002", "195662009", "237602007")
  )

  expect_equal(colnames(result), colnames(expected_result))
  expect_equal(result %>%
                   sdf_sort(c("id", "gender", "condition_code")) %>%
                   head(5) %>%
                   sdf_collect(), expected_result)
})


# test_extract_no_filters
test_that("test datasource extract with no filters", {

  # ... previous setup code ...

  result <- ds_extract(
      test_data_source(),
      "Patient",
      columns = c("id", "gender", condition_code = "reverseResolve(Condition.subject).code.coding.code")
  )

  expected_result <- tibble::tibble(
      id = c("121503c8-9564-4b48-9086-a22df717948e", "121503c8-9564-4b48-9086-a22df717948e", "121503c8-9564-4b48-9086-a22df717948e", "121503c8-9564-4b48-9086-a22df717948e", "121503c8-9564-4b48-9086-a22df717948e"),
      gender = c("female", "female", "female", "female", "female"),
      condition_code = c("10509002", "15777000", "195662009", "271737000", "363406005")
  )

  expect_equal(colnames(result), colnames(expected_result))
  expect_equal(result %>%
                   sdf_sort(c("id", "gender", "condition_code")) %>%
                   head(5) %>%
                   sdf_collect(), expected_result)
}):30


# test_aggregate
test_that("test datasource aggregate", {

  # ... previous setup code ...

  agg_result <- ds_aggregate(
      test_data_source(),
      "Patient",
      aggregations = list(
          patient_count = "count()"
      ),
      groupings = list("gender", marital_status_code = "maritalStatus.coding.code"),
      filters = list("birthDate > @1957-06-06")
  )

  expected_result <- tibble::tibble(
      gender = c("male", "male", "female", "female"),
      marital_status_code = c("S", "M", "S", "M"),
      patient_count = c(1, 2, 3, 1)
  )

  expect_equal(colnames(agg_result), colnames(expected_result))
  expect_equal(agg_result %>% sdf_collect(), expected_result)
})


# test_aggregate_no_filter
test_that("test datasource aggregate with no filters", {

  # ... previous setup code ...

  agg_result <- ds_aggregate(
      test_data_source(),
      "Patient",
      aggregations = c(
          patient_count = "count()"
      ),
      groupings = c(
          gender = "gender",
          marital_status_code = "maritalStatus.coding.code"
      )
  )

  expected_result <- tibble::tibble(
      gender = c("male", "male", "female", "female"),
      marital_status_code = c("S", "M", "S", "M"),
      patient_count = c(3, 2, 3, 1)
  )

  expect_equal(colnames(agg_result), colnames(expected_result))
  expect_equal(agg_result %>% sdf_collect(), expected_result)
})


# test_many_aggregate_no_grouping
test_that("test datasource aggregate with no grouping", {

  # ... previous setup code ...

  ResultRow <- tibble::tibble(
      patient_count = 9,
      id_count = 9
  )

  agg_result <- ds_aggregate(
      test_data_source(),
      "Patient",
      aggregations = c(
          patient_count = "count()",
          id_count = "id.count()"
      )
  )

  expect_equal(colnames(agg_result), colnames(ResultRow))
  expect_equal(agg_result %>% sdf_collect(), ResultRow)
})


SOD_VIEW_JSON <- paste(
    '{',
    '  "resource": "Patient",',
    '  "select": [',
    '    {',
    '      "column": [',
    '        {',
    '          "path": "gender",',
    '          "name": "gender"',
    '        }',
    '      ]',
    '    },',
    '    {',
    '      "forEachOrNull": "name",',
    '      "column": [',
    '        {',
    '          "path": "given",',
    '          "name": "given_name"',
    '        }',
    '      ]',
    '    }',
    '  ],',
    '  "where": [',
    '    {',
    '      "path": "gender = \'male\'"',
    '    }',
    '  ]',
    '}',
    sep = "\n"
)


# test SOF view with json
test_that("test SOF view with json", {

  # expectations
  ResultRow <- tibble::tibble(
      gender = c('male', 'male', 'male', 'male', 'male'),
      given_name = c('Seymour882', 'Guy979', 'Pedro316', 'Gilberto712', 'Shirley182')
  )

  # actuals
  sof_result <- ds_view(
      test_data_source(),
      "Patient",
      json = SOD_VIEW_JSON
  )

  expect_equal(colnames(sof_result), colnames(ResultRow))
  expect_equal(sof_result %>% sdf_collect(), ResultRow)
})


# test SOF view with json
test_that("test SOF view with components", {

  # expectations
  ResultRow <- tibble::tibble(
      gender = c('female', 'female', 'female', 'female', 'female'),
      given_name = c('Su690', 'Ophelia894', 'Karina848', 'Karina848', 'Cherryl901')
  )

  # actuals
  sof_result <- ds_view(
      test_data_source(),
      resource = "Patient",
      select = list(
          list(
              column = list(
                  list(
                      path = "gender",
                      name = "gender"
                  )
              )
          ),
          list(
              forEachOrNull = "name",
              column = list(
                  list(
                      path = "given",
                      name = "given_name"
                  )
              )
          )
      ),
      where = list(
          list(
              path = "gender = 'female'"
          )
      )
  )
  expect_equal(colnames(sof_result), colnames(ResultRow))
  expect_equal(sof_result %>% sdf_collect(), ResultRow)
})


