json_resources_dir <- function() {
  test_path("testdata", "ndjson")
}

test_data_source <- function() {
  spark <- def_spark()
  pc <- def_pathling_context(spark)
  pathling_read_ndjson(pc, json_resources_dir())
}

SOD_VIEW_JSON <- paste(
  "{",
  '  "resource": "Patient",',
  '  "select": [',
  "    {",
  '      "column": [',
  "        {",
  '          "path": "gender",',
  '          "name": "gender"',
  "        }",
  "      ]",
  "    },",
  "    {",
  '      "forEachOrNull": "name",',
  '      "column": [',
  "        {",
  '          "path": "given",',
  '          "name": "given_name"',
  "        }",
  "      ]",
  "    }",
  "  ],",
  '  "where": [',
  "    {",
  '      "path": "gender = \'male\'"',
  "    }",
  "  ]",
  "}",
  sep = "\n"
)


# test SOF view with json
test_that("test SOF view with json", {
  # expectations
  ResultRow <- tibble::tibble(
    gender = c("male", "male", "male", "male", "male"),
    given_name = c("Seymour882", "Guy979", "Pedro316", "Gilberto712", "Shirley182")
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
    gender = c("female", "female", "female", "female", "female"),
    given_name = c("Su690", "Ophelia894", "Karina848", "Karina848", "Cherryl901")
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
