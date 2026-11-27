#  Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Tests the schema set file functions of the R API, which delegate to the Java SchemaSets (T062,
# FR-028).

spark <- def_spark()

# The JSON form of a struct type, as Spark writes it, from a JSON text of it.
struct_json <- function(json) {
  spark %>%
    j_invoke_static("org.apache.spark.sql.types.DataType", "fromJson", json) %>%
    j_invoke("json")
}

# A set out of canonical order, with non-nullable fields and with empty and nested field metadata.
schema_set <- function() {
  list(
    Patient = struct_json(paste0(
      '{"type":"struct","fields":[',
      '{"name":"id","type":"string","nullable":false,"metadata":{}},',
      '{"name":"resourceType","type":"string","nullable":true,"metadata":{}},',
      '{"name":"name","type":{"type":"array","elementType":{"type":"struct","fields":[',
      '{"name":"given","type":{"type":"array","elementType":"string","containsNull":false},',
      '"nullable":true,"metadata":{"comment":"the given name","source":{"version":2}}},',
      '{"name":"family","type":"string","nullable":false,"metadata":{}}]},',
      '"containsNull":false},"nullable":true,"metadata":{}}]}'
    )),
    Condition = struct_json(paste0(
      '{"type":"struct","fields":[',
      '{"name":"resourceType","type":"string","nullable":true,"metadata":{}},',
      '{"name":"id","type":"string","nullable":true,"metadata":{}},',
      '{"name":"_recordedDate","type":{"type":"struct","fields":[',
      '{"name":"__start","type":"timestamp","nullable":true,"metadata":{}},',
      '{"name":"__end","type":"timestamp","nullable":true,"metadata":{}}]},',
      '"nullable":true,"metadata":{}}]}'
    ))
  )
}

# The content the Java SchemaSets.toJson gives for a schema set.
java_to_json <- function(schemas) {
  java_set <- spark %>% j_invoke_new("java.util.LinkedHashMap")
  for (type in names(schemas)) {
    java_set %>% j_invoke(
      "put", type,
      j_invoke_static(spark, "org.apache.spark.sql.types.DataType", "fromJson", schemas[[type]])
    )
  }
  spark %>% j_invoke_static("au.csiro.pathling.library.SchemaSets", "toJson", java_set)
}

parse_json <- function(json) jsonlite::fromJSON(json, simplifyVector = FALSE)

test_that("a set written and read back is equal to it", {
  path <- tempfile(fileext = ".json")

  pathling_write_schemas(spark, schema_set(), path)

  expect_equal(pathling_read_schemas(spark, path), schema_set())
})

test_that("a schema with empty and nested field metadata survives a round trip unchanged", {
  path <- tempfile(fileext = ".json")
  schemas <- schema_set()["Patient"]

  pathling_write_schemas(spark, schemas, path)
  read_back <- pathling_read_schemas(spark, path)

  expect_identical(parse_json(read_back$Patient), parse_json(schemas$Patient))
  given <- parse_json(read_back$Patient)$fields[[3]]$type$elementType$fields[[1]]
  expect_equal(given$metadata, list(comment = "the given name", source = list(version = 2L)))
  expect_length(parse_json(read_back$Patient)$fields[[1]]$metadata, 0)
})

test_that("the file written parses to the same JSON object as SchemaSets.toJson", {
  path <- tempfile(fileext = ".json")

  pathling_write_schemas(spark, schema_set(), path)

  expect_identical(
    parse_json(paste(readLines(path, warn = FALSE), collapse = "\n")),
    parse_json(java_to_json(schema_set()))
  )
})

test_that("a file written from SchemaSets.toJson is read to the same set", {
  path <- tempfile(fileext = ".json")
  writeLines(java_to_json(schema_set()), path)

  expect_equal(pathling_read_schemas(spark, path), schema_set())
})

test_that("a malformed file is refused with an error naming the file", {
  for (content in c("", "not json", "[]", '{"Patient": ')) {
    path <- tempfile(fileext = ".json")
    writeLines(content, path)

    expect_error(pathling_read_schemas(spark, path), basename(path), fixed = TRUE)
  }
})

test_that("a value that is not a struct is refused with an error naming its key", {
  values <- c(
    '"string"',
    "1",
    '{"type":"array","elementType":"string","containsNull":true}'
  )
  for (value in values) {
    path <- tempfile(fileext = ".json")
    writeLines(paste0('{"Condition": ', value, "}"), path)

    expect_error(pathling_read_schemas(spark, path), "Condition")
  }
})
