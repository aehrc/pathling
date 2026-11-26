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

# Tests the schema equality of spec SC-002 (T011).

# The helpers read the annotation prefix from the JVM of an open Spark connection.
def_spark()

sf <- function(name, type, nullable = TRUE, metadata = "{}") {
  sprintf(
    '{"name":"%s","type":%s,"nullable":%s,"metadata":%s}',
    name, type, tolower(as.character(nullable)), metadata
  )
}

st <- function(...) {
  sprintf('{"type":"struct","fields":[%s]}', paste(c(...), collapse = ","))
}

arr <- function(element, contains_null = TRUE) {
  sprintf(
    '{"type":"array","elementType":%s,"containsNull":%s}',
    element, tolower(as.character(contains_null))
  )
}

map_of <- function(value, value_contains_null = TRUE) {
  sprintf(
    '{"type":"map","keyType":"string","valueType":%s,"valueContainsNull":%s}',
    value, tolower(as.character(value_contains_null))
  )
}

# The test schema, with every field, array element and map value nullable or not, and metadata on
# the fields where not.
test_schema <- function(nullable = TRUE) {
  metadata <- if (nullable) "{}" else '{"comment":"x"}'
  name <- st(
    sf("family", '"string"', nullable, metadata),
    sf("given", arr('"string"', nullable), nullable)
  )
  code <- st(sf("code", '"string"', nullable, metadata))
  st(
    sf("id", '"string"', nullable, metadata),
    sf("name", arr(name, nullable), nullable),
    sf("tags", map_of(code, nullable), nullable)
  )
}

schema <- test_schema()

test_that("nullability and metadata are ignored at every depth", {
  expect_true(same_schema(schema, schema))
  expect_true(same_schema(schema, test_schema(nullable = FALSE)))
  expect_true(same_schema(test_schema(nullable = FALSE), schema))
})

test_that("annotation fields are ignored", {
  annotated <- st(
    sf("id", '"string"'),
    sf("__first", '"decimal(38,6)"'),
    sf("name", arr(st(
      sf("family", '"string"'), sf("given", arr('"string"')), sf("__flag", '"boolean"')
    ))),
    sf("tags", map_of(st(sf("code", '"string"'), sf("__canonical_code", '"string"')))),
    sf("__custom", '"string"')
  )
  expect_true(same_schema(schema, annotated))
  expect_true(same_schema(annotated, schema))
})

test_that("a group left with only annotation fields is dropped", {
  tags <- sf("tags", map_of(st(sf("code", '"string"'))))
  grouped <- st(
    sf("id", '"string"'),
    sf("_id", st(sf("__start", '"timestamp"'), sf("__end", '"timestamp"'))),
    sf("name", arr(st(
      sf("family", '"string"'), sf("given", arr('"string"')),
      sf("_given", arr(st(sf("__flag", '"boolean"'))))
    ))),
    tags
  )
  with_base_field <- st(
    sf("id", '"string"'),
    sf("_id", st(sf("id", '"string"'), sf("__start", '"timestamp"'))),
    sf("name", arr(st(sf("family", '"string"'), sf("given", arr('"string"'))))),
    tags
  )
  expect_true(same_schema(schema, grouped))
  expect_true(same_schema(grouped, schema))
  expect_false(same_schema(schema, with_base_field))
  expect_false(same_schema(with_base_field, schema))
})

test_that("a group left with only annotation fields is dropped at any depth", {
  rest <- c(
    sf("name", arr(st(sf("family", '"string"'), sf("given", arr('"string"'))))),
    sf("tags", map_of(st(sf("code", '"string"'))))
  )
  nested <- st(
    sf("id", '"string"'),
    sf("_id", st(
      sf("_inner", arr(st(sf("__flag", '"boolean"')))), sf("__start", '"timestamp"')
    )),
    rest
  )
  nested_with_base_field <- st(
    sf("id", '"string"'),
    sf("_id", st(sf("_inner", st(sf("id", '"string"'), sf("__flag", '"boolean"'))))),
    rest
  )
  expect_true(same_schema(schema, nested))
  expect_true(same_schema(nested, schema))
  expect_false(same_schema(schema, nested_with_base_field))
  expect_false(same_schema(nested_with_base_field, schema))
})

test_that("a missing or extra field makes schemas differ", {
  missing <- st(
    sf("id", '"string"'),
    sf("name", arr(st(sf("family", '"string"')))),
    sf("tags", map_of(st(sf("code", '"string"'))))
  )
  extra <- st(
    sf("id", '"string"'),
    sf("name", arr(st(sf("family", '"string"'), sf("given", arr('"string"'))))),
    sf("tags", map_of(st(sf("code", '"string"'), sf("system", '"string"'))))
  )
  expect_false(same_schema(schema, missing))
  expect_false(same_schema(missing, schema))
  expect_false(same_schema(schema, extra))
  expect_false(same_schema(extra, schema))
})

test_that("a renamed or retyped field makes schemas differ", {
  renamed <- st(
    sf("id", '"string"'),
    sf("name", arr(st(sf("family", '"string"'), sf("givenName", arr('"string"'))))),
    sf("tags", map_of(st(sf("code", '"string"'))))
  )
  retyped <- st(
    sf("id", '"string"'),
    sf("name", arr(st(sf("family", '"string"'), sf("given", '"string"')))),
    sf("tags", map_of(st(sf("code", '"string"'))))
  )
  retyped_leaf <- st(
    sf("id", '"string"'),
    sf("name", arr(st(sf("family", '"string"'), sf("given", arr('"string"'))))),
    sf("tags", map_of(st(sf("code", '"integer"'))))
  )
  expect_false(same_schema(schema, renamed))
  expect_false(same_schema(schema, retyped))
  expect_false(same_schema(schema, retyped_leaf))
})

test_that("a different field order makes schemas differ", {
  name <- arr(st(sf("family", '"string"'), sf("given", arr('"string"'))))
  tags <- map_of(st(sf("code", '"string"')))
  top_level <- st(sf("name", name), sf("id", '"string"'), sf("tags", tags))
  nested <- st(
    sf("id", '"string"'),
    sf("name", arr(st(sf("given", arr('"string"')), sf("family", '"string"')))),
    sf("tags", tags)
  )
  expect_false(same_schema(schema, top_level))
  expect_false(same_schema(schema, nested))
})

test_that("schema sets are the same when their types and schemas are", {
  observation <- st(sf("id", '"string"'), sf("status", '"string"'))
  expect_true(same_schema_set(
    list(Patient = schema, Observation = observation),
    list(Patient = test_schema(nullable = FALSE), Observation = observation)
  ))
  expect_false(same_schema_set(
    list(Patient = schema, Observation = observation), list(Patient = schema)
  ))
  expect_false(same_schema_set(list(Patient = schema), list(Patient = observation)))
  expect_false(same_schema_set(list(Patient = schema), list(Condition = schema)))
})

test_that("a schema set that does not name every schema is refused", {
  expect_error(same_schema_set(list(schema), list(schema)), "must name")
  expect_error(same_schema_set(list(Patient = schema), list(schema)), "must name")
  expect_error(
    same_schema_set(list(Patient = schema, schema), list(Patient = schema, schema)), "must name"
  )
})
