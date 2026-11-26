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

# Tests the sources of the R API read with a schema set, given as a named list of strings, each a
# struct type's JSON form (T022, FR-001, FR-004).

# Eleven resource types of the bundles test data, one more than the ten that an earlier binding
# could pass.
eleven_types <- c(
  "CarePlan", "CareTeam", "Claim", "Condition", "DiagnosticReport", "Encounter",
  "Immunization", "MedicationRequest", "Observation", "Patient", "Procedure"
)

# The JSON form of the schema of a Spark data frame.
schema_json <- function(sdf) {
  sdf %>%
    sparklyr::spark_dataframe() %>%
    sparklyr::invoke("schema") %>%
    sparklyr::invoke("json")
}

# The JSON form of a schema of the first three base columns of a data frame, in reverse order, so
# that it is narrower than the data and in another field order.
narrow_schema <- function(sdf) {
  prefix <- annotation_prefix()
  base <- Filter(function(column) !startsWith(column, prefix), colnames(sdf))
  sdf %>%
    select(all_of(rev(head(base, 3)))) %>%
    schema_json()
}

# The schema of each of the given types, as the data source reads it, as a named list.
schemas_of <- function(ds, types) {
  stats::setNames(lapply(types, function(type) {
    ds %>%
      ds_read(type) %>%
      schema_json()
  }), types)
}

# Expects each of the given types to have the same rows in its top-level primitive columns, other
# than annotations, in a data source as in another, and to have at least one row.
expect_same_primitive_rows <- function(ds, expected_ds, types) {
  prefix <- annotation_prefix()
  for (type in types) {
    sdf <- ds %>% ds_read(type)
    columns <- Filter(function(column) {
      !startsWith(column$name, prefix) && !grepl("^(Struct|Array|Map)", column$type)
    }, sparklyr::sdf_schema(sdf))
    rows <- function(data) {
      data %>%
        select(all_of(names(columns))) %>%
        collect() %>%
        arrange(across(everything()))
    }
    actual <- rows(sdf)

    expect_gt(nrow(actual), 0)
    expect_equal(actual, rows(expected_ds %>% ds_read(type)))
  }
}

# The NDJSON test data read without a schema set.
inferred_source <- function(pc) {
  pc %>% pathling_read_ndjson(test_path("testdata", "ndjson"))
}

# A schema set for Patient and Condition narrower than the data and in another order.
defined_schemas <- function(pc) {
  inferred <- inferred_source(pc)
  types <- c("Patient", "Condition")
  stats::setNames(lapply(types, function(type) {
    inferred %>%
      ds_read(type) %>%
      narrow_schema()
  }), types)
}

# Opens a source of the given kind over the Patient and Condition test data.
open_source <- function(kind, pc, schemas, database = "default") {
  switch(kind,
    ndjson = pathling_read_ndjson(pc, test_path("testdata", "ndjson"), schemas = schemas),
    bundles = pathling_read_bundles(
      pc, test_path("testdata", "bundles"), c("Patient", "Condition"),
      schemas = schemas
    ),
    datasets = {
      inferred <- inferred_source(pc)
      pathling_read_datasets(
        pc,
        list(Patient = inferred %>% ds_read("Patient"), Condition = inferred %>% ds_read("Condition")),
        schemas = schemas
      )
    },
    parquet = pathling_read_parquet(pc, test_path("testdata", "parquet"), schemas = schemas),
    delta = pathling_read_delta(pc, test_path("testdata", "delta"), schemas = schemas),
    tables = pathling_read_tables(pc, schema = database, schemas = schemas),
    stop("Unknown source kind: ", kind)
  )
}

sources <- c("ndjson", "bundles", "datasets", "parquet", "delta", "tables")

# The tables source reads from a database of its own holding the NDJSON test data.
spark <- def_spark()
pc <- def_pathling_context(spark)
defined <- defined_schemas(pc)
database <- paste0("defined_", as.integer(runif(1, 0, 1e9)))
spark %>% sdf_sql(paste("CREATE DATABASE", database))
inferred_source(pc) %>% ds_write_tables(schema = database)

for (kind in sources) {
  test_that(paste("the", kind, "source reads each type with the schema the set gives it"), {
    ds <- open_source(kind, pc, defined, database)

    expect_true(same_schema_set(schemas_of(ds, names(defined)), defined))
    # The projection keeps every row and its values, as the source read without a schema set has
    # them.
    expect_same_primitive_rows(ds, open_source(kind, pc, NULL, database), names(defined))
  })
}

test_that("a set of more than ten types is accepted", {
  bundles_dir <- test_path("testdata", "bundles")
  bundles <- pc %>% pathling_read_bundles(bundles_dir, eleven_types)
  schemas <- stats::setNames(
    lapply(eleven_types, function(type) {
      bundles %>%
        ds_read(type) %>%
        narrow_schema()
    }),
    eleven_types
  )

  ds <- pc %>% pathling_read_bundles(bundles_dir, eleven_types, schemas = schemas)

  expect_true(same_schema_set(schemas_of(ds, eleven_types), schemas))
  expect_same_primitive_rows(ds, bundles, eleven_types)
})

for (kind in sources) {
  test_that(paste("the", kind, "source refuses a string that is not a struct type's JSON form"), {
    values <- list(
      array = '{"type":"array","elementType":"string","containsNull":true}',
      malformed = "not a schema"
    )

    for (value in values) {
      schemas <- list(Patient = defined$Patient, Condition = value)

      expect_error(open_source(kind, pc, schemas), "Condition")
    }
  })
}
