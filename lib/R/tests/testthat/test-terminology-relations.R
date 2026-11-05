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
#
#  Author: John Grimes

# The default context uses the mock terminology service, which holds the value set
# http://snomed.info/sct?fhir_vs=refset/723264001 and the concept map
# http://snomed.info/sct?fhir_cm=100. The supplied resources follow the worked examples of the SQL
# on FHIR "Terminology in SQL" page.

supplied_value_set <- function() {
  jsonlite::toJSON(
    list(
      resourceType = "ValueSet",
      url = "http://example.org/ValueSet/cardiovascular-disease",
      expansion = list(
        timestamp = "2026-09-16T00:00:00Z",
        total = 2,
        contains = list(
          list(system = "http://snomed.info/sct", code = "22298006"),
          list(system = "http://hl7.org/fhir/sid/icd-10", version = "2019", code = "I21")
        )
      )
    ),
    auto_unbox = TRUE
  )
}

supplied_concept_map <- function() {
  jsonlite::toJSON(
    list(
      resourceType = "ConceptMap",
      url = "http://example.org/ConceptMap/sct-to-icd10",
      group = list(list(
        source = "http://snomed.info/sct",
        target = "http://hl7.org/fhir/sid/icd-10|2019",
        element = list(
          list(code = "22298006", target = list(list(code = "I21", equivalence = "equivalent"))),
          list(code = "102499006", target = list(list(equivalence = "unmatched")))
        )
      ))
    ),
    auto_unbox = TRUE
  )
}

test_that("pathling_value_set returns one row per member under the specified columns", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pathling_value_set(pc, "http://snomed.info/sct?fhir_vs=refset/723264001") %>%
    sdf_collect()

  expect_equal(colnames(result), c("system", "version", "code", "display", "inactive"))
  expect_equal(result$system, SNOMED_URI)
  expect_equal(result$code, "368529001")
  expect_true(is.na(result$version))
})

test_that("pathling_value_set fails for an unknown value set", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  expect_error(
    pathling_value_set(pc, "http://example.org/ValueSet/unknown"),
    "could not be resolved"
  )
})

test_that("pathling_value_set_from_json uses the supplied expansion and honours the cap", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pathling_value_set_from_json(pc, supplied_value_set()) %>%
    dplyr::arrange(code) %>%
    sdf_collect()

  expect_equal(result$code, c("22298006", "I21"))
  expect_equal(result$version, c(NA, "2019"))
  expect_error(
    pathling_value_set_from_json(pc, supplied_value_set(), max_members = 1),
    "more than the maximum of 1 members"
  )
})

test_that("pathling_concept_map returns one row per mapping and honours the cap", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pathling_concept_map(pc, "http://snomed.info/sct?fhir_cm=100", max_mappings = 2) %>%
    dplyr::arrange(target_code) %>%
    sdf_collect()

  expect_equal(result$source_code, c("368529001", "368529001"))
  expect_equal(result$target_code, c("368529002", "55916-3"))
  expect_equal(result$relationship, c("equivalent", "related-to"))
  expect_error(
    pathling_concept_map(pc, "http://snomed.info/sct?fhir_cm=100", max_mappings = 1),
    "more than the maximum of 1 mappings"
  )
})

test_that("pathling_concept_map_from_json keeps a no-mapping row", {
  spark <- def_spark()
  pc <- def_pathling_context(spark)

  result <- pathling_concept_map_from_json(pc, supplied_concept_map()) %>%
    dplyr::arrange(source_code) %>%
    sdf_collect()

  # The unmatched element is a row of its own, with the group's target system and version but no
  # target code.
  expect_equal(result$source_code, c("102499006", "22298006"))
  expect_equal(result$target_code, c(NA, "I21"))
  expect_equal(result$target_version, c("2019", "2019"))
})
