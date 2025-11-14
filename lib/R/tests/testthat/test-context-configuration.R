#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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


test_that("default configurations are correct", {
  # Create PathlingContext with all defaults
  pc <- pathling_connect()
  spark <- pathling_spark(pc)

  # Retrieve EncodingConfiguration
  encoding_config <- pc %>% j_invoke("getEncodingConfiguration")

  # Verify default encoding configuration values
  expect_equal(encoding_config %>% j_invoke("getMaxNestingLevel"), 3L)
  expect_equal(encoding_config %>% j_invoke("isEnableExtensions"), FALSE)

  # Verify default open types match STANDARD_OPEN_TYPES
  open_types <- encoding_config %>%
    j_invoke("getOpenTypes") %>%
    as.character()
  expected_types <- c(
    "boolean", "code", "date", "dateTime", "decimal", "integer",
    "string", "Coding", "CodeableConcept", "Address", "Identifier", "Reference"
  )
  expect_setequal(open_types, expected_types)

  # Retrieve QueryConfiguration
  query_config <- pc %>% j_invoke("getQueryConfiguration")

  # Verify default query configuration values
  expect_equal(query_config %>% j_invoke("isExplainQueries"), FALSE)
  expect_equal(query_config %>% j_invoke("getMaxUnboundTraversalDepth"), 10L)

  # Clean up
  pathling_disconnect(pc)
})


test_that("custom configurations round trip correctly", {
  # Create PathlingContext with all non-default parameters
  pc <- pathling_connect(
    max_nesting_level = 5,
    enable_extensions = TRUE,
    enabled_open_types = c("string", "boolean"),
    explain_queries = TRUE,
    max_unbound_traversal_depth = 20
  )
  spark <- pathling_spark(pc)

  # Retrieve EncodingConfiguration
  encoding_config <- pc %>% j_invoke("getEncodingConfiguration")

  # Verify custom encoding configuration values
  expect_equal(encoding_config %>% j_invoke("getMaxNestingLevel"), 5L)
  expect_equal(encoding_config %>% j_invoke("isEnableExtensions"), TRUE)

  # Verify custom open types
  open_types <- encoding_config %>%
    j_invoke("getOpenTypes") %>%
    as.character()
  expect_setequal(open_types, c("string", "boolean"))

  # Retrieve QueryConfiguration
  query_config <- pc %>% j_invoke("getQueryConfiguration")

  # Verify custom query configuration values
  expect_equal(query_config %>% j_invoke("isExplainQueries"), TRUE)
  expect_equal(query_config %>% j_invoke("getMaxUnboundTraversalDepth"), 20L)

  # Clean up
  pathling_disconnect(pc)
})
