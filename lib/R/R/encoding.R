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

library(purrr)

#' Encode FHIR JSON or XML to a dataframe
#'
#' Takes a Spark DataFrame with string representations of FHIR resources in the given column and
#' encodes the resources of the given types as Spark DataFrame.
#'
#' @param pc The Pathling context object.
#' @param df A Spark DataFrame containing the resources to encode.
#' @param resource_name The name of the FHIR resource to extract (e.g., "Condition", "Observation").
#' @param input_type The MIME type of input string encoding. Defaults to "application/fhir+json".
#' @param column The column in which the resources to encode are stored. If set to NULL, the input
#'   DataFrame is assumed to have one column of type string.
#' @return A Spark DataFrame containing the given type of resources encoded into Spark columns.
#'
#' @family encoding functions
#'
#' @importFrom sparklyr sdf_register spark_dataframe j_invoke
#'
#' @export
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' json_resources_df <- pathling_spark(pc) %>%
#'   sparklyr::spark_read_text(path = system.file("extdata", "ndjson", "Condition.ndjson",
#'     package = "pathling"
#'   ))
#' pc %>% pathling_encode(json_resources_df, "Condition")
#' pathling_disconnect(pc)
pathling_encode <- function(pc, df, resource_name, input_type = NULL, column = NULL) {
  sdf_register(j_invoke(
    pc, "encode", spark_dataframe(df), resource_name,
    input_type %||% MimeType$FHIR_JSON, column
  ))
}

#' Encode FHIR Bundles to a dataframe
#'
#' Takes a dataframe with string representations of FHIR bundles in the given column and outputs
#' a dataframe of encoded resources.
#'
#' @param pc A Pathling context object.
#' @param df A Spark DataFrame containing the bundles with the resources to encode.
#' @param resource_name The name of the FHIR resource to extract (Condition, Observation, etc.).
#' @param input_type The MIME type of the input string encoding. Defaults to 'application/fhir+json'.
#' @param column The column in which the resources to encode are stored. If 'NULL', then the
#'   input DataFrame is assumed to have one column of type string.
#'
#' @return A Spark DataFrame containing the given type of resources encoded into Spark columns.
#'
#' @family encoding functions
#'
#' @importFrom sparklyr sdf_register spark_dataframe j_invoke
#'
#' @export
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' json_resources_df <- pathling_spark(pc) %>%
#'   sparklyr::spark_read_text(
#'     path = system.file("extdata", "bundle-xml", package = "pathling"),
#'     whole = TRUE
#'   )
#' pc %>% pathling_encode_bundle(json_resources_df, "Condition",
#'   input_type = MimeType$FHIR_XML, column = "contents"
#' )
#' pathling_disconnect(pc)
pathling_encode_bundle <- function(pc, df, resource_name, input_type = NULL, column = NULL) {
  sdf_register(j_invoke(
    pc, "encodeBundle", spark_dataframe(df), resource_name,
    input_type %||% MimeType$FHIR_JSON, column
  ))
}

#' Annotate resources stored in the Parquet on FHIR layout
#'
#' Annotates resources stored in the Parquet on FHIR layout, as the annotation options of
#' \code{\link{pathling_connect}} say. Encoding annotates what it encodes, so this is for resources
#' stored without annotations, such as Parquet or Delta written by another producer or with
#' different options. Writing resources does not add annotations, and neither does reading them
#' for a query. No kind of annotation is defined in this release, so with the default options the
#' resources are returned unchanged, and with \code{drop_unknown_annotations} the only effect is that
#' fields whose names begin with "__" are left out.
#'
#' @param pc The Pathling context object.
#' @param df A Spark DataFrame of resources of one type, in the Parquet on FHIR layout.
#' @param resource_name The name of the type of the resources.
#'
#' @return A Spark DataFrame containing the resources, annotated.
#'
#' @family encoding functions
#'
#' @importFrom sparklyr sdf_register spark_dataframe j_invoke
#'
#' @export
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' json_resources_df <- pathling_spark(pc) %>%
#'   sparklyr::spark_read_text(path = system.file("extdata", "ndjson", "Condition.ndjson",
#'     package = "pathling"
#'   ))
#' conditions <- pc %>% pathling_encode(json_resources_df, "Condition")
#' pc %>% pathling_annotate(conditions, "Condition")
#' pathling_disconnect(pc)
pathling_annotate <- function(pc, df, resource_name) {
  sdf_register(j_invoke(pc, "annotate", spark_dataframe(df), resource_name))
}
