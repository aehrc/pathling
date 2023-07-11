#  Copyright 2023 Commonwealth Scientific and Industrial Research
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


#' Encode FHIR Resources
#'
#' Takes a Spark DataFrame with string representations of FHIR resources in the given column and
#' encodes the resources of the given types as Spark DataFrame.
#'
#' @param pc The PathlingContext object.
#' @param df A Spark DataFrame containing the resources to encode.
#' @param resource_name The name of the FHIR resource to extract (e.g., "Condition", "Observation").
#' @param input_type The mime type of input string encoding. Defaults to "application/fhir+json".
#' @param column The column in which the resources to encode are stored. If set to NULL, the input
#'   DataFrame is assumed to have one column of type string.
#' @return A Spark DataFrame containing the given type of resources encoded into Spark columns.
#'
#' @import rlang
#' @import sparklyr
#'
#'@export
ptl_encode <- function(pc, df, resource_name, input_type = NULL, column = NULL) {
  sdf_register(j_invoke(pc, "encode", spark_dataframe(df), resource_name,
      input_type %||% MimeType$FHIR_JSON, column))
}

#' Encodes FHIR Bundles into Spark DataFrame
#'
#' Takes a dataframe with string representations of FHIR bundles in the given column and encodes
#' the resources of the given types as Spark DataFrame.
#'
#' @param pc A PathlingContext instance.
#' @param df A Spark DataFrame containing the bundles with the resources to encode.
#' @param resource_name The name of the FHIR resource to extract (Condition, Observation, etc.).
#' @param input_type The MIME type of the input string encoding. Defaults to 'application/fhir+json'.
#' @param column The column in which the resources to encode are stored. If 'NULL', then the
#'   input DataFrame is assumed to have one column of type string.
#'
#' @return A Spark DataFrame containing the given type of resources encoded into Spark columns.
#'
#' @export
ptl_encode_bundle <- function(pc, df, resource_name, input_type = NULL, column = NULL) {
  sdf_register(j_invoke(pc, "encodeBundle",  spark_dataframe(df), resource_name,
      input_type %||% MimeType$FHIR_JSON, column))
}
