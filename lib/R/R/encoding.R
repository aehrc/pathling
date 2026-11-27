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
#' @param schema The schema to encode the resources with, as the JSON form of a struct type, such
#'   as one taken from an existing table's schemas. If NULL, the schema is inferred from the data,
#'   and a streaming dataframe is refused. With a schema, a streaming dataframe is encoded as a
#'   streaming dataframe.
#' @return A Spark DataFrame containing the given type of resources encoded into Spark columns.
#'
#' @family encoding functions
#'
#' @importFrom sparklyr sdf_register spark_dataframe spark_connection j_invoke
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
pathling_encode <- function(pc, df, resource_name, input_type = NULL, column = NULL,
                            schema = NULL) {
  if (!is.null(schema)) {
    return(sdf_register(j_invoke(
      pc, "encodeWithSchema", select_documents(df, column), resource_name,
      input_type %||% MimeType$FHIR_JSON,
      to_java_schema(spark_connection(df), resource_name, schema)
    )))
  }
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
#' @param schema The schema to encode the resources with, as the JSON form of a struct type, such
#'   as one taken from an existing table's schemas. If NULL, the schema is inferred from the data,
#'   and a streaming dataframe is refused. With a schema, a streaming dataframe is encoded as a
#'   streaming dataframe.
#'
#' @return A Spark DataFrame containing the given type of resources encoded into Spark columns.
#'
#' @family encoding functions
#'
#' @importFrom sparklyr sdf_register spark_dataframe spark_connection j_invoke
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
pathling_encode_bundle <- function(pc, df, resource_name, input_type = NULL, column = NULL,
                                   schema = NULL) {
  if (!is.null(schema)) {
    return(sdf_register(j_invoke(
      pc, "encodeBundleWithSchema", select_documents(df, column), resource_name,
      input_type %||% MimeType$FHIR_JSON,
      to_java_schema(spark_connection(df), resource_name, schema)
    )))
  }
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
#' for a query. The numeric annotation of a decimal, the range annotations of a date, dateTime or
#' instant, and the canonical annotation of a quantity are defined in this release, so with the
#' default options resources without any of those are returned unchanged, and with
#' \code{drop_unknown_annotations} the only other effect is that fields whose names begin with
#' "__" are left out.
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

#' Select the column of documents to encode
#'
#' @param df A Spark dataframe.
#' @param column The column holding the documents, or NULL if the dataframe has only that column.
#'
#' @return A jobj referring to a Spark dataset with only the column of documents.
#'
#' @importFrom sparklyr spark_dataframe j_invoke
#' @noRd
select_documents <- function(df, column) {
  documents <- spark_dataframe(df)
  if (is.null(column)) documents else j_invoke(documents, "select", column, list())
}

#' Convert the schema of one resource type to a Java struct type
#'
#' The schema is converted as a schema set of one entry, so that it is refused with an error naming
#' the resource type when it is not a struct type's JSON form.
#'
#' @param sc The Spark connection.
#' @param resource_name The resource type code the schema is for.
#' @param schema A string holding the JSON form of a struct type.
#'
#' @return A jobj referring to a \code{StructType}.
#'
#' @importFrom sparklyr j_invoke j_invoke_new
#' @noRd
to_java_schema <- function(sc, resource_name, schema) {
  java_schemas <- to_java_schema_map(sc, stats::setNames(list(schema), resource_name))
  j_invoke_new(sc, "java.util.LinkedHashMap", java_schemas) %>%
    j_invoke("get", resource_name)
}
