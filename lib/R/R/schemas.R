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

#' Put a schema set into canonical order
#'
#' Returns a schema set with each schema in canonical field order at every depth, the order an
#' inferred load gives, without reading any data. Each field keeps its nullability and metadata. A
#' field the FHIR definitions do not describe is left out.
#'
#' @param pc The PathlingContext object.
#' @param schemas A named list of schemas, keyed by resource type code, each a string holding a
#'   struct type's JSON form.
#'
#' @return The schema set in canonical order, in the same form.
#'
#' @family schema set functions
#'
#' @importFrom sparklyr j_invoke spark_connection
#'
#' @export
#'
#' @examples \dontrun{
#' canonical <- pc %>% pathling_canonical_schemas(schemas)
#' }
pathling_canonical_schemas <- function(pc, schemas) {
  java_schemas <- to_java_schema_map(spark_connection(pc), schemas)
  from_java_schema_map(j_invoke(pc, "canonicalSchemas", java_schemas))
}

#' Write a schema set file
#'
#' Saves a schema set to a file holding one JSON object, which maps each resource type code to the
#' JSON form of its struct type. The file can be read by \code{\link{pathling_read_schemas}}, and
#' by the Python and Java APIs.
#'
#' @param sc The Spark connection.
#' @param schemas A named list of schemas, keyed by resource type code, each a string holding a
#'   struct type's JSON form.
#' @param path The path of the file to write.
#'
#' @return No return value, called for its side effect.
#'
#' @family schema set functions
#'
#' @importFrom sparklyr invoke_static spark_connection
#'
#' @export
#'
#' @examples \dontrun{
#' pathling_write_schemas(sc, ds_schemas(data_source), "schemas.json")
#' }
pathling_write_schemas <- function(sc, schemas, path) {
  java_schemas <- to_java_schema_map(spark_connection(sc), schemas)
  content <- invoke_static(
    spark_connection(sc), "au.csiro.pathling.library.SchemaSets", "toJson", java_schemas
  )
  writeLines(content, path)
  invisible(NULL)
}

#' Read a schema set file
#'
#' Reads a schema set from a file written by \code{\link{pathling_write_schemas}}, or by the Python
#' or Java APIs.
#'
#' @param sc The Spark connection.
#' @param path The path of the file to read.
#'
#' @return A named list of schemas, keyed by resource type code in the order of the file, each a
#'   string holding a struct type's JSON form.
#'
#' @family schema set functions
#'
#' @importFrom sparklyr j_invoke_static spark_connection
#'
#' @export
#'
#' @examples \dontrun{
#' schemas <- pathling_read_schemas(sc, "schemas.json")
#' }
pathling_read_schemas <- function(sc, path) {
  content <- paste(readLines(path, warn = FALSE), collapse = "\n")
  java_schemas <- tryCatch(
    j_invoke_static(
      spark_connection(sc), "au.csiro.pathling.library.SchemaSets", "fromJson", content
    ),
    error = function(e) {
      stop("Cannot read the schema set file ", path, ": ", conditionMessage(e), call. = FALSE)
    }
  )
  from_java_schema_map(java_schemas)
}

#' Convert a Java map of resource types to schemas into a named list
#'
#' The map Java returns is an instance of a class internal to the JDK, whose methods sparklyr
#' cannot call, and so are its key set and iterators. The map and its keys are therefore first
#' copied into a \code{LinkedHashMap} and an \code{ArrayList}, which are public classes, keeping
#' their order. The keys and the JSON forms are fetched with \code{invoke}, which converts them to R
#' values, where \code{j_invoke} would return references to them.
#'
#' @param java_schemas A jobj referring to a \code{java.util.Map} from resource type codes to
#'   \code{StructType} objects.
#'
#' @return A named list of the JSON forms of the schemas, in the order of the map.
#'
#' @importFrom sparklyr invoke j_invoke j_invoke_new spark_connection
#' @noRd
from_java_schema_map <- function(java_schemas) {
  sc <- spark_connection(java_schemas)
  copy <- j_invoke_new(sc, "java.util.LinkedHashMap", java_schemas)
  resource_types <- as.character(unlist(
    j_invoke_new(sc, "java.util.ArrayList", j_invoke(copy, "keySet")) %>% invoke("toArray")
  ))
  stats::setNames(
    lapply(resource_types, function(resource_type) {
      copy %>%
        j_invoke("get", resource_type) %>%
        invoke("json")
    }),
    resource_types
  )
}
