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

#' Import a SNOMED CT RF2 snapshot release
#'
#' Imports a SNOMED CT RF2 snapshot release (archive or extracted directory) into a local
#' terminology store. The import runs as Spark jobs and replaces any previous content for the same
#' code system version atomically.
#'
#' @param pc The PathlingContext object.
#' @param source The path to an RF2 release archive or extracted directory, on any filesystem
#'   accessible through the Hadoop FileSystem API.
#' @param storage_path The terminology store location, created if absent.
#' @param edition_uri An explicit SNOMED edition/version URI, overriding detection. Defaults to
#'   NULL.
#' @param dense_id_order How dense identifiers are assigned to concepts, either "code-order" (the
#'   default) or "pre-order". The pre-order makes the runtime hierarchy index materially smaller, in
#'   exchange for identifiers that shift more between releases. Defaults to NULL.
#' @param default_dialect The dialect whose preferred synonyms become the stored display of every
#'   concept, given as a dialect tag such as "en-GB", as a private-use dialect extension tag, or as a
#'   language reference set identifier. When NULL, the dialect is chosen from the release: the sole
#'   language reference set where there is only one, or US English for the International edition. A
#'   release that holds several and is not the International edition fails the import, naming the
#'   candidates. Defaults to NULL.
#'
#' @return The PathlingContext object, invisibly.
#'
#' @family terminology import functions
#'
#' @importFrom sparklyr j_invoke j_invoke_static spark_connection
#'
#' @export
pathling_import_snomed <- function(pc, source, storage_path, edition_uri = NULL,
                                   dense_id_order = NULL, default_dialect = NULL) {
  options <- NULL
  if (!is.null(edition_uri) || !is.null(dense_id_order) || !is.null(default_dialect)) {
    builder <- j_invoke_static(
      spark_connection(pc),
      "au.csiro.pathling.library.terminology.TerminologyImportOptions", "builder"
    )
    if (!is.null(edition_uri)) {
      builder <- j_invoke(builder, "editionUri", edition_uri)
    }
    if (!is.null(dense_id_order)) {
      order <- j_invoke_static(
        spark_connection(pc),
        "au.csiro.pathling.terminology.store.DenseIdOrder", "fromValue", dense_id_order
      )
      builder <- j_invoke(builder, "denseIdOrder", order)
    }
    if (!is.null(default_dialect)) {
      builder <- j_invoke(builder, "defaultDialect", default_dialect)
    }
    options <- j_invoke(builder, "build")
  }
  j_invoke(pc, "importSnomed", source, storage_path, options)
  invisible(pc)
}

#' Import FHIR terminology resources
#'
#' Imports FHIR R4 CodeSystem, ValueSet, and ConceptMap resources into a local terminology store.
#' The source may be a JSON file, a directory of JSON files, or a FHIR NPM package (.tgz).
#'
#' @param pc The PathlingContext object.
#' @param source The path to a JSON file, a directory of JSON files, or a FHIR NPM package, on any
#'   filesystem accessible through the Hadoop FileSystem API.
#' @param storage_path The terminology store location, created if absent.
#'
#' @return The PathlingContext object, invisibly.
#'
#' @family terminology import functions
#'
#' @importFrom sparklyr j_invoke
#'
#' @export
pathling_import_fhir_terminology <- function(pc, source, storage_path) {
  j_invoke(pc, "importFhirTerminology", source, storage_path)
  invisible(pc)
}
