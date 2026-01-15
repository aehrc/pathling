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

#' Execute a SQL on FHIR view
#'
#' Executes a SQL on FHIR view definition and returns the result as a Spark DataFrame.
#'
#' @param ds The DataSource object containing the data to be queried.
#' @param resource A string representing the type of FHIR resource that the view is based upon, e.g. 'Patient' or 'Observation'.
#' @param select A list of columns and nested selects to include in the view. Each element should be a list with appropriate structure.
#' @param constants An optional list of constants that can be used in FHIRPath expressions.
#' @param where An optional list of FHIRPath expressions that can be used to filter the view.
#' @param json An optional JSON string representing the view definition, as an alternative to providing the parameters as R objects.
#' @return A Spark DataFrame containing the results of the view.
#'
#' @importFrom sparklyr j_invoke sdf_register
#' @importFrom jsonlite toJSON
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/running-queries}{Pathling documentation - SQL on FHIR}
#'
#' @export
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#' data_source %>% ds_view("Patient",
#'   select = list(
#'     list(
#'       column = list(
#'         list(path = "id", name = "id"),
#'         list(path = "gender", name = "gender"),
#'         list(
#'           path = "telecom.where(system='phone').value",
#'           name = "phone_numbers", collection = TRUE
#'         )
#'       )
#'     ),
#'     list(
#'       forEach = "name",
#'       column = list(
#'         list(path = "use", name = "name_use"),
#'         list(path = "family", name = "family_name")
#'       ),
#'       select = list(
#'         list(
#'           forEachOrNull = "given",
#'           column = list(
#'             list(path = "$this", name = "given_name")
#'           )
#'         )
#'       )
#'     )
#'   ),
#'   where = list(
#'     list(path = "gender = 'male'")
#'   )
#' )
#' }
ds_view <- function(ds, resource, select = NULL, constants = NULL, where = NULL, json = NULL) {
  jquery <- j_invoke(ds, "view", as.character(resource))

  if (!is.null(json)) {
    j_invoke(jquery, "json", as.character(json))
  } else {
    # Build the query from individual components
    query <- list()

    if (!is.null(resource)) {
      query$resource <- resource
    }

    if (!is.null(select)) {
      query$select <- select
    }

    if (!is.null(constants)) {
      query$constants <- constants
    }

    if (!is.null(where)) {
      query$where <- where
    }

    # Convert the query to JSON and pass it to the Java method
    query_json <- jsonlite::toJSON(query, auto_unbox = TRUE)
    j_invoke(jquery, "json", as.character(query_json))
  }

  sdf_register(j_invoke(jquery, "execute"))
}
