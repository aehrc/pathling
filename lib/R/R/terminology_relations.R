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

#' Get a value set as a relation
#'
#' Resolves a value set to its membership and returns it as a relation that Spark SQL can join to,
#' as described in the SQL on FHIR
#' \href{https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html}{Terminology in SQL}
#' page. The relation has one row per member and the columns \code{system}, \code{version},
#' \code{code}, \code{display} and \code{inactive}; \code{system} and \code{code} are never null.
#'
#' The membership is obtained from the configured terminology layer when this function is called,
#' so every use of the relation sees the same membership. A value set that cannot be resolved, or
#' whose membership cannot be determined or exceeds the cap, is an error.
#'
#' @param pc The PathlingContext object.
#' @param canonical The canonical URL of the value set, optionally with a \code{|version} suffix.
#' @param max_members The largest membership accepted. When NULL, 100,000 members are accepted.
#'   Defaults to NULL.
#'
#' @return A Spark DataFrame containing the value set relation.
#'
#' @family terminology relation functions
#'
#' @importFrom sparklyr j_invoke sdf_register
#'
#' @export
pathling_value_set <- function(pc, canonical, max_members = NULL) {
  jdf <- if (is.null(max_members)) {
    j_invoke(pc, "valueSet", canonical)
  } else {
    j_invoke(pc, "valueSet", canonical, as.integer(max_members))
  }
  sdf_register(jdf)
}

#' Get a supplied value set as a relation
#'
#' Returns the membership of a ValueSet supplied as FHIR JSON as a relation with the columns of
#' \code{\link{pathling_value_set}}. Where the resource carries an \code{expansion}, that expansion
#' is the membership and the terminology layer is not consulted; an incomplete expansion is
#' rejected. Where it carries only a \code{compose}, the configured terminology layer expands it.
#'
#' @param pc The PathlingContext object.
#' @param json The ValueSet resource as FHIR JSON.
#' @param max_members The largest membership accepted. When NULL, 100,000 members are accepted.
#'   Defaults to NULL.
#'
#' @return A Spark DataFrame containing the value set relation.
#'
#' @family terminology relation functions
#'
#' @importFrom sparklyr j_invoke sdf_register
#'
#' @export
pathling_value_set_from_json <- function(pc, json, max_members = NULL) {
  jdf <- if (is.null(max_members)) {
    j_invoke(pc, "valueSetFromJson", as.character(json))
  } else {
    j_invoke(pc, "valueSetFromJson", as.character(json), as.integer(max_members))
  }
  sdf_register(jdf)
}

#' Get a concept map as a relation
#'
#' Resolves a concept map to its mappings and returns them as a relation that Spark SQL can join
#' to, as described in the SQL on FHIR
#' \href{https://build.fhir.org/ig/HL7/sql-on-fhir/en/terminology-in-sql.html}{Terminology in SQL}
#' page. The relation has one row per mapping, plus one row with a null \code{target_code} for each
#' source code the map states has no mapping, and the columns \code{source_system},
#' \code{source_version}, \code{source_code}, \code{source_display}, \code{target_system},
#' \code{target_version}, \code{target_code}, \code{target_display} and \code{relationship};
#' \code{source_system} and \code{source_code} are never null. \code{relationship} is an R5
#' ConceptMapRelationship code, converted from the R4 equivalence.
#'
#' The mappings are read from the configured terminology layer when this function is called. A
#' concept map that cannot be resolved, carries content the relation cannot represent, or exceeds
#' the cap is an error.
#'
#' @param pc The PathlingContext object.
#' @param canonical The canonical URL of the concept map, optionally with a \code{|version} suffix.
#' @param max_mappings The largest number of mappings accepted. When NULL, 100,000 mappings are
#'   accepted. Defaults to NULL.
#'
#' @return A Spark DataFrame containing the concept map relation.
#'
#' @family terminology relation functions
#'
#' @importFrom sparklyr j_invoke sdf_register
#'
#' @export
pathling_concept_map <- function(pc, canonical, max_mappings = NULL) {
  jdf <- if (is.null(max_mappings)) {
    j_invoke(pc, "conceptMap", canonical)
  } else {
    j_invoke(pc, "conceptMap", canonical, as.integer(max_mappings))
  }
  sdf_register(jdf)
}

#' Get a supplied concept map as a relation
#'
#' Returns the mappings of a ConceptMap supplied as FHIR JSON as a relation with the columns of
#' \code{\link{pathling_concept_map}}, without consulting the terminology layer.
#'
#' @param pc The PathlingContext object.
#' @param json The ConceptMap resource as FHIR JSON.
#' @param max_mappings The largest number of mappings accepted. When NULL, 100,000 mappings are
#'   accepted. Defaults to NULL.
#'
#' @return A Spark DataFrame containing the concept map relation.
#'
#' @family terminology relation functions
#'
#' @importFrom sparklyr j_invoke sdf_register
#'
#' @export
pathling_concept_map_from_json <- function(pc, json, max_mappings = NULL) {
  jdf <- if (is.null(max_mappings)) {
    j_invoke(pc, "conceptMapFromJson", as.character(json))
  } else {
    j_invoke(pc, "conceptMapFromJson", as.character(json), as.integer(max_mappings))
  }
  sdf_register(jdf)
}
