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

#' @export
SNOMED_URI = 'http://snomed.info/sct'

#' @export
snomed_code <-function(code) {
  rlang::expr(struct(NULL, SNOMED_URI, NULL, string({{code}}), NULL, NULL))
}

#' Allowed property types.
#'
#' @export
PropertyType <- list(
  STRING = "string",
  INTEGER = "integer",
  BOOLEAN = "boolean",
  DECIMAL = "decimal",
  DATETIME = "dateTime",
  CODE = "code",
  CODING = "Coding"
)

#' Concept map equivalences.
#'
#' @export
Equivalence <- list(
  RELATEDTO = "relatedto",
  EQUIVALENT = "equivalent",
  EQUAL = "equal",
  WIDER = "wider",
  SUBSUMES = "subsumes",
  NARROWER = "narrower",
  SPECIALIZES = "specializes",
  INEXACT = "inexact",
  UNMATCHED = "unmatched",
  DISJOINT = "disjoint"
)


#' Checks if Coding is a member of ValueSet.
#'
#' Takes a Coding or array of Codings column as its input. Returns the column which contains a
#' Boolean value, indicating whether any of the input Codings is a member of the specified FHIR
#' ValueSet.
#'
#' @param coding A Column containing a struct representation of a Coding or an array of such structs.
#' @param value_set_uri An identifier for a FHIR ValueSet.
#'
#' @return A Column containing the result of the operation.
#'
#' @examples
#' # Example usage of member_of function
#' member_of(coding, value_set_uri)
#'
#' @export
trm_member_of <- function(coding, value_set_uri) {
  rlang::expr(member_of({{coding}}, {{value_set_uri}}))
}


#' Translates a Coding column.
#'
#' Takes a Coding column as input. Returns the Column which contains an array of
#' Coding value with translation targets from the specified FHIR ConceptMap. There
#' may be more than one target concept for each input concept. Only the translation with
#' the specified equivalences are returned.
#' See also \code{\link{Equivalence}}.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param concept_map_uri An identifier for a FHIR ConceptMap.
#' @param reverse The direction to traverse the map. FALSE results in "source to target"
#'   mappings, while TRUE results in "target to source".
#' @param equivalences A value of a collection of values from the ConceptMapEquivalence ValueSet.
#' @param target Identifies the value set in which a translation is sought. If there's no
#'   target specified, the server should return all known translations.
#'
#' @return A Column containing the result of the operation (an array of Coding structs).
#'
#' @examples
#' # Example usage of trm_translate function
#' trm_translate(coding, concept_map_uri, reverse = FALSE, equivalences = c("equivalent", "wider"), target = NULL)
#'
#' @export
trm_translate <- function(coding, concept_map_uri, reverse = FALSE, equivalences = NULL, target = NULL) {
  rlang::expr(translate({{ coding }}, {{ concept_map_uri }}, {{ reverse }},
    {{ equivalences }}, {{ target }}))
}

#' Checks if left Coding subsumes right Coding.
#'
#' Takes two Coding columns as input. Returns the Column, which contains a Boolean value,
#' indicating whether the left Coding subsumes the right Coding.
#'
#' @param left_coding A Column containing a struct representation of a Coding or an array of Codings.
#' @param right_coding A Column containing a struct representation of a Coding or an array of Codings.
#'
#' @return A Column containing the result of the operation (boolean).
#'
#' @examples
#' # Example usage of trm_subsumes function
#' trm_subsumes(left_coding, right_coding)
#'
#' @export
trm_subsumes <- function(left_coding, right_coding) {
  rlang::expr(subsumes({{ left_coding }}, {{ right_coding }}))
}

#' Checks if left Coding is subsumed by right Coding.
#'
#' Takes two Coding columns as input. Returns the Column, which contains a Boolean value,
#' indicating whether the left Coding is subsumed by the right Coding.
#'
#' @param left_coding A Column containing a struct representation of a Coding or an array of Codings.
#' @param right_coding A Column containing a struct representation of a Coding or an array of Codings.
#'
#' @return A Column containing the result of the operation (boolean).
#'
#' @examples
#' # Example usage of trm_subsumed_by function
#' trm_subsumed_by(left_coding, right_coding)
#'
#' @export
trm_subsumed_by <- function(left_coding, right_coding) {
  rlang::expr(subsumed_by({{ left_coding }}, {{ right_coding }}))
}

#' Retrieves the canonical display name for a Coding.
#'
#' Takes a Coding column as its input. Returns the Column, which contains the canonical display
#' name associated with the given code.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param accept_language The optional language preferences for the returned display name.
#'        Overrides the parameter `accept_language` in
#'        `ptl_connect`.
#'
#' @return A Column containing the result of the operation (String).
#'
#' @examples
#' # Example usage of trm_display function
#' trm_display(coding, accept_language = NULL)
#'
#' @export
trm_display <- function(coding, accept_language = NULL) {
  rlang::expr(display({{ coding }}, {{ accept_language }}))
}

#' Retrieves the values of properties for a Coding.
#'
#' Takes a Coding column as its input. Returns the Column, which contains the values of properties
#' for this coding with specified names and types. The type of the result column depends on the
#' types of the properties. Primitive FHIR types are mapped to their corresponding SQL primitives.
#' Complex types are mapped to their corresponding structs. The allowed property types are:
#' code | Coding | string | integer | boolean | dateTime | decimal.
#' See also \code{\link{PropertyType}}.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param property_code The code of the property to retrieve.
#' @param property_type The type of the property to retrieve.
#' @param accept_language The optional language preferences for the returned property values.
#'        Overrides the parameter `accept_language` in `PathlingContext.create`.
#'
#' @return The Column containing the result of the operation (array of property values).
#'
#' @examples
#' # Example usage of trm_property_of function
#' trm_property_of(coding, property_code, property_type = "string", accept_language = NULL)
#'
#' @export
trm_property_of <- function(coding, property_code, property_type = "string", accept_language = NULL) {
  rlang::expr(property_of({{ coding }}, {{ property_code }}, {{property_type}}, {{ accept_language }}))
}


#' Retrieves the values of designations for a Coding.
#'
#' Takes a Coding column as its input. Returns the Column, which contains the values of
#' designations (strings) for this coding for the specified use and language. If the language is
#' not provided (is null), then all designations with the specified type are returned regardless of
#' their language.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param use The code with the use of the designations.
#' @param language The language of the designations.
#'
#' @return The Column containing the result of the operation (array of strings with designation values).
#'
#' @examples
#' # Example usage of trm_designation function
#' trm_designation(coding, use = NULL, language = NULL)
#'
#' @export
trm_designation <- function(coding, use = NULL, language = NULL) {
  rlang::expr(designation({{ coding }}, {{ use }}, {{ language }}))
}
