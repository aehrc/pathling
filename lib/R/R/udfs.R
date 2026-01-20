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

#
# Placeholders for SQL functions and UDFs.
#
designation <- function(...) {}
display <- function(...) {}
struct <- function(...) {}
string <- function(...) {}
member_of <- function(...) {}
translate_coding <- function(...) {}
subsumes <- function(...) {}
property_Coding <- function(...) {}
property_boolean <- function(...) {}
property_dateTime <- function(...) {}
property_decimal <- function(...) {}
property_integer <- function(...) {}
property_string <- function(...) {}

#' Convert a vector to a SQL array literal
#'
#' Converts a vector to an expression with the corresponding SQL array literal.
#'
#' @param value A character or numeric vector to be converted
#'
#' @return The \code{quosure} with the SQL array literal that can be used in \code{dplyr::mutate}.
to_array <- function(value) {
  if (!is.null(value)) {
    rlang::new_quosure(rlang::expr(array(!!!value)))
  } else {
    rlang::new_quosure(rlang::expr(NULL))
  }
}

#' Coding property data types
#'
#' The following data types are supported:
#' \itemize{
#'   \item \code{STRING} - A string value.
#'   \item \code{INTEGER} - An integer value.
#'   \item \code{BOOLEAN} - A boolean value.
#'   \item \code{DECIMAL} - A decimal value.
#'   \item \code{DATETIME} - A date/time value.
#'   \item \code{CODE} - A code value.
#'   \item \code{CODING} - A Coding value.
#' }
#'
#' @seealso \href{https://hl7.org/fhir/R4/datatypes.html}{FHIR R4 - Data Types}
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

#' Concept map equivalence types
#'
#' The following values are supported:
#' \itemize{
#'   \item \code{RELATEDTO} - The concepts are related to each other, and have at least some overlap in meaning, but the exact relationship is not known.
#'   \item \code{EQUIVALENT} - The definitions of the concepts mean the same thing (including when structural implications of meaning are considered) (i.e. extensionally identical).
#'   \item \code{EQUAL} - The definitions of the concepts are exactly the same (i.e. only grammatical differences) and structural implications of meaning are identical or irrelevant (i.e. intentionally identical).
#'   \item \code{WIDER} - The target mapping is wider in meaning than the source concept.
#'   \item \code{SUBSUMES} - The target mapping subsumes the meaning of the source concept (e.g. the source is-a target).
#'   \item \code{NARROWER} - The target mapping is narrower in meaning than the source concept. The sense in which the mapping is narrower SHALL be described in the comments in this case, and applications should be careful when attempting to use these mappings operationally.
#'   \item \code{SPECIALIZES} - The target mapping specializes the meaning of the source concept (e.g. the target is-a source).
#'   \item \code{INEXACT} - There is some similarity between the concepts, but the exact relationship is not known.
#'   \item \code{UNMATCHED} - This is an explicit assertion that there is no mapping between the source and target concept.
#'   \item \code{DISJOINT} - This is an explicit assertion that the target concept is not in any way related to the source concept.
#' }
#'
#' @seealso \href{https://hl7.org/fhir/R4/valueset-concept-map-equivalence.html}{FHIR R4 - ConceptMapEquivalence}
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

#' Test membership within a value set
#'
#' Takes a Coding or array of Codings column as its input. Returns the column which contains a
#' Boolean value, indicating whether any of the input Codings is a member of the specified FHIR
#' ValueSet.
#'
#' @param codings A Column containing a struct representation of a Coding or an array of such
#' structs.
#' @param value_set_uri An identifier for a FHIR ValueSet.
#'
#' @return A Column containing the result of the operation.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#value-set-membership}{Pathling documentation - Value set membership}
#'
#' @family terminology functions
#'
#' @export
#'
#' @examples
tx_member_of <- function(codings, value_set_uri) {
  rlang::expr(member_of({{ codings }}, {{ value_set_uri }}))
}

#' Translate between value sets
#'
#' Takes a Coding column as input. Returns the Column which contains an array of
#' Coding value with translation targets from the specified FHIR ConceptMap. There
#' may be more than one target concept for each input concept. Only the translation with
#' the specified equivalences are returned.
#'
#' @param codings A Column containing a struct representation of a Coding.
#' @param concept_map_uri An identifier for a FHIR ConceptMap.
#' @param reverse The direction to traverse the map. FALSE results in "source to target"
#'   mappings, while TRUE results in "target to source".
#' @param equivalences A value of a collection of values from the ConceptMapEquivalence ValueSet.
#' @param target Identifies the value set in which a translation is sought. If there's no
#'   target specified, the server should return all known translations.
#'
#' @return A Column containing the result of the operation (an array of Coding structs).
#'
#' @seealso \code{\link{Equivalence}}
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#concept-translation}{Pathling documentation - Concept translation}
#'
#' @family terminology functions
#'
#' @export
#' @examples \dontrun{
#' # Translates the codings of the Condition `code` using a SNOMED implicit concept map.
#' pc %>%
#'   pathling_example_resource("Condition") %>%
#'   sparklyr::mutate(
#'     id,
#'     translation = !!tx_translate(
#'       code[["coding"]],
#'       "http://snomed.info/sct?fhir_cm=900000000000527005"
#'     ),
#'     .keep = "none"
#'   )
#' }
tx_translate <- function(codings, concept_map_uri, reverse = FALSE, equivalences = NULL, target = NULL) {
  rlang::expr(translate_coding(
    {{ codings }}, {{ concept_map_uri }}, {{ reverse }},
    !!to_array(equivalences), {{ target }}
  ))
}

#' Test subsumption between codings
#'
#' Takes two Coding columns as input. Returns a Column that contains a Boolean value,
#' indicating whether the left Coding subsumes the right Coding.
#'
#' @param left_codings A Column containing a struct representation of a Coding or an array of Codings.
#' @param right_codings A Column containing a struct representation of a Coding or an array of Codings.
#'
#' @return A Column containing the result of the operation (boolean).
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#subsumption-testing}{Pathling documentation - Subsumption testing}
#'
#' @family terminology functions
#'
#' @export
#' @examples \dontrun{
#' # Test the codings of the Condition `code` for subsumption of a SNOMED CT code.
#' pc %>%
#'   pathling_example_resource("Condition") %>%
#'   sparklyr::mutate(
#'     id,
#'     subsumes = !!tx_subsumes(
#'       code[["coding"]],
#'       !!tx_to_snomed_coding("444814009")
#'     ),
#'     .keep = "none"
#'   )
#' }
tx_subsumes <- function(left_codings, right_codings) {
  rlang::expr(subsumes({{ left_codings }}, {{ right_codings }}, FALSE))
}

#' Test subsumption between codings
#'
#' Takes two Coding columns as input. Returns a Column that contains a Boolean value,
#' indicating whether the left Coding is subsumed by the right Coding.
#'
#' @param left_codings A Column containing a struct representation of a Coding or an array of Codings.
#' @param right_codings A Column containing a struct representation of a Coding or an array of Codings.
#'
#' @return A Column containing the result of the operation (boolean).
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#subsumption-testing}{Pathling documentation - Subsumption testing}
#'
#' @family terminology functions
#'
#' @export
#'
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#'
#' # Test the codings of the Condition `code` for subsumption by a SNOMED CT code.
#' pc %>%
#'   pathling_example_resource("Condition") %>%
#'   sparklyr::mutate(
#'     id,
#'     is_subsumed_by = !!tx_subsumed_by(
#'       code[["coding"]],
#'       !!tx_to_snomed_coding("444814009")
#'     ),
#'     .keep = "none"
#'   )
#'
#' pathling_disconnect(pc)
tx_subsumed_by <- function(left_codings, right_codings) {
  rlang::expr(subsumes({{ left_codings }}, {{ right_codings }}, TRUE))
}

#' Get the display text for codings
#'
#' Takes a Coding column as its input. Returns a Column that contains the canonical display
#' name associated with the given code.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param accept_language The optional language preferences for the returned display name.
#'        Overrides the parameter `accept_language` in \code{\link{pathling_connect}}.
#'
#' @return A Column containing the result of the operation (String).
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#multi-language-support}{Pathling documentation - Multi-language support}
#'
#' @family terminology functions
#'
#' @export
#'
#' @examples \dontrun{
#' # Get the display name of the first coding of the Condition resource, with the default language.
#' pc %>%
#'   pathling_example_resource("Condition") %>%
#'   sparklyr::mutate(
#'     id,
#'     display = !!tx_display(code[["coding"]][[0]]),
#'     .keep = "none"
#'   )
#' }
tx_display <- function(coding, accept_language = NULL) {
  rlang::expr(display({{ coding }}, {{ accept_language }}))
}

#' Get properties for codings
#'
#' Takes a Coding column as its input. Returns a Column that contains the values of properties
#' for this coding with specified names and types. The type of the result column depends on the
#' types of the properties. Primitive FHIR types are mapped to their corresponding SQL primitives.
#' Complex types are mapped to their corresponding structs.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param property_code The code of the property to retrieve.
#' @param property_type The type of the property to retrieve.
#' @param accept_language The optional language preferences for the returned property values.
#'        Overrides the parameter `accept_language` in `PathlingContext.create`.
#'
#' @return The Column containing the result of the operation (array of property values).
#'
#' @seealso \code{\link{PropertyType}}
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#retrieving-properties}{Pathling documentation - Retrieving properties}
#'
#' @family terminology functions
#'
#' @export
#'
#' @examples \dontrun{
#' # Get the (first) value of the `inactive` property of the first coding of the Condition resource.
#' pc %>%
#'   pathling_example_resource("Condition") %>%
#'   sparklyr::mutate(id,
#'     is_inavtive = (!!tx_property_of(
#'       code[["coding"]][[0]],
#'       "inactive", PropertyType$BOOLEAN
#'     ))[[0]],
#'     .keep = "none"
#'   )
#' }
tx_property_of <- function(coding, property_code, property_type = "string", accept_language = NULL) {
  if (property_type == PropertyType$CODE) {
    rlang::expr(property_code({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else if (property_type == PropertyType$CODING) {
    rlang::expr(property_Coding({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else if (property_type == PropertyType$STRING) {
    rlang::expr(property_string({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else if (property_type == PropertyType$INTEGER) {
    rlang::expr(property_integer({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else if (property_type == PropertyType$BOOLEAN) {
    rlang::expr(property_boolean({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else if (property_type == PropertyType$DATETIME) {
    rlang::expr(property_dateTime({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else if (property_type == PropertyType$DECIMAL) {
    rlang::expr(property_decimal({{ coding }}, {{ property_code }}, {{ accept_language }}))
  } else {
    stop("Unsupported property type: ", property_type)
  }
}

#' Get designations for codings
#'
#' Takes a Coding column as its input. Returns a Column that contains the values of designations
#' (strings) for this coding that match the specified use and language. If the language is
#' not provided, then all designations with the specified type are returned regardless of
#' their language.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param use The code with the use of the designations.
#' @param language The language of the designations.
#'
#' @return The Column containing the result of the operation (array of strings with designation values).
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/terminology#retrieving-designations}{Pathling documentation - Retrieving designations}
#'
#' @family Terminology functions
#'
#' @export
#'
#' @examples \dontrun{
#' # Get the (first) SNOMED CT "Fully specified name" ('900000000000003001')
#' # for the first coding of the Condition resource, in the 'en' language.
#' pc %>%
#'   pathling_example_resource("Condition") %>%
#'   sparklyr::mutate(
#'     id,
#'     designation = (!!tx_designation(code[["coding"]][[0]],
#'       !!tx_to_snomed_coding("900000000000003001"),
#'       language = "en"
#'     ))[[0]],
#'     .keep = "none"
#'   )
#' }
tx_designation <- function(coding, use = NULL, language = NULL) {
  rlang::expr(designation({{ coding }}, {{ use }}, {{ language }}))
}
