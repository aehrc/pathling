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

#
# Placeholders for SQL functions and UDFs
#
designation <- function(...) { }
display <- function(...) { }
struct <- function(...) { }
string <- function(...) { }
member_of <- function(...) { }
translate_coding <- function(...) { }
subsumes <- function(...) { }
property_Coding <- function(...) { }
property_boolean <- function(...) { }
property_dateTime <- function(...) { }
property_decimal <- function(...) { }
property_integer <- function(...) { }
property_string <- function(...) { }

#' Converts a vector to an expression with the corresponding SQL array litera.
#' @param value A character or numeric vector to be converted
#' @return The `quosure` with the SQL array literal that can be used in dplyr::mutate.
to_array <- function(value) {
  if (!is.null(value)) {
    rlang::new_quosure(rlang::expr(array(!!!value)))
  } else {
    rlang::new_quosure(rlang::expr(NULL))
  }
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
#' \code{trm_member_of()} takes a Coding or array of Codings column as its input. Returns the column which contains a
#' Boolean value, indicating whether any of the input Codings is a member of the specified FHIR
#' ValueSet.
#'
#' @param codings A Column containing a struct representation of a Coding or an array of such structs.
#' @param value_set_uri An identifier for a FHIR ValueSet.
#'
#' @return A Column containing the result of the operation.
#' 
#' @family terminology functions
#'
#' @export
#' 
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Test the codings of the Condition `code` for membership in a SNOMED CT ValueSet.
#' pc %>% pathlyr_example_resource('Condition') %>%
#'      sparklyr::mutate(
#'          id, 
#'          is_member = !!trm_member_of(code[['coding']], 
#'                  'http://snomed.info/sct?fhir_vs=refset/723264001'), 
#'          .keep='none')
#' 
#' ptl_disconnect(pc)
trm_member_of <- function(codings, value_set_uri) {
  rlang::expr(member_of({ { codings } }, { { value_set_uri } }))
}

#' Translates a Coding column.
#'
#' \code{trm_translate()} a Coding column as input. Returns the Column which contains an array of
#' Coding value with translation targets from the specified FHIR ConceptMap. There
#' may be more than one target concept for each input concept. Only the translation with
#' the specified equivalences are returned.
#' See also \code{\link{Equivalence}}.
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
#' @family terminology functions
#' 
#' @export
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Translates the codings of the Condition `code` using a SNOMED implicit concept map.
#' pc %>% pathlyr_example_resource('Condition') %>%
#'     sparklyr::mutate(
#'          id,
#'          translation = !!trm_translate(code[['coding']],
#'                  'http://snomed.info/sct?fhir_cm=900000000000527005'),
#'          .keep='none')
#'  
#' ptl_disconnect(pc)
trm_translate <- function(codings, concept_map_uri, reverse = FALSE, equivalences = NULL, target = NULL) {
  rlang::expr(translate_coding({ { codings } }, { { concept_map_uri } }, { { reverse } },
                               !!to_array(equivalences), { { target } }))
}

#' Checks if left Coding subsumes right Coding.
#'
#' \code{trm_subsumes()} two Coding columns as input. Returns the Column,
#' which contains a Boolean value,
#' indicating whether the left Coding subsumes the right Coding.
#'
#' @param left_codings A Column containing a struct representation of a Coding or an array of Codings.
#' @param right_codings A Column containing a struct representation of a Coding or an array of Codings.
#'
#' @return A Column containing the result of the operation (boolean).
#' 
#' @family terminology functions
#'
#' @export
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Test the codings of the Condition `code` for subsumption of a SNOMED CT code.
#' pc %>% pathlyr_example_resource('Condition') %>%
#'     sparklyr::mutate(
#'          id,
#'          subsumes = !!trm_subsumes(code[['coding']],
#'              !!trm_to_snomed_coding('444814009')),
#'          .keep='none')
#'  
#' ptl_disconnect(pc)
trm_subsumes <- function(left_codings, right_codings) {
  rlang::expr(subsumes({ { left_codings } }, { { right_codings } }, FALSE))
}

#' Checks if left Coding is subsumed by right Coding.
#'
#' \code{trm_subsumed_by()} takes two Coding columns as input. Returns the Column, 
#' which contains a Boolean value,
#' indicating whether the left Coding is subsumed by the right Coding.
#'
#' @param left_codings A Column containing a struct representation of a Coding or an array of Codings.
#' @param right_codings A Column containing a struct representation of a Coding or an array of Codings.
#'
#' @return A Column containing the result of the operation (boolean).
#'
#' @family terminology functions
#' 
#' @export
#' 
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Test the codings of the Condition `code` for subsumption by a SNOMED CT code.
#' pc %>% pathlyr_example_resource('Condition') %>%
#'     sparklyr::mutate(
#'          id,
#'          is_subsumed_by = !!trm_subsumed_by(code[['coding']],
#'              !!trm_to_snomed_coding('444814009')),
#'          .keep='none')
#'  
#' ptl_disconnect(pc)
trm_subsumed_by <- function(left_codings, right_codings) {
  rlang::expr(subsumes({ { left_codings } }, { { right_codings } }, TRUE))
}

#' Retrieves the canonical display name for a Coding.
#'
#' \code{trm_display()} takes a Coding column as its input. Returns the Column, which contains the canonical display
#' name associated with the given code.
#'
#' @param coding A Column containing a struct representation of a Coding.
#' @param accept_language The optional language preferences for the returned display name.
#'        Overrides the parameter `accept_language` in
#'        `ptl_connect`.
#'
#' @return A Column containing the result of the operation (String).
#'
#' @family terminology functions
#' 
#' @export
#' 
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Get the display nane of the first coding of the Condition resource code with default language
#' pc %>% pathlyr_example_resource('Condition') %>%
#'      sparklyr::mutate(
#'          id, 
#'          display = !!trm_display(code[['coding']][[0]]), 
#'          .keep='none')
#' 
#' ptl_disconnect(pc)
trm_display <- function(coding, accept_language = NULL) {
  rlang::expr(display({ { coding } }, { { accept_language } }))
}

#' Retrieves the values of properties for a Coding.
#'
#' \code{trm_property_of()} takes a Coding column as its input. 
#' Returns the Column, which contains the values of properties
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
#' @family terminology functions
#'
#' @export
#' 
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Get the (first) value of `inactive` property of the first coding of the Condition resource code
#' pc %>% pathlyr_example_resource('Condition') %>%
#'      sparklyr::mutate(id, 
#'          is_inavtive = (!!trm_property_of(code[['coding']][[0]], 
#'                                  "inactive",PropertyType$BOOLEAN))[[0]], 
#'          .keep='none'
#'      )
#' 
#' ptl_disconnect(pc)
trm_property_of <- function(coding, property_code, property_type = "string", accept_language = NULL) {

  if (property_type == PropertyType$CODE) {
    rlang::expr(property_code({ { coding } }, { { property_code } }, { { accept_language } }))
  } else if (property_type == PropertyType$CODING) {
    rlang::expr(property_Coding({ { coding } }, { { property_code } }, { { accept_language } }))
  } else if (property_type == PropertyType$STRING) {
    rlang::expr(property_string({ { coding } }, { { property_code } }, { { accept_language } }))
  } else if (property_type == PropertyType$INTEGER) {
    rlang::expr(property_integer({ { coding } }, { { property_code } }, { { accept_language } }))
  } else if (property_type == PropertyType$BOOLEAN) {
    rlang::expr(property_boolean({ { coding } }, { { property_code } }, { { accept_language } }))
  } else if (property_type == PropertyType$DATETIME) {
    rlang::expr(property_dateTime({ { coding } }, { { property_code } }, { { accept_language } }))
  } else if (property_type == PropertyType$DECIMAL) {
    rlang::expr(property_decimal({ { coding } }, { { property_code } }, { { accept_language } }))
  } else {
    stop("Unsupported property type: ", property_type)
  }
}

#' Retrieves the values of designations for a Coding.
#'
#' \code{trm_designation()} takes a Coding column as its input. Returns the Column, which contains the values of
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
#' @family Terminology functions
#' 
#' @export
#' 
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' 
#' # Get the (first) value of the SNONED-CD designation code '900000000000003001'  
#' # for the first coding of the Condition resource code for language 'en'.
#' pc %>% pathlyr_example_resource('Condition') %>% 
#'      sparklyr::mutate(
#'             id, 
#'             designation = (!!trm_designation(code[['coding']][[0]], 
#'                      !!trm_to_snomed_coding('900000000000003001'), language = 'en'))[[0]], 
#'             .keep='none')
#' ptl_disconnect(pc)
trm_designation <- function(coding, use = NULL, language = NULL) {
  rlang::expr(designation({ { coding } }, { { use } }, { { language } }))
}
