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

#' The URI of the SNOMED code system.
#' @export
SNOMED_URI <- "http://snomed.info/sct"

#' The URI of the LOINC code system.
#' @export
LOINC_URI <- "http://loinc.org"

#' Converts a Column containing codes into a Column that contains a Coding struct.
#'
#' The Coding struct Column can be used as an input to terminology functions such as `member_of` and `translate`.
#'
#' @param coding_column The Column containing the codes.
#' @param system The URI of the system the codes belong to.
#' @param version The version of the code system.
#'
#' @return A Column containing a Coding struct.
#'
#' @examples
#' # Example usage of trm_to_coding function
#' trm_to_coding(coding_column, system, version = NULL)
#'
#' @export
trm_to_coding <- function(coding_column, system, version = NULL) {
  rlang::expr(if (!is.null({ { coding_column } }))
                  struct(
                      NULL,
                      string({ { system } }),
                      string({ { version } }),
                      string({ { coding_column } }),
                      NULL,
                      NULL
                  ) else NULL)
}

#' Converts a Column containing codes into a Column that contains a SNOMED Coding struct.
#'
#' The Coding struct Column can be used as an input to terminology functions such as `member_of` and `translate`.
#'
#' @param coding_column The Column containing the codes.
#' @param version The version of the code system.
#'
#' @return A Column containing a Coding struct.
#'
#' @examples
#' # Example usage of trm_to_snomed_coding function
#' trm_to_snomed_coding(coding_column, version = NULL)
#'
#' @export
trm_to_snomed_coding <- function(coding_column, version = NULL) {
  trm_to_coding({ { coding_column } }, SNOMED_URI, { { version } })
}


#' Converts a SNOMED CT ECL expression into a FHIR ValueSet URI.
#'
#' Can be used with the `member_of` function.
#'
#' @param ecl The ECL expression.
#'
#' @return The ValueSet URI.
#'
#' @examples
#' # Example usage of trm_to_ecl_value_set function
#' trm_to_ecl_value_set('<<373265006 |Analgesic (substance)|')
#'
#' @importFrom utils URLencode
#' 
#' @export
trm_to_ecl_value_set <- function(ecl) {
  # TODO: Check that reserved true captures: urllib.parse.quote(ecl, safe="()*!'")
  paste0(SNOMED_URI, "?fhir_vs=ecl/", URLencode(ecl, reserved = TRUE))
}




