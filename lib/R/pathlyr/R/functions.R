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


#' Functions converting codes into a Column that contains a Coding struct.
#'
#' @rdname trm_to_coding
#' @name trm_to_xxxx_coding
#' 
#' @details 
#' The Coding struct Column can be used as an input to terminology functions such 
#' as \code{\link{trm_member_of}} and \code{\link{trm_translate}}.
#' Please note that inside \code{sparklyr} verbs such as \code{mutate} the functions calls need to 
#' be preceeded with \code{!!}, e.g: \code{!!trm_to_coding(CODE, SNOMED_URI)}.
#'
#' @param coding_column The Column containing the codes.
#' @param system The URI of the system the codes belong to.
#' @param version The version of the code system.
#'
#' @return A Column containing a Coding struct.
#'
#' @examplesIf ptl_is_spark_installed()
#' pc <- ptl_connect()
#' condition_df <- ptl_spark(pc) %>% sparklyr::copy_to(conditions)
#' 
#' # Convert codes to codins with explicit system
#' condition_df %>% sparklyr::mutate(snomedCoding = !!trm_to_coding(CODE, SNOMED_URI), .keep = 'none')
#'
#' # Convert codes to SNOMED codings
#' condition_df %>% sparklyr::mutate(snomedCoding = !!trm_to_snomed_coding(CODE), .keep = 'none')
#' 
#' ptl_disconnect(pc)
NULL

#' @rdname trm_to_coding
#' 
#' @family terminology helpers
#' 
#' @description
#' \code{trm_to_coding()} converts a Column containing codes into a Column that contains a Coding struct.
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

#' @rdname trm_to_coding
#' 
#' @description
#' \code{trm_to_snomed_coding()} converts a Column containing codes into a Column that 
#' contains a SNOMED Coding struct.
#'
#' @family terminlogy helpers
#
#' @export
trm_to_snomed_coding <- function(coding_column, version = NULL) {
  trm_to_coding({ { coding_column } }, SNOMED_URI, { { version } })
}

#' Terminology helper functions
#' 
#' @description
#' \code{trm_to_ecl_value_set} converts a SNOMED CT ECL expression into a FHIR ValueSet URI. 
#' It can be used with the `\code{\link{trm_member_of}} function.
#'
#' @param ecl The ECL expression.
#'
#' @return The ValueSet URI.
#'
#' @family terminlogy helpers
#' 
#' @importFrom utils URLencode
#' 
#' @export
#' 
#' @examplesIf ptl_is_spark_installed()
#' # Example usage of trm_to_ecl_value_set function
#' trm_to_ecl_value_set('<<373265006 |Analgesic (substance)|')
trm_to_ecl_value_set <- function(ecl) {
  paste0(SNOMED_URI, "?fhir_vs=ecl/", URLencode(ecl, reserved = TRUE))
}




