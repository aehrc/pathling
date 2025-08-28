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

#' SNOMED CT system URI
#'
#' The URI of the SNOMED CT code system: \code{http://snomed.info/sct}.
#' 
#' @seealso \href{https://terminology.hl7.org/SNOMEDCT.html}{Using SNOMED CT with HL7 Standards}
#' 
#' @export
SNOMED_URI <- "http://snomed.info/sct"

#' LOINC system URI
#'
#' The URI of the LOINC code system: \code{http://loinc.org}.
#' 
#' @seealso \href{https://terminology.hl7.org/LOINC.html}{Using LOINC with HL7 Standards}
#'
#' @export
LOINC_URI <- "http://loinc.org"

#' Convert codes to Coding structures
#' 
#' Converts a Column containing codes into a Column that contains a Coding struct.
#' 
#' The Coding struct Column can be used as an input to terminology functions such as 
#' \code{\link{tx_member_of}} and \code{\link{tx_translate}}. Please note that inside 
#' \code{sparklyr} verbs such as \code{mutate} the functions calls need to be preceded with 
#' \code{!!}, e.g: \code{!!tx_to_coding(CODE, SNOMED_URI)}.
#' 
#' @param coding_column The Column containing the codes.
#' @param system The URI of the system the codes belong to.
#' @param version The version of the code system.
#'
#' @return A Column containing a Coding struct.
#' 
#' @seealso \href{https://hl7.org/fhir/R4/datatypes.html#Coding}{FHIR R4 - Coding}
#'
#' @family terminology helpers
#'
#' @export
#' 
#' @examples \dontrun{
#' condition_df <- pathling_spark(pc) %>% sparklyr::copy_to(conditions)
#' 
#' # Convert codes to ICD-10 codings.
#' condition_df %>% sparklyr::mutate(
#'     icdCoding = !!tx_to_coding(CODE, "http://hl7.org/fhir/sid/icd-10"), .keep = 'none'
#' )
#' }
tx_to_coding <- function(coding_column, system, version = NULL) {
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

#' Convert SNOMED CT codes to Coding structures
#' 
#' Converts a Column containing codes into a Column that contains a SNOMED Coding struct.
#' 
#' The Coding struct Column can be used as an input to terminology functions such as 
#' \code{\link{tx_member_of}} and \code{\link{tx_translate}}. Please note that inside 
#' \code{sparklyr} verbs such as \code{mutate} the functions calls need to be preceded with 
#' \code{!!}, e.g: \code{!!tx_to_coding(CODE, SNOMED_URI)}.
#' 
#' @param coding_column The Column containing the codes.
#' @param version The version of the code system.
#' 
#' @return A Column containing a Coding struct.
#'
#' @family terminology helpers
#
#' @export
#' 
#' @examples \dontrun{
#' condition_df <- pathling_spark(pc) %>% sparklyr::copy_to(conditions)
#' 
#' # Convert codes to SNOMED CT codings.
#' # Equivalent to: tx_to_coding(CODE, "http://snomed.info/sct")
#' condition_df %>% sparklyr::mutate(snomedCoding = !!tx_to_snomed_coding(CODE), .keep = 'none')
#' }
tx_to_snomed_coding <- function(coding_column, version = NULL) {
  tx_to_coding({ { coding_column } }, SNOMED_URI, { { version } })
}

#' Convert LOINC codes to Coding structures
#'
#' Converts a Column containing codes into a Column that contains a LOINC Coding struct.
#' 
#' The Coding struct Column can be used as an input to terminology functions such as 
#' \code{\link{tx_member_of}} and \code{\link{tx_translate}}. Please note that inside 
#' \code{sparklyr} verbs such as \code{mutate} the functions calls need to be preceded with 
#' \code{!!}, e.g: \code{!!tx_to_coding(CODE, SNOMED_URI)}.
#' 
#' @param coding_column The Column containing the codes.
#' @param version The version of the code system.
#' 
#' @return A Column containing a Coding struct.
#' 
#' @family terminology helpers
#' 
#' @export
#' 
#' @examples \dontrun{
#' condition_df <- pathling_spark(pc) %>% sparklyr::copy_to(conditions)
#' 
#' # Convert codes to LOINC codings.
#' # Equivalent to: tx_to_coding(CODE, "http://loinc.org")
#' condition_df %>% sparklyr::mutate(loincCoding = !!tx_to_loinc_coding(CODE), .keep = 'none')
#' }
tx_to_loinc_coding <- function(coding_column, version = NULL) {
  tx_to_coding({ { coding_column } }, LOINC_URI, { { version } })
}

#' Convert a SNOMED CT ECL expression to a ValueSet URI
#' 
#' Converts a SNOMED CT ECL expression into a FHIR ValueSet URI. It can be used with the 
#'`\code{\link{tx_member_of}} function.
#'
#' @param ecl The ECL expression.
#'
#' @return The ValueSet URI.
#' 
#' @seealso \href{https://terminology.hl7.org/SNOMEDCT.html#snomed-ct-implicit-value-sets}{Using SNOMED CT with HL7 Standards - Implicit Value Sets}
#'
#' @family terminology helpers
#' 
#' @importFrom utils URLencode
#' 
#' @export
#' 
#' @examples \dontrun{
#' # Example usage of tx_to_ecl_value_set function
#' tx_to_ecl_value_set('<<373265006 |Analgesic (substance)|')
#' }
tx_to_ecl_value_set <- function(ecl) {
  paste0(SNOMED_URI, "?fhir_vs=ecl/", URLencode(ecl, reserved = TRUE))
}
