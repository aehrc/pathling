#
# This file is part of the Pathlyr package for R.
# Terminology functions
#

#' @export
SNOMED_URI = 'http://snomed.info/sct'

#' @export
snomed_code <-function(code) {
  rlang::expr(struct(NULL, SNOMED_URI, NULL, string({{code}}), NULL, NULL))
}

#' @export
trm_member_of <-function(coding, value_set) {
  rlang::expr(member_of({{coding}}, {{value_set}}))
}
