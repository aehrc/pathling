#' @import sparklyr

#' @param sc A sparkyr spark connection
#' @return The result of some calculation
#' @export
ptl_connect <- function(sc, max_nesting_level=3, enable_extensions=FALSE) {
  invoke <- sparklyr::invoke
  # Build an encoders configuration object from the provided parameters.
  encoders_config <-invoke_static(sc, "au.csiro.pathling.config.EncodingConfiguration", "builder") %>%
    invoke("maxNestingLevel", as.integer(max_nesting_level)) %>%
    invoke("enableExtensions", as.logical(enable_extensions)) %>%
    invoke("build")
  sparklyr::invoke_static(sc, "au.csiro.pathling.library.PathlingContext", "create",
                spark_session(sc), encoders_config)
}
