#' @import sparklyr

#'@export
ptl_encode <-function(pc, text_sdf, resource_type) {
  sdf_register(j_invoke(pc, 'encode', spark_dataframe(text_sdf), resource_type))
}
