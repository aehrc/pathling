#'@import sparklyr

data_sources <- function(pc) {
  j_invoke(pc, "datasources")
}

#'@export
ds_from_ndjson_dir <- function(pc, ndjson_dir_uri) {
  pc %>% data_sources() %>% j_invoke("fromNdjsonDir", ndjson_dir_uri)
}
