#TODO: unfix the version somehow
spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/library-runtime-6.3.0.jar"),
        package = "pathlyr"
      )
    ),
    packages = c(
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
