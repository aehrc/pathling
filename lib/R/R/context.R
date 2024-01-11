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


#' Terminology cache storage type
#' 
#' The type of storage to use for the terminology cache.
#'
#' The following values are supported:
#' \itemize{
#'   \item \code{MEMORY} - Use an in-memory cache
#'   \item \code{DISK} - Use a disk-based cache
#' }
#' 
#' @export
StorageType <- list(
    MEMORY = "memory",
    DISK = "disk"
)

#' Create or retrieve the Pathling context
#' 
#' Creates a Pathling context with the given configuration options.
#'
#' If no Spark session is provided and there is not one already present in this process, a new
#' one will be created.
#'
#' If a SparkSession is not provided, and one is already running within the current process, it
#' will be reused.
#'
#' It is assumed that the Pathling library API JAR is already on the classpath. If you are running 
#' your own cluster, make sure it is on the list of packages.
#' 
#' @param spark A pre-configured SparkSession instance, use this if you need to control the way
#'   that the session is set up
#' @param max_nesting_level Controls the maximum depth of nested element data that is encoded
#'   upon import. This affects certain elements within FHIR resources that contain recursive
#'   references, e.g., QuestionnaireResponse.item.
#' @param enable_extensions Enables support for FHIR extensions
#' @param enabled_open_types The list of types that are encoded within open types, such as
#'   extensions.
#' @param enable_terminology Enables the use of terminology functions
#' @param terminology_server_url The endpoint of a FHIR terminology service (R4) that the server
#'   can use to resolve terminology queries.
#' @param terminology_verbose_request_logging Setting this option to TRUE will enable additional
#'   logging of the details of requests to the terminology service.
#' @param terminology_socket_timeout The maximum period (in milliseconds) that the server should
#'   wait for incoming data from the HTTP service
#' @param max_connections_total The maximum total number of connections for the client
#' @param max_connections_per_route The maximum number of connections per route for the client
#' @param terminology_retry_enabled Controls whether terminology requests that fail for possibly
#'   transient reasons should be retried
#' @param terminology_retry_count The number of times to retry failed terminology requests
#' @param enable_cache Set this to FALSE to disable caching of terminology requests
#' @param cache_max_entries Sets the maximum number of entries that will be held in memory
#' @param cache_storage_type The type of storage to use for the terminology cache
#' @param cache_storage_path The path on disk to use for the cache
#' @param cache_default_expiry The default expiry time for cache entries (in seconds)
#' @param cache_override_expiry If provided, this value overrides the expiry time provided by the
#'   terminology server
#' @param token_endpoint An OAuth2 token endpoint for use with the client credentials grant
#' @param enable_auth Enables authentication of requests to the terminology server
#' @param client_id A client ID for use with the client credentials grant
#' @param client_secret A client secret for use with the client credentials grant
#' @param scope A scope value for use with the client credentials grant
#' @param token_expiry_tolerance The minimum number of seconds that a token should have before
#'   expiry when deciding whether to send it with a terminology request
#' @param accept_language The default value of the Accept-Language HTTP header passed to the
#'   terminology server
#'
#' @return A Pathling context instance initialized with the specified configuration
#'
#' @importFrom sparklyr j_invoke_static j_invoke
#' 
#' @family context lifecycle functions
#'
#' @export
#' 
#' @examplesIf pathling_is_spark_installed()
#' # Create PathlingContext for an existing Spark connecton.
#' sc <- sparklyr::spark_connect(master = "local")
#' pc <- pathling_connect(spark = sc)
#' pathling_disconnect(pc)
#' 
#' # Create PathlingContext with a new Spark connection.
#' pc <- pathling_connect()
#' spark <- pathling_spark(pc)
#' pathling_disconnect_all()
pathling_connect <- function(
    spark = NULL,
    max_nesting_level = 3,
    enable_extensions = FALSE,
    enabled_open_types = c(
        "boolean", "code", "date", "dateTime", "decimal", "integer",
        "string", "Coding", "CodeableConcept", "Address", "Identifier", "Reference"
    ),
    enable_terminology = TRUE,
    terminology_server_url = "https://tx.ontoserver.csiro.au/fhir",
    terminology_verbose_request_logging = FALSE,
    terminology_socket_timeout = 60000,
    max_connections_total = 32,
    max_connections_per_route = 16,
    terminology_retry_enabled = TRUE,
    terminology_retry_count = 2,
    enable_cache = TRUE,
    cache_max_entries = 200000,
    cache_storage_type = StorageType$MEMORY,
    cache_storage_path = NULL,
    cache_default_expiry = 600,
    cache_override_expiry = NULL,
    token_endpoint = NULL,
    enable_auth = FALSE,
    client_id = NULL,
    client_secret = NULL,
    scope = NULL,
    token_expiry_tolerance = 120,
    accept_language = NULL
) {


  spark_info <- pathling_spark_info()


  new_spark_connection <- function() {
    sparklyr::spark_connect(master = "local[*]", config = list("sparklyr.shell.conf" = c(
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )), version = spark_info$spark_version)
  }

  spark <- if (!is.null(spark)) {
    spark
  } else {
    new_spark_connection()
  }

  spark_runtime_version <- sparklyr::spark_version(spark)
  if (package_version(spark_runtime_version) < spark_info$spark_version) {
    warning(sprintf("Incompatible version of spark: %s, while Pathling requires at least: %s",
                    spark_runtime_version, spark_info$spark_version))
  }

  encoders_config <- spark %>%
      j_invoke_static(
          "au.csiro.pathling.config.EncodingConfiguration", "builder") %>%
      j_invoke("maxNestingLevel", as.integer(max_nesting_level)) %>%
      j_invoke("enableExtensions", as.logical(enable_extensions)) %>%
      j_invoke("openTypes", spark %>% j_to_set(enabled_open_types)) %>%
      j_invoke("build")

  client_config <- j_invoke_static(
      spark, "au.csiro.pathling.config.HttpClientConfiguration", "builder"
  ) %>%
      j_invoke("socketTimeout", as.integer(terminology_socket_timeout)) %>%
      j_invoke("maxConnectionsTotal", as.integer(max_connections_total)) %>%
      j_invoke("maxConnectionsPerRoute", as.integer(max_connections_per_route)) %>%
      j_invoke("retryEnabled", as.logical(terminology_retry_enabled)) %>%
      j_invoke("retryCount", as.integer(terminology_retry_count)) %>%
      j_invoke("build")

  cache_storage_type_enum <- j_invoke_static(
      spark, "au.csiro.pathling.config.HttpClientCachingStorageType", "fromCode",
      as.character(cache_storage_type)
  )

  cache_config <- j_invoke_static(
      spark, "au.csiro.pathling.config.HttpClientCachingConfiguration", "builder"
  ) %>%
      j_invoke("enabled", as.logical(enable_cache)) %>%
      j_invoke("maxEntries", as.integer(cache_max_entries)) %>%
      j_invoke("storageType", cache_storage_type_enum) %>%
      j_invoke("storagePath", cache_storage_path) %>%
      j_invoke("defaultExpiry", as.integer(cache_default_expiry)) %>%
      j_invoke("overrideExpiry", cache_override_expiry) %>%
      j_invoke("build")

  auth_config <- j_invoke_static(
      spark, "au.csiro.pathling.config.TerminologyAuthConfiguration", "builder"
  ) %>%
      j_invoke("enabled", as.logical(enable_auth)) %>%
      j_invoke("tokenEndpoint", token_endpoint) %>%
      j_invoke("clientId", client_id) %>%
      j_invoke("clientSecret", client_secret) %>%
      j_invoke("scope", scope) %>%
      j_invoke("tokenExpiryTolerance", as.integer(token_expiry_tolerance)) %>%
      j_invoke("build")

  terminology_config <- j_invoke_static(
      spark, "au.csiro.pathling.config.TerminologyConfiguration", "builder"
  ) %>%
      j_invoke("enabled", as.logical(enable_terminology)) %>%
      j_invoke("serverUrl", terminology_server_url) %>%
      j_invoke("verboseLogging", as.logical(terminology_verbose_request_logging)) %>%
      j_invoke("client", client_config) %>%
      j_invoke("cache", cache_config) %>%
      j_invoke("authentication", auth_config) %>%
      j_invoke("acceptLanguage", accept_language) %>%
      j_invoke("build")

  j_invoke_static(spark, "au.csiro.pathling.library.PathlingContext", "create",
                  sparklyr::spark_session(spark),
                  encoders_config, terminology_config)
}

#' Get the Spark session
#' 
#' Returns the Spark connection associated with a Pathling context.
#'
#' @param pc The PathlingContext object.
#' 
#' @return The Spark connection associated with this Pathling context.
#' 
#' @family context lifecycle functions
#' 
#' @export
pathling_spark <- function(pc) {
  sparklyr::spark_connection(pc)
}

#' Disconnect from the Spark session
#'
#' Disconnects the Spark connection associated with a Pathling context.
#' 
#' @param pc The PathlingContext object.
#' 
#' @return No return value, called for side effects only.
#' 
#' @family context lifecycle functions
#'
#' @export
pathling_disconnect <- function(pc) {
  sparklyr::spark_disconnect(pathling_spark(pc))
  invisible(NULL)
}

#' Disconnect all Spark connections
#' 
#' @return No return value, called for side effects only.
#' 
#' @family context lifecycle functions
#' 
#' @export
pathling_disconnect_all <- function() {
  sparklyr::spark_disconnect_all()
  invisible(NULL)
}
