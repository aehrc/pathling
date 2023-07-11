#' @import sparklyr


EQ_EQUIVALENT <- "equivalent"

StorageType <- list(
  MEMORY = "memory",
  DISK = "disk"
)


#' Creates a PathlingContext with the given configuration options.
#'
#' This should only be done once within a SparkSession - subsequent calls with different
#' configuration may produce an error.
#'
#' If no SparkSession is provided, and there is not one already present in this process - a new
#' SparkSession will be created.
#'
#' If a SparkSession is not provided, and one is already running within the current process, it
#' will be reused - and it is assumed that the Pathling library API JAR is already on the
#' classpath. If you are running your own cluster, make sure it is on the list of packages.
#'
#' @param cls The class object (not used in the implementation)
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
#' @param enable_delta Enables the use of Delta for storage of FHIR data
#'
#' @return A PathlingContext instance initialized with the specified configuration
#'
#' @examples
#' # Example usage
#' context <- create()
#' coding <- to_coding(c("code1", "code2"), "http://example.com/system")
#' member_of(coding, "http://example.com/value_set")
#'
#' @import sparklyr
#'
#' @export
ptl_connect <- function(
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
  accept_language = NULL,
  enable_delta = FALSE
) {
  invoke <- sparklyr::invoke

# TODO: Re-enable this to support delta
#   def_new_spark_session <- function() {
#     spark_builder <- SparkSession.builder.config("spark.jars", find_jar())
#     spark_builder <- if (enable_delta) {
#       spark_builder %>%
#         invoke(
#           "config",
#           "spark.jars.packages",
#           "io.delta:delta-core_2.12:2.3.0"
#         ) %>%
#         invoke(
#           "config",
#           "spark.sql.extensions",
#           "io.delta.sql.DeltaSparkSessionExtension"
#         ) %>%
#         invoke(
#           "config",
#           "spark.sql.catalog.spark_catalog",
#           "org.apache.spark.sql.delta.catalog.DeltaCatalog"
#         )
#     } else {
#       spark_builder
#     }
#     spark_builder$getOrCreate()
#   }

  spark <- if (!is.null(spark)) {
    spark
  } else {
    spark_connect(master = "local")
  }

  # TODO: Refactor - create  builder in Java for API config
  encoders_config <- invoke_static(
    spark, "au.csiro.pathling.config.EncodingConfiguration", "builder"
  ) %>%
    invoke("maxNestingLevel", as.integer(max_nesting_level)) %>%
    invoke("enableExtensions", as.logical(enable_extensions)) %>%
    #invoke("openTypes", as(jvm$java.util.HashSet(enabled_open_types), "java.util.Set")) %>%
    invoke("build")

  client_config <- invoke_static(
    spark, "au.csiro.pathling.config.HttpClientConfiguration", "builder"
  ) %>%
    invoke("socketTimeout", as.integer(terminology_socket_timeout)) %>%
    invoke("maxConnectionsTotal", as.integer(max_connections_total)) %>%
    invoke("maxConnectionsPerRoute", as.integer(max_connections_per_route)) %>%
    invoke("retryEnabled", as.logical(terminology_retry_enabled)) %>%
    invoke("retryCount", as.integer(terminology_retry_count)) %>%
    invoke("build")

  cache_storage_type_enum <- invoke_static(
    spark, "au.csiro.pathling.config.HttpClientCachingStorageType", "fromCode",
    as.character(cache_storage_type)
  )

  cache_config <- invoke_static(
    spark, "au.csiro.pathling.config.HttpClientCachingConfiguration", "builder"
  ) %>%
    invoke("enabled", as.logical(enable_cache)) %>%
    invoke("maxEntries", as.integer(cache_max_entries)) %>%
    invoke("storageType", cache_storage_type_enum) %>%
    invoke("storagePath", cache_storage_path) %>%
    invoke("defaultExpiry", as.integer(cache_default_expiry)) %>%
    invoke("overrideExpiry", cache_override_expiry) %>%
    invoke("build")

  auth_config <- invoke_static(
    spark, "au.csiro.pathling.config.TerminologyAuthConfiguration", "builder"
  ) %>%
    invoke("enabled", as.logical(enable_auth)) %>%
    invoke("tokenEndpoint", token_endpoint) %>%
    invoke("clientId", client_id) %>%
    invoke("clientSecret", client_secret) %>%
    invoke("scope", scope) %>%
    invoke("tokenExpiryTolerance", as.integer(token_expiry_tolerance)) %>%
    invoke("build")

  terminology_config <- invoke_static(
    spark, "au.csiro.pathling.config.TerminologyConfiguration", "builder"
  ) %>%
    invoke("enabled", as.logical(enable_terminology)) %>%
    invoke("serverUrl", terminology_server_url) %>%
    invoke("verboseLogging", as.logical(terminology_verbose_request_logging)) %>%
    invoke("client", client_config) %>%
    invoke("cache", cache_config) %>%
    invoke("authentication", auth_config) %>%
    invoke("acceptLanguage", accept_language) %>%
    invoke("build")

  sparklyr::invoke_static(spark, "au.csiro.pathling.library.PathlingContext", "create",
                spark_session(spark), encoders_config, terminology_config)
}
