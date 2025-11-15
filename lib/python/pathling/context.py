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

# noinspection PyPackageRequirements

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Sequence, TYPE_CHECKING

from pathling._version import (
    __java_version__,
    __scala_version__,
    __delta_version__,
    __hadoop_version__,
)
from pathling.fhir import MimeType

if TYPE_CHECKING:
    from .datasource import DataSources

__all__ = ["PathlingContext"]


class StorageType:
    MEMORY: str = "memory"
    DISK: str = "disk"


# noinspection PyProtectedMember
class PathlingContext:
    """
    Main entry point for Pathling API functionality.
    Should be instantiated with the :func:`PathlingContext.create` class method.

    Example use::

        pc = PathlingContext.create(spark)
        patient_df = pc.encode(spark.read.text('ndjson_resources'), 'Patient')
        patient_df.show()
    """

    @property
    def spark(self) -> SparkSession:
        """
        Returns the SparkSession associated with this context.
        """
        return self._spark

    @classmethod
    def create(
        cls,
        spark: Optional[SparkSession] = None,
        max_nesting_level: Optional[int] = 3,
        enable_extensions: Optional[bool] = False,
        enabled_open_types: Optional[Sequence[str]] = (
            "boolean",
            "code",
            "date",
            "dateTime",
            "decimal",
            "integer",
            "string",
            "Coding",
            "CodeableConcept",
            "Address",
            "Identifier",
            "Reference",
        ),
        enable_terminology: Optional[bool] = True,
        terminology_server_url: Optional[str] = "https://tx.ontoserver.csiro.au/fhir",
        terminology_verbose_request_logging: Optional[bool] = False,
        terminology_socket_timeout: Optional[int] = 60_000,
        max_connections_total: Optional[int] = 32,
        max_connections_per_route: Optional[int] = 16,
        terminology_retry_enabled: Optional[bool] = True,
        terminology_retry_count: Optional[int] = 2,
        enable_cache: Optional[bool] = True,
        cache_max_entries: Optional[int] = 200_000,
        cache_storage_type: Optional[str] = StorageType.MEMORY,
        cache_storage_path: Optional[str] = None,
        cache_default_expiry: Optional[int] = 600,
        cache_override_expiry: Optional[int] = None,
        token_endpoint: Optional[str] = None,
        enable_auth: Optional[bool] = False,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        scope: Optional[str] = None,
        token_expiry_tolerance: Optional[int] = 120,
        accept_language: Optional[str] = None,
        explain_queries: Optional[bool] = False,
        max_unbound_traversal_depth: Optional[int] = 10,
        enable_delta=False,
        enable_remote_debugging: Optional[bool] = False,
        debug_port: Optional[int] = 5005,
        debug_suspend: Optional[bool] = True,
    ) -> "PathlingContext":
        """
        Creates a :class:`PathlingContext` with the given configuration options. This should only
        be done once within a SparkSession - subsequent calls with different configuration may
        produce an error.

        If no SparkSession is provided, and there is not one already present in this process - a
        new SparkSession will be created.

        If a SparkSession is not provided, and one is already running within the current process,
        it will be reused - and it is assumed that the Pathling library API JAR is already on the
        classpath. If you are running your own cluster, make sure it is on the list of packages.

        If a SparkSession is provided, it needs to include the Pathling library API JAR on its
        classpath. You can get the path for the JAR (which is bundled with the Python package)
        using the `pathling.etc.find_jar` method.

        :param spark: a pre-configured :class:`SparkSession` instance, use this if you need to
               control the way that the session is set up
        :param max_nesting_level: controls the maximum depth of nested element data that is encoded
               upon import. This affects certain elements within FHIR resources that contain
               recursive references, e.g. `QuestionnaireResponse.item
               <https://hl7.org/fhir/R4/questionnaireresponse.html>`_.
        :param enable_extensions: enables support for FHIR extensions
        :param enabled_open_types: the list of types that are encoded within open types,
               such as extensions. This default list was taken from the data types that are common
               to extensions found in widely-used IGs, such as the US and AU base profiles. In
               general, you will get the best query performance by encoding your data with the
               shortest possible list.
        :param enable_terminology: enables the use of terminology functions
        :param terminology_server_url: the endpoint of a FHIR terminology service (R4) that the
               server can use to resolve terminology queries. The default server is suitable for
               testing purposes only.
        :param terminology_verbose_request_logging: setting this option to `True` will enable
               additional logging of the details of requests to the terminology service. Note that
               logging is subject to the Spark logging level, which you can set using
               `SparkContext.setLogLevel`. Verbose request logging is sent to the `DEBUG` logging
               level.
        :param terminology_socket_timeout: the maximum period (in milliseconds) that the server
               should wait for incoming data from the HTTP service
        :param max_connections_total: the maximum total number of connections for the client
        :param max_connections_per_route: the maximum number of connections per route for the client
        :param terminology_retry_enabled: controls whether terminology requests that fail for
               possibly transient reasons (network connections, DNS problems) should be retried
        :param terminology_retry_count: the number of times to retry failed terminology requests
        :param enable_cache: set this to false to disable caching of terminology requests (not
               recommended)
        :param cache_max_entries: sets the maximum number of entries that will be held in memory
        :param cache_storage_type: the type of storage to use for the terminology cache. See
               `StorageType`.
        :param cache_storage_path: the path on disk to use for the cache, required when
               `cache_storage_type` is `disk`
        :param cache_default_expiry: the default expiry time for cache entries (in seconds), used
               when the server does not provide an expiry value
        :param cache_override_expiry: if provided, this value overrides the expiry time provided by
               the terminology server
        :param enable_auth: enables authentication of requests to the terminology server
        :param token_endpoint: an OAuth2 token endpoint for use with the client credentials grant
        :param client_id: a client ID for use with the client credentials grant
        :param client_secret: a client secret for use with the client credentials grant
        :param scope: a scope value for use with the client credentials grant
        :param token_expiry_tolerance: the minimum number of seconds that a token should have
               before expiry when deciding whether to send it with a terminology request
        :param accept_language: the default value of the Accept-Language HTTP header passed to
               the terminology server. The value may contain multiple languages, with weighted
               preferences as defined in
               https://www.rfc-editor.org/rfc/rfc9110.html#name-accept-language. If not provided,
               the header is not sent. The server can use the header to return the result in the
               preferred language if it is able. The actual behaviour may depend on the server
               implementation and the code systems used.
        :param explain_queries: setting this option to `True` will enable additional logging relating
               to the query plan used to execute queries
        :param max_unbound_traversal_depth: maximum depth for self-referencing structure traversals
               in repeat operations. Controls how deeply nested hierarchical data can be flattened
               during projection.
        :param enable_delta: enables the use of Delta for storage of FHIR data.
               Only supported when no SparkSession is provided.
        :param enable_remote_debugging: enables remote debugging for the JVM process.
        :param debug_port: the port for the debugger to listen on (default: 5005)
        :param debug_suspend: if true, the JVM will suspend until a debugger is attached
        :return: a :class:`PathlingContext` instance initialized with the specified configuration
        """

        def _new_spark_session():
            spark_builder = (
                SparkSession.builder.config(
                    "spark.jars.packages",
                    f"au.csiro.pathling:library-runtime:{__java_version__},"
                    f"io.delta:delta-spark_{__scala_version__}:{__delta_version__},"
                )
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )

            # Add remote debugging configuration if enabled
            if enable_remote_debugging:
                suspend_option = "y" if debug_suspend else "n"
                debug_options = f"-agentlib:jdwp=transport=dt_socket,server=y,suspend={suspend_option},address={debug_port}"
                spark_builder = spark_builder.config("spark.driver.extraJavaOptions", debug_options)

            return spark_builder.getOrCreate()

        spark = spark or SparkSession.getActiveSession() or _new_spark_session()
        jvm = spark._jvm

        # Build an encoders configuration object from the provided parameters.
        encoders_config = (
            jvm.au.csiro.pathling.config.EncodingConfiguration.builder()
            .maxNestingLevel(max_nesting_level)
            .enableExtensions(enable_extensions)
            .openTypes(jvm.java.util.HashSet(enabled_open_types))
            .build()
        )

        # Build a terminology client configuration object from the provided parameters.
        client_config = (
            jvm.au.csiro.pathling.config.HttpClientConfiguration.builder()
            .socketTimeout(terminology_socket_timeout)
            .maxConnectionsTotal(max_connections_total)
            .maxConnectionsPerRoute(max_connections_per_route)
            .retryEnabled(terminology_retry_enabled)
            .retryCount(terminology_retry_count)
            .build()
        )

        # Build a terminology cache configuration object from the provided parameters.
        cache_storage_type_enum = (
            jvm.au.csiro.pathling.config.HttpClientCachingStorageType.fromCode(
                cache_storage_type
            )
        )
        cache_config = (
            jvm.au.csiro.pathling.config.HttpClientCachingConfiguration.builder()
            .enabled(enable_cache)
            .maxEntries(cache_max_entries)
            .storageType(cache_storage_type_enum)
            .storagePath(cache_storage_path)
            .defaultExpiry(cache_default_expiry)
            .overrideExpiry(cache_override_expiry)
            .build()
        )

        # Build a terminology authentication configuration object from the provided parameters.
        auth_config = (
            jvm.au.csiro.pathling.config.TerminologyAuthConfiguration.builder()
            .enabled(enable_auth)
            .tokenEndpoint(token_endpoint)
            .clientId(client_id)
            .clientSecret(client_secret)
            .scope(scope)
            .tokenExpiryTolerance(token_expiry_tolerance)
            .build()
        )

        # Build a terminology configuration object from the provided parameters.
        terminology_config = (
            jvm.au.csiro.pathling.config.TerminologyConfiguration.builder()
            .enabled(enable_terminology)
            .serverUrl(terminology_server_url)
            .verboseLogging(terminology_verbose_request_logging)
            .client(client_config)
            .cache(cache_config)
            .authentication(auth_config)
            .acceptLanguage(accept_language)
            .build()
        )

        # Build a query configuration object from the provided parameters.
        query_config = (
            jvm.au.csiro.pathling.config.QueryConfiguration.builder()
            .explainQueries(explain_queries)
            .maxUnboundTraversalDepth(max_unbound_traversal_depth)
            .build()
        )

        jpc: JavaObject = (
            jvm.au.csiro.pathling.library.PathlingContext.builder(spark._jsparkSession)
            .encodingConfiguration(encoders_config)
            .terminologyConfiguration(terminology_config)
            .queryConfiguration(query_config)
            .build()
        )
        return PathlingContext(spark, jpc)

    def __init__(self, spark: SparkSession, jpc: JavaObject) -> None:
        self._spark: SparkSession = spark
        self._jpc: JavaObject = jpc

    def _wrap_df(self, jdf: JavaObject) -> DataFrame:
        #
        # Before Spark v3.3 Dataframes were constructs with SQLContext, which was available
        # in `_wrapped` attribute of SparkSession.
        # Since v3.3 Dataframes are constructed with SparkSession instance directly.
        #
        return DataFrame(
            jdf,
            self._spark._wrapped if hasattr(self._spark, "_wrapped") else self._spark,
        )

    def version(self):
        """
        :return: The version of the Pathling library.
        """
        return self._jpc.getVersion()

    def encode(
        self,
        df: DataFrame,
        resource_name: str,
        input_type: Optional[str] = None,
        column: Optional[str] = None,
    ) -> DataFrame:
        """
        Takes a dataframe with a string representations of FHIR resources in the given column and
        encodes the resources of the given types as Spark dataframe.

        :param df: a :class:`DataFrame` containing the resources to encode.
        :param resource_name: the name of the FHIR resource to extract (Condition, Observation,
               etc.)
        :param input_type: the mime type of input string encoding. Defaults to
               `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If 'None' then the
               input dataframe is assumed to have one column of type string.
        :return: a :class:`DataFrame` containing the given type of resources encoded into Spark
                 columns
        """

        return self._wrap_df(
            self._jpc.encode(
                df._jdf, resource_name, input_type or MimeType.FHIR_JSON, column
            )
        )

    def encode_bundle(
        self,
        df: DataFrame,
        resource_name: str,
        input_type: Optional[str] = None,
        column: Optional[str] = None,
    ) -> DataFrame:
        """
        Takes a dataframe with a string representations of FHIR bundles  in the given column and
        encodes the resources of the given types as Spark dataframe.

        :param df: a :class:`DataFrame` containing the bundles with the resources to encode.
        :param resource_name: the name of the FHIR resource to extract (Condition, Observation,
               etc.)
        :param input_type: the MIME type of the input string encoding. Defaults to
               `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If 'None' then the
               input dataframe is assumed to have one column of type string.
        :return: a :class:`DataFrame` containing the given type of resources encoded into Spark
                 columns
        """
        return self._wrap_df(
            self._jpc.encodeBundle(
                df._jdf, resource_name, input_type or MimeType.FHIR_JSON, column
            )
        )


    @property
    def read(self) -> "DataSources":
        """
        Provides access to the instance of :class:`DataSource` factory.
        """
        from pathling.datasource import DataSources

        return DataSources(self)
