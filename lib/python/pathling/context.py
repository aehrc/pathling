#  Copyright 2022 Commonwealth Scientific and Industrial Research
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

from typing import Optional, Sequence

from deprecated import deprecated

# noinspection PyPackageRequirements
from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession, Column

from pathling.coding import Coding
from pathling.etc import find_jar
from pathling.fhir import MimeType

__all__ = ["PathlingContext"]

EQ_EQUIVALENT = "equivalent"


class StorageType:
    MEMORY: str = "memory"
    DISK: str = "disk"


# noinspection PyProtectedMember
class PathlingContext:
    """
    Main entry point for Pathling API functionality.
    Should be instantiated with the :func:`PathlingContext.create` class method.

    Example use::

        ptl = PathlingContext.create(spark)
        patient_df = ptl.encode(spark.read.text('ndjson_resources'), 'Patient')
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
        fhir_version: Optional[str] = None,
        max_nesting_level: Optional[int] = None,
        enable_extensions: Optional[bool] = None,
        enabled_open_types: Optional[Sequence[str]] = None,
        terminology_server_url: Optional[str] = None,
        terminology_socket_timeout: Optional[int] = None,
        terminology_verbose_request_logging: Optional[bool] = None,
        max_connections_total: Optional[int] = None,
        max_connections_per_route: Optional[int] = None,
        terminology_retry_enabled: Optional[bool] = None,
        terminology_retry_count: Optional[int] = None,
        cache_max_entries: Optional[int] = None,
        cache_storage_type: Optional[str] = StorageType.MEMORY,
        cache_storage_path: Optional[str] = None,
        cache_default_expiry: Optional[int] = None,
        cache_override_expiry: Optional[int] = None,
        token_endpoint: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        scope: Optional[str] = None,
        token_expiry_tolerance: Optional[int] = None,
        mock_terminology: bool = False,
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

        http services.
        :param spark: the :class:`SparkSession` instance.
        :param fhir_version: the FHIR version to use.
            Must a valid FHIR version string. Defaults to R4.
        :param max_nesting_level: the maximum nesting level for recursive data types.
            Zero (0) indicates that all direct or indirect fields of type T in element of type T
            should be skipped
        :param enable_extensions: switches on/off the support for FHIR extensions
        :param enabled_open_types: list of types that are encoded within open types, such as
            extensions
        :param terminology_server_url: the URL of the FHIR terminology server used to resolve
            terminology queries
        :param terminology_socket_timeout: the socket timeout for terminology server requests
        :param terminology_verbose_request_logging: enables verbose logging of terminology server
        requests
        :param max_connections_total: the maximum total number of connections for  http services.
        :param max_connections_per_route: the maximum number of connections per route for
        :param terminology_retry_enabled: enables retrying of terminology server requests
        :param terminology_retry_count: the maximum number of times to retry terminology requests
        :param cache_max_entries: the maximum number of cached entries.
        :param cache_storage_type: the type of cache storage to use for http service. By default,
        uses transient in-memory cache. 'None' disables caching all together.
        :param cache_storage_path: the path on disk where the cache is stored when the storage
        type is 'disk'.
        :param cache_default_expiry: the amount of time (in seconds) that a response from the
        terminology server should be cached if the server does not specify an expiry.
        :param cache_override_expiry: if provided, this value overrides the expiry time provided
        by the terminology server.
        :param token_endpoint: an OAuth2 token endpoint for use with the client credentials grant
        :param client_id: a client ID for use with the client credentials grant
        :param client_secret: a client secret for use with the client credentials grant
        :param scope: a scope value for use with the client credentials grant
        :param token_expiry_tolerance: the minimum number of seconds that a token should have
            before expiry when deciding whether to send it with a terminology request
        :return: a DataFrame containing the given resource encoded into Spark columns
        """
        spark = (
            spark
            or SparkSession.getActiveSession()
            or SparkSession.builder.config("spark.jars", find_jar()).getOrCreate()
        )
        jvm = spark._jvm

        # Build a Java configuration object from the provided parameters.
        config = (
            jvm.au.csiro.pathling.library.PathlingContextConfiguration.builder()
            .fhirVersion(fhir_version)
            .maxNestingLevel(max_nesting_level)
            .extensionsEnabled(enable_extensions)
            .openTypesEnabled(enabled_open_types)
            .terminologyServerUrl(terminology_server_url)
            .terminologySocketTimeout(terminology_socket_timeout)
            .terminologyVerboseRequestLogging(terminology_verbose_request_logging)
            .maxConnectionsTotal(max_connections_total)
            .maxConnectionsPerRoute(max_connections_per_route)
            .terminologyRetryEnabled(terminology_retry_enabled)
            .terminologyRetryCount(terminology_retry_count)
            .cacheMaxEntries(cache_max_entries)
            .cacheStorageType(cache_storage_type)
            .cacheStoragePath(cache_storage_path)
            .cacheDefaultExpiry(cache_default_expiry)
            .cacheOverrideExpiry(cache_override_expiry)
            .tokenEndpoint(token_endpoint)
            .clientId(client_id)
            .clientSecret(client_secret)
            .scope(scope)
            .tokenExpiryTolerance(token_expiry_tolerance)
            .mockTerminology(mock_terminology)
            .build()
        )

        jpc: JavaObject = jvm.au.csiro.pathling.library.PathlingContext.create(
            spark._jsparkSession, config
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

    def encode(
        self,
        df: DataFrame,
        resource_name: str,
        input_type: Optional[MimeType] = None,
        column: Optional[str] = None,
    ) -> DataFrame:
        """
        Takes a dataframe with a string representations of FHIR resources  in the given column and
        encodes the resources of the given types as Spark dataframe.

        :param df: a :class:`DataFrame` containing the resources to encode.
        :param resource_name: the name of the FHIR resource to extract
            (Condition, Observation, etc).
        :param input_type: the mime type of input string encoding.
            Defaults to `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If None then
            the input dataframe is assumed to have one column of type string.
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
        input_type: Optional[MimeType] = None,
        column: Optional[str] = None,
    ) -> DataFrame:
        """
        Takes a dataframe with a string representations of FHIR bundles  in the given column and
        encodes the resources of the given types as Spark dataframe.

        :param df: a :class:`DataFrame` containing the bundles with the resources to encode.
        :param resource_name: the name of the FHIR resource to extract
            (condition, observation, etc).
        :param input_type: the mime type of input string encoding.
            Defaults to `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If None then
            the input dataframe is assumed to have one column of type string.
        :return: a :class:`DataFrame` containing the given type of resources encoded into Spark
            columns
        """
        return self._wrap_df(
            self._jpc.encodeBundle(
                df._jdf, resource_name, input_type or MimeType.FHIR_JSON, column
            )
        )

    @deprecated(reason="You should use the 'udfs.member_of' UDF instead")
    def member_of(
        self,
        df: DataFrame,
        coding_column: Column,
        value_set_uri: str,
        output_column_name: str,
    ):
        """
        Takes a dataframe with a Coding column as input. A new column is created which contains a
        Boolean value, indicating whether the input Coding is a member of the specified FHIR
        ValueSet.

        :param df: a DataFrame containing the input data
        :param coding_column: a Column containing a struct representation of a Coding
        :param value_set_uri: an identifier for a FHIR ValueSet
        :param output_column_name: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        return self._wrap_df(
            self._jpc.memberOf(
                df._jdf, coding_column._jc, value_set_uri, output_column_name
            )
        )

    @deprecated(reason="You should use the 'udfs.translate' UDF instead")
    def translate(
        self,
        df: DataFrame,
        coding_column: Column,
        concept_map_uri: str,
        reverse: Optional[bool] = False,
        equivalence: Optional[str] = EQ_EQUIVALENT,
        target: Optional[str] = None,
        output_column_name: Optional[str] = "result",
    ):
        """
        Takes a dataframe with a Coding column as input. A new column is created which contains
        the array of Codings value with translation targets from the specified FHIR ConceptMap.
        There may be more than one target concept for each input concept.

        :param df: a DataFrame containing the input data
        :param coding_column: a Column containing a struct representation of a Coding
        :param concept_map_uri: an identifier for a FHIR ConceptMap
        :param reverse: the direction to traverse the map - false results in "source to target"
            mappings, while true results in "target to source"
        :param equivalence: a comma-delimited set of values from the ConceptMapEquivalence ValueSet
        :param target: identifies the value set in which a translation is sought.  If there's no
            target specified, the server should return all known translations.
        :param output_column_name: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        return self._wrap_df(
            self._jpc.translate(
                df._jdf,
                coding_column._jc,
                concept_map_uri,
                reverse,
                equivalence,
                target,
                output_column_name,
            )
        )

    @deprecated(reason="You should use the 'udfs.subsumes' UDF instead")
    def subsumes(
        self,
        df: DataFrame,
        output_column_name: str,
        left_coding_column: Optional[Column] = None,
        right_coding_column: Optional[Column] = None,
        left_coding: Optional[Coding] = None,
        right_coding: Optional[Coding] = None,
    ):
        """
        Takes a dataframe with two Coding columns. A new column is created which contains a
        Boolean value, indicating whether the left Coding subsumes the right Coding.

        :param df: a DataFrame containing the input data
        :param left_coding_column: a Column containing a struct representation of a Coding,
            for the left-hand side of the subsumption test
        :param right_coding_column: a Column containing a struct representation of a Coding,
            for the right-hand side of the subsumption test
        :param left_coding: a Coding object for the left-hand side of the subsumption test
        :param right_coding: a Coding object for the right-hand side of the subsumption test
        :param output_column_name: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        if (left_coding_column is None and left_coding is None) or (
            right_coding_column is None and right_coding is None
        ):
            raise ValueError(
                "Must provide either left_coding_column or left_coding, and either "
                "right_coding_column or right_coding"
            )
        left_column = left_coding.to_literal() if left_coding else left_coding_column
        right_column = (
            right_coding.to_literal() if right_coding else right_coding_column
        )
        return self._wrap_df(
            self._jpc.subsumes(
                df._jdf, left_column._jc, right_column._jc, output_column_name
            )
        )
