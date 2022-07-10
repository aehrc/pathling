# noinspection PyPackageRequirements
from typing import Optional, Sequence

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession, Column

from pathling.coding import Coding
from pathling.etc import find_jar
from pathling.fhir import MimeType

__all__ = ["PathlingContext"]

EQ_EQUIVALENT = "equivalent"


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
    def create(cls, spark: Optional[SparkSession] = None,
               fhir_version: Optional[str] = None,
               max_nesting_level: Optional[int] = None,
               enable_extensions: Optional[bool] = None,
               enabled_open_types: Optional[Sequence[str]] = None,
               terminology_server_url: Optional[str] = None) -> "PathlingContext":
        """
        Creates a :class:`PathlingContext` with the given configuration options.

        If no SparkSession is provided, and there is not one already present in this process - a
        new SparkSession will be created.

        If a SparkSession is not provided, and one is already running within the current process,
        it will be reused - and it is assumed that the Pathling library API JAR is already on the
        classpath. If you are running your own cluster, make sure it is on the list of packages.

        If a SparkSession is provided, it needs to include the Pathling library API JAR on its
        classpath. You can get the path for the JAR (which is bundled with the Python package)
        using the `pathling.etc.find_jar` method.

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
        :return: a DataFrame containing the given resource encoded into Spark columns
        """
        spark = (spark or
                 SparkSession.getActiveSession() or
                 SparkSession.builder
                 .config('spark.jars', find_jar())
                 .getOrCreate())
        jvm: JavaObject = spark._jvm
        jpc: JavaObject = jvm.au.csiro.pathling.library.PathlingContext.create(
                spark._jsparkSession, fhir_version, max_nesting_level, enable_extensions,
                enabled_open_types, terminology_server_url)
        return PathlingContext(spark, jpc)

    def __init__(self, spark: SparkSession, jpc: JavaObject) -> None:
        self._spark: SparkSession = spark
        self._jpc: JavaObject = jpc

    def _wrap_df(self, jdf: JavaObject) -> DataFrame:
        #
        # Before Spark v3.3 Dataframes were constructs with SQLContext, which was available
        # in `_wrapped` attribute of SparkSession.
        # Since v3.3 Dataframes are constructed with SparkSession instance direclty.
        #
        return DataFrame(jdf,
                         self._spark._wrapped if hasattr(self._spark, '_wrapped') else self._spark)

    def encode(self, df: DataFrame, resource_name: str,
               input_type: Optional[str] = None, column: Optional[str] = None) -> DataFrame:
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

        return self._wrap_df(self._jpc.encode(df._jdf, resource_name,
                                              input_type or MimeType.FHIR_JSON,
                                              column))

    def encode_bundle(self, df: DataFrame, resource_name: str,
                      input_type: Optional[str] = None, column: Optional[str] = None) -> DataFrame:
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
        return self._wrap_df(self._jpc.encodeBundle(df._jdf, resource_name,
                                                    input_type or MimeType.FHIR_JSON,
                                                    column))

    def member_of(self, df: DataFrame, coding_column: Column, value_set_url: str,
                  output_column_name: str):
        """
        Takes a dataframe with a Coding column as input. A new column is created which contains a 
        Boolean value, indicating whether the input Coding is a member of the specified FHIR 
        ValueSet.

        :param df: a DataFrame containing the input data
        :param coding_column: a Column containing a struct representation of a Coding
        :param value_set_url: an identifier for a FHIR ValueSet
        :param output_column_name: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        return self._wrap_df(
                self._jpc.memberOf(df._jdf, coding_column._jc, value_set_url, output_column_name))

    def translate(self, df: DataFrame, coding_column: Column, concept_map_uri: str,
                  reverse: Optional[bool] = False, equivalence: Optional[str] = EQ_EQUIVALENT,
                  output_column_name: Optional[str] = "result"):
        """
        Takes a dataframe with a Coding column as input. A new column is created which contains a 
        Coding value and contains translation targets from the specified FHIR ConceptMap. There 
        may be more than one target concept for each input concept.

        :param df: a DataFrame containing the input data
        :param coding_column: a Column containing a struct representation of a Coding
        :param concept_map_uri: an identifier for a FHIR ConceptMap
        :param reverse: the direction to traverse the map - false results in "source to target" 
        mappings, while true results in "target to source"
        :param equivalence: a comma-delimited set of values from the ConceptMapEquivalence ValueSet
        :param output_column_name: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        return self._wrap_df(
                self._jpc.translate(df._jdf, coding_column._jc, concept_map_uri, reverse,
                                    equivalence,
                                    output_column_name))

    def subsumes(self, df: DataFrame, output_column_name: str,
                 left_coding_column: Optional[Column] = None,
                 right_coding_column: Optional[Column] = None,
                 left_coding: Optional[Coding] = None,
                 right_coding: Optional[Coding] = None):
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
                right_coding_column is None and right_coding is None):
            raise ValueError(
                    "Must provide either left_coding_column or left_coding, and either "
                    "right_coding_column or right_coding")
        left_column = left_coding.to_literal() if left_coding else left_coding_column
        right_column = right_coding.to_literal() if right_coding else right_coding_column
        return self._wrap_df(
                self._jpc.subsumes(df._jdf, left_column._jc, right_column._jc, output_column_name))
