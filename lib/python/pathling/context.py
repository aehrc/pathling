from typing import Optional, Sequence

# noinspection PyPackageRequirements
from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession, Column

from pathling.etc import find_jar
from pathling.fhir import MimeType

__all__ = ["PathlingContext"]

EQ_EQUIVALENT = "equivalent"


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
    def create(cls, spark: SparkSession = None,
               fhirVersion: Optional[str] = None,
               maxNestingLevel: Optional[int] = None,
               enableExtensions: Optional[bool] = None,
               enabledOpenTypes: Optional[Sequence[str]] = None,
               terminologyServerUrl: Optional[str] = None) -> "PathlingContext":
        """
        Creates a :class:`PathlingContext` using an existing :class:`SparkSession` and configuration
        options.

        :param spark: the :class:`SparkSession` instance.
        :param fhirVersion: the FHIR version to use.
            Must a valid FHIR version string. Defaults to R4.
        :param maxNestingLevel: the maximum nesting level for recursive data types.
            Zero (0) indicates that all direct or indirect fields of type T in element of type T
            should be skipped
        :param enableExtensions: switches on/off the support for FHIR extensions
        :param enabledOpenTypes: list of types that are encoded within open types, such as
            extensions
        :param terminologyServerUrl: the URL of the FHIR terminology server used to resolve
            terminology queries
        :return: a DataFrame containing the given resource encoded into Spark columns
        """
        spark = spark or SparkSession.builder.config('spark.jars', find_jar()).getOrCreate()
        jvm: JavaObject = spark._jvm
        jpc: JavaObject = jvm.au.csiro.pathling.library.PathlingContext.create(
                spark._jsparkSession, fhirVersion, maxNestingLevel, enableExtensions,
                enabledOpenTypes, terminologyServerUrl)
        return PathlingContext(spark, jpc)

    def __init__(self, spark: SparkSession, jpc: JavaObject) -> None:
        self._spark: SparkSession = spark
        self._jpc: JavaObject = jpc

    def _wrapDF(self, jdf: JavaObject) -> DataFrame:
        return DataFrame(jdf, self._spark)

    def encode(self, df: DataFrame, resourceName: str,
               inputType: Optional[str] = None, column: Optional[str] = None) -> DataFrame:
        """
        Takes a dataframe with a string representations of FHIR resources  in the given column and
        encodes the resources of the given types as Spark dataframe.

        :param df: a :class:`DataFrame` containing the resources to encode.
        :param resourceName: the name of the FHIR resource to extract
            (Condition, Observation, etc).
        :param inputType: the mime type of input string encoding.
            Defaults to `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If None then
            the input dataframe is assumed to have one column of type string.
        :return: a :class:`DataFrame` containing the given type of resources encoded into Spark
            columns
        """

        return self._wrapDF(self._jpc.encode(df._jdf, resourceName,
                                             inputType or MimeType.FHIR_JSON,
                                             column))

    def encodeBundle(self, df: DataFrame, resourceName: str,
                     inputType: Optional[str] = None, column: Optional[str] = None) -> DataFrame:
        """
        Takes a dataframe with a string representations of FHIR bundles  in the given column and
        encodes the resources of the given types as Spark dataframe.

        :param df: a :class:`DataFrame` containing the bundles with the resources to encode.
        :param resourceName: the name of the FHIR resource to extract
            (condition, observation, etc).
        :param inputType: the mime type of input string encoding.
            Defaults to `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If None then
            the input dataframe is assumed to have one column of type string.
        :return: a :class:`DataFrame` containing the given type of resources encoded into Spark
            columns
        """
        return self._wrapDF(self._jpc.encodeBundle(df._jdf, resourceName,
                                                   inputType or MimeType.FHIR_JSON,
                                                   column))

    def memberOf(self, df: DataFrame, codingColumn: Column, valueSetUrl: str,
                 outputColumnName: str):
        """
        Takes a dataframe with a Coding column as input. A new column is created which contains a 
        Boolean value, indicating whether the input Coding is a member of the specified FHIR 
        ValueSet.

        :param df: a DataFrame containing the input data
        :param codingColumn: a Column containing a struct representation of a Coding
        :param valueSetUrl: an identifier for a FHIR ValueSet
        :param outputColumnName: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        return self._wrapDF(
                self._jpc.memberOf(df._jdf, codingColumn._jc, valueSetUrl, outputColumnName))

    def translate(self, df: DataFrame, codingColumn: Column, conceptMapUri: str,
                  reverse: Optional[bool] = False, equivalence: Optional[str] = EQ_EQUIVALENT,
                  outputColumnName: Optional[str] = "result"):
        """
        Takes a dataframe with a Coding column as input. A new column is created which contains a 
        Coding value and contains translation targets from the specified FHIR ConceptMap. There 
        may be more than one target concept for each input concept.

        :param df: a DataFrame containing the input data
        :param codingColumn: a Column containing a struct representation of a Coding
        :param conceptMapUri: an identifier for a FHIR ConceptMap
        :param reverse: the direction to traverse the map - false results in "source to target" 
        mappings, while true results in "target to source"
        :param equivalence: a comma-delimited set of values from the ConceptMapEquivalence ValueSet
        :param outputColumnName: the name of the result column
        :return: A new dataframe with an additional column containing the result of the operation.
        """
        return self._wrapDF(
                self._jpc.translate(df._jdf, codingColumn._jc, conceptMapUri, reverse, equivalence,
                                    outputColumnName))
