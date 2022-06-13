from typing import Optional, Sequence

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession

from pathling.fhir import MimeType

__all__ = ["PathlingContext"]


class PathlingContext:
    """
    Main entry point for Pathling API functionality.
    Should be instantiated with the :func:`PathlingContext.create` class method.

    Example use::

        ptl = PathlingContext.create(spark)
        patient_df = ptl.encode(spark.read.text('ndjson_resources'), 'Patient')
        patient_df.show()
    """

    @classmethod
    def create(cls, sparkSession: SparkSession,
               fhirVersion: Optional[str] = None,
               maxNestingLevel: Optional[int] = None,
               enableExtensions: Optional[bool] = None,
               enabledOpenTypes: Optional[Sequence[str]] = None) -> "PathlingContext":
        """
        Creates a :class:`PathlingContext` for given :class:`SparkSession` and configuration
        options.

        :param sparkSession: the :class:`SparkSession` instance.
        :param fhirVersion: the FHIR version to use.
            Must a valid FHIR version string. Defaults to R4.
        :param maxNestingLevel: the maximum nesting level for recursive data types.
            Zero (0) indicates that all direct or indirect fields of type T in element of type T
            should be skipped
        :param enableExtensions: switches on/off the support for FHIR extensions
        :param enabledOpenTypes: list of types that are encoded within open types, such as
            extensions
        :return: a DataFrame containing the given resource encoded into Spark columns
        """
        jvm: JavaObject = sparkSession._jvm
        jpc: JavaObject = jvm.au.csiro.pathling.api.PathlingContext.create(
                sparkSession._jsparkSession, fhirVersion, maxNestingLevel, enableExtensions,
                enabledOpenTypes)
        return PathlingContext(sparkSession, jpc)

    def __init__(self, sparkSession: SparkSession, jpc: JavaObject) -> None:
        self._sparkSession: SparkSession = sparkSession
        self._jpc: JavaObject = jpc

    def _wrapDF(self, jdf: JavaObject) -> DataFrame:
        return DataFrame(jdf, self._sparkSession._wrapped)

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
