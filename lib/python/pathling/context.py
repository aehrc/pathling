from pyspark.sql import DataFrame

from pathling.fhir import MimeType


class PathlingContext:
    """
    """

    @classmethod
    def of(cls, sparkSession, fhirVersion=None, maxNestingLevel=None, enableExtensions=None,
           enabledOpenTypes=None):
        """
        Creates a PathlingContext for given SparkSession and configuration options.

        :param sparkSession: the SparkSession instance
        :param fhirVersion: the FHIR version to use. Must a valid FHIR version string. Defaults to R4.
        :param maxNestingLevel: the maximum nesting level for recursive data types. Zero (0) indicates
            that all direct or indirect fields of type T in element of type T should be skipped
        :param enableExtensions: switches on/off the support for FHIR extensions
        :param enabledOpenTypes: list of types that are encoded within open types, such as extensions
        :return: a DataFrame containing the given resource encoded into Spark columns
        """
        jvm = sparkSession._jvm
        jpc = jvm.au.csiro.pathling.api.PathlingContext.of(sparkSession._jsparkSession,
                                                           fhirVersion, maxNestingLevel, enableExtensions,
                                                           enabledOpenTypes)
        return PathlingContext(sparkSession, jpc)

    def __init__(self, sparkSession, jpc):
        self._sparkSession = sparkSession
        self._jpc = jpc

    def _wrapDF(self, jdf):
        return DataFrame(jdf, self._sparkSession._wrapped)

    def encode(self, df, resourceName, inputType=None, column=None):
        """
        Takes a dataframe with a string representations of FHIR resources  in the given column and encodes
        the resources of the given types as Spark dataframe.

        :param df: a DataFrame containing the resources to encode.
        :param resourceName: the name of the FHIR resource to extract
            (condition, observation, etc).
        :param inputType: the mime type of input string encoding.
            Defaults to `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If None then
            the input dataframe is assumed to have one column of type string.
        :return: a DataFrame containing the given type of resources encoded into Spark columns
        """

        return self._wrapDF(self._jpc.pyEncode(df._jdf, resourceName,
                                               inputType or MimeType.FHIR_JSON,
                                               column))

    def encodeBundle(self, df, resourceName, inputType=None, column=None):
        """
        Takes a dataframe with a string representations of FHIR bundles  in the given column and encodes
        the resources of the given types as Spark dataframe.

        :param df: a DataFrame containing the bundles with the resources to encode.
        :param resourceName: the name of the FHIR resource to extract
            (condition, observation, etc).
        :param inputType: the mime type of input string encoding.
            Defaults to `application/fhir+json`.
        :param column: the column in which the resources to encode are stored. If None then
            the input dataframe is assumed to have one column of type string.
        :return: a DataFrame containing the given type of resources encoded into Spark columns
        """
        return self._wrapDF(self._jpc.pyEncodeBundle(df._jdf, resourceName,
                                                     inputType or MimeType.FHIR_JSON,
                                                     column))
