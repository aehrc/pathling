"""
Support for loading FHIR bundles into Pathling. This includes the following features:

* Allow users to load bundles from a given location
* Convert bundle entries into Spark Dataframes

See the methods below for details.
"""
from pyspark.sql import DataFrame


def _bundles(jvm):
    return jvm.au.csiro.pathling.api.Bundles.forR4()


def load_from_directory(sparkSession, path, minPartitions=1):
    """
    Returns a Java RDD of bundles loaded from the given path. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.

    :param sparkSession: the SparkSession instance
    :param path: path to directory of FHIR bundles to load
    :return: a Java RDD of bundles for use with :func:`extract_entry`
    """

    bundles = _bundles(sparkSession._jvm)
    return bundles.loadFromDirectory(sparkSession._jsparkSession, path, minPartitions)


def from_json(df, column):
    """
    Takes a dataframe with JSON-encoded bundles in the given column and returns
    a Java RDD of Bundle records. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.

    :param df: a DataFrame containing bundles to decode
    :param column: the column in which the bundles to decode are stored
    :return: a Java RDD of bundles for use with :func:`extract_entry`
    """
    bundles = _bundles(df._sc._jvm)
    return bundles.fromJson(df._jdf, column)


def from_resource_json(df, column):
    """
    Takes a dataframe with JSON-encoded resources in the given column and returns
    a Java RDD of Bundle records where every resource is wrapped in a separate bundle.
    Note this RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.

    :param df: a DataFrame containing resources to decode
    :param column: the column in which the resources to decode are stored
    :return: a Java RDD of bundles for use with :func:`extract_entry`
    """
    bundles = _bundles(df._sc._jvm)
    return bundles.fromResourceJson(df._jdf, column)


def from_xml(df, column):
    """
    Takes a dataframe with XML-encoded bundles in the given column and returns
    a Java RDD of Bundle records. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like extract_entry.

    :param df: a DataFrame containing bundles to decode
    :param column: the column in which the bundles to decode are stored
    :return: a Java RDD of bundles for use with :func:`extract_entry`
    """
    bundles = _bundles(df._sc._jvm)
    return bundles.fromXml(df._jdf, column)


def extract_entry(sparkSession, javaRDD, resourceName, maxNestingLevel=None, enableExtensions=None,
                  enabledOpenTypes=None):
    """
    Returns a dataset for the given entry type from the bundles.

    :param sparkSession: the SparkSession instance
    :param javaRDD: the RDD produced by :func:`load_from_directory` or other methods
        in this package
    :param resourceName: the name of the FHIR resource to extract
        (condition, observation, etc)
    :param maxNestingLevel: the maximum nesting level for recursive data types. Zero (0) indicates
        that all direct or indirect fields of type T in element of type T should be skipped
    :param enableExtensions: switches on/off the support for FHIR extensions
    :param enabledOpenTypes: list of types that are encoded within open types, such as extensions
    :return: a DataFrame containing the given resource encoded into Spark columns
    """

    bundles = _bundles(sparkSession._jvm)
    return DataFrame(
        bundles.extractEntry(sparkSession._jsparkSession, javaRDD, resourceName, maxNestingLevel,
                             enableExtensions, enabledOpenTypes), sparkSession._wrapped)
