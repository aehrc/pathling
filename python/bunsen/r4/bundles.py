"""
Support for loading FHIR bundles into Bunsen. This includes the following features:

* Allow users to load bundles from a given location
* Convert bundle entries into Spark Dataframes
* Save all entities with a bundle collection to a distinct table for each (e.g., an observation table, a condition table, and so on.)
* Converts the results of a Bunsen query back into bundles that can then be used elsewhere.

See the methods below for details.
"""
from pyspark.sql import DataFrame
import json

def _bundles(jvm):
    return jvm.com.cerner.bunsen.Bundles.forR4()

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

def extract_entry(sparkSession, javaRDD, resourceName):
    """
    Returns a dataset for the given entry type from the bundles.

    :param sparkSession: the SparkSession instance
    :param javaRDD: the RDD produced by :func:`load_from_directory` or other methods
        in this package
    :param resourceName: the name of the FHIR resource to extract
        (condition, observation, etc)
    :return: a DataFrame containing the given resource encoded into Spark columns
    """

    bundles = _bundles(sparkSession._jvm)
    return DataFrame(
            bundles.extractEntry(sparkSession._jsparkSession, javaRDD, resourceName),
            sparkSession._wrapped)

def extract_entry(sparkSession, javaRDD, resourceName):
    """
    Returns a dataset for the given entry type from the bundles.

    :param sparkSession: the SparkSession instance
    :param javaRDD: the RDD produced by :func:`load_from_directory` or other methods
        in this package
    :param resourceName: the name of the FHIR resource to extract
        (condition, observation, etc)
    :return: a DataFrame containing the given resource encoded into Spark columns
    """

    bundles = _bundles(sparkSession._jvm)
    return DataFrame(
        bundles.extractEntry(sparkSession._jsparkSession, javaRDD, resourceName),
        sparkSession._wrapped)

def write_to_database(sparkSession, javaRDD, databaseName, resourceNames):
    """
    Writes the bundles in the give RDD and saves them to a database, where
    each table in the database has the same name of the resource it represents.

    :param sparkSession: the SparkSession instance
    :param javaRDD: the RDD produced by :func:`load_from_directory` or other methods
        in this package
    :param databaseName: name of the database to write the resources to
    :param resourceNames: the names of the FHIR resource to extract
        (condition, observation, etc)
    """

    gateway = sparkSession.sparkContext._gateway
    namesArray = gateway.new_array(gateway.jvm.String, len(resourceNames))
    for idx, name in enumerate(resourceNames):
        namesArray[idx] = name

    bundles = _bundles(sparkSession._jvm)

    bundles.saveAsDatabase(sparkSession._jsparkSession, javaRDD, databaseName, namesArray)

def save_as_database(sparkSession, path, databaseName, *resourceNames, **kwargs):
    """
    DEPRECATED. Users can easily do this by combining the load_from_directory and
    write_to_database functions.

    Loads the bundles in the path and saves them to a database, where
    each table in the database has the same name of the resource it represents.

    :param sparkSession: the SparkSession instance
    :param path: path to directory of FHIR bundles to load
    :param databaseName: name of the database to write the resources to
    :param resourceNames: the names of the FHIR resource to extract
        (condition, observation, etc)
    """
    rdd = load_from_directory(sparkSession,path, kwargs.get('minPartitions', 1))

    if (kwargs.get('cache', True)):
        rdd.cache()

    write_to_database(sparkSession, rdd, databaseName, resourceNames)

    if (kwargs.get('cache', True)):
        rdd.unpersist()

def to_bundle(sparkSession, dataset):
    """
    Converts a dataset of FHIR resources to a bundle containing those resources.
    Use with caution against large datasets.

    :param sparkSession: the SparkSession instance
    :param dataset: a DataFrame of encoded FHIR Resources
    :return: a JSON bundle of the dataset contents
    """

    jvm = sparkSession._jvm

    json_string = jvm.com.cerner.bunsen.r4.python.Functions.toJsonBundle(dataset._jdf)

    return json.loads(json_string)
