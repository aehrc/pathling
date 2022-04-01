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
    return jvm.Bundles.forStu3()

def load_from_directory(sparkSession, path, minPartitions=1):
    """
    Returns a Java RDD of bundles loaded from the given path. Note this
    RDD contains Bundle records that aren't serializable in Python,
    so users should use this class as merely a parameter to other methods
    in this module, like :func:`extract_entry`.

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

def extract_entry(sparkSession, javaRDD, resourceTypeUrl):
    """
    Returns a dataset for the given entry type from the bundles.

    :param sparkSession: the SparkSession instance
    :param javaRDD: the RDD produced by :func:`load_from_directory` or other methods
        in this package
    :param resourceTypeUrl: the type of the FHIR resource to extract
        (Condition, Observation, etc, for the base profile, or the URL of the structure definition)
    :return: a DataFrame containing the given resource encoded into Spark columns
    """

    bundles = _bundles(sparkSession._jvm)
    return DataFrame(
            bundles.extractEntry(sparkSession._jsparkSession, javaRDD, resourceTypeUrl),
            sparkSession._wrapped)

def write_to_database(sparkSession, javaRDD, databaseName, resourceTypeUrls):
    """
    Writes the bundles in the give RDD and saves them to a database, where
    each table in the database has the same name of the resource it represents.

    :param sparkSession: the SparkSession instance
    :param javaRDD: the RDD produced by :func:`load_from_directory` or other methods
        in this package
    :param databaseName: name of the database to write the resources to
    :param resourceTypeUrls: the types of the FHIR resource to extract
        (Condition, Observation, etc, for the base profile, or the URL of the structure definition)
    """

    gateway = sparkSession.sparkContext._gateway
    namesArray = gateway.new_array(gateway.jvm.String, len(resourceTypeUrls))
    for idx, name in enumerate(resourceTypeUrls):
        namesArray[idx] = name

    bundles = _bundles(sparkSession._jvm)

    bundles.saveAsDatabase(sparkSession._jsparkSession, javaRDD, databaseName, namesArray)

def to_bundle(sparkSession, dataset, resourceTypeUrl):
    """
    Converts a dataset of FHIR resources to a bundle containing those resources.
    Use with caution against large datasets.

    :param sparkSession: the SparkSession instance
    :param dataset: a DataFrame of encoded FHIR Resources
    :param resourceTypeUrl: the type of the FHIR resource to extract
        (Condition, Observation, etc, for the base profile, or the URL of the structure definition)
    :return: a JSON bundle of the dataset contents
    """

    jvm = sparkSession._jvm

    json_string = jvm.com.cerner.bunsen.stu3.python.Functions.toJsonBundle(dataset._jdf,
                                                                           resourceTypeUrl)

    return json.loads(json_string)
