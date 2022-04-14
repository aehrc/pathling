import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pytest import fixture

from pathling.r4 import bundles as bdls
from pathling.etc import find_jar as find_pathling_jar

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))


@fixture(scope="module")
def spark_session(request):
    """
    Fixture for creating a Spark Session available for all tests in this
    testing session.
    """

    gateway_log = logging.getLogger('java_gateway')
    gateway_log.setLevel(logging.ERROR)

    # Get the shaded JAR for testing purposes.
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', find_pathling_jar()) \
        .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
        .config('spark.sql.warehouse.dir', mkdtemp()) \
        .getOrCreate()

    request.addfinalizer(lambda: spark.stop())

    return spark


# Bundles Tests
@fixture(scope="module")
def json_bundles_dir():
    return os.path.join(PROJECT_DIR,
                        'encoders/src/test/resources/data/bundles/R4/json/')

@fixture(scope="module")
def json_resources_dir():
    return os.path.join(PROJECT_DIR,
                        'encoders/src/test/resources/data/resources/R4/json/')

@fixture(scope="module")
def bundles(spark_session, json_bundles_dir):
    return bdls.load_from_directory(spark_session, json_bundles_dir)

def test_load_from_directory(bundles):
    assert bundles.count() == 5

def test_extract_entry(spark_session, bundles):
    assert bdls.extract_entry(spark_session, bundles, 'Patient').count() == 5
    assert bdls.extract_entry(spark_session, bundles, 'Condition').count() == 107

def test_json_bundles_from_df(spark_session, json_bundles_dir):
    json_bundles_df = spark_session.read.text(json_bundles_dir, wholetext=True)
    bundles = bdls.from_json(json_bundles_df, 'value')
    assert bundles.count() == 5
    assert bdls.extract_entry(spark_session, bundles, 'Patient').count() == 5

def test_json_resources_from_df(spark_session, json_resources_dir):
    json_resources_df = spark_session.read.text(json_resources_dir)
    resource_bundles = bdls.from_resource_json(json_resources_df, 'value')
    assert resource_bundles.count() == 1583
    assert bdls.extract_entry(spark_session, resource_bundles, 'Patient').count() == 9
