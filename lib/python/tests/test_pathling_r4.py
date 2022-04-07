import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pytest import fixture

from pathling.r4 import bundles as bdls

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
    # TODO: Decide what is the best way to handle this
    # shaded_jar =  os.environ['SHADED_JAR_PATH']
    shaded_jar = os.path.join(PROJECT_DIR, 'encoders/target/encoders-5.0.0-all.jar')

    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', shaded_jar) \
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
def bundles(spark_session, json_bundles_dir):
    return bdls.load_from_directory(spark_session, json_bundles_dir)


def test_load_from_directory(bundles):
    assert bundles.count() == 5


# def test_json_bundles_from_df(spark_session, json_bundles_dir):
#    spark_session.read.text
#    json_bundles = spark_session.sparkContext.wholeTextFiles('tests/resources/bundles/json').toDF()
#
#    bundles = from_json(json_bundles, '_2')#
#
#    assert extract_entry(spark_session, bundles, 'Condition').count() == 5


def test_extract_entry(spark_session, bundles):
    assert bdls.extract_entry(spark_session, bundles, 'Patient').count() == 5
    assert bdls.extract_entry(spark_session, bundles, 'Condition').count() == 107
