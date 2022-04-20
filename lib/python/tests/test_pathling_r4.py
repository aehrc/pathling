import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pytest import fixture

from pathling.etc import find_jar as find_pathling_jar
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
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', find_pathling_jar()) \
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


@fixture(scope="module")
def resource_bundles(spark_session, json_resources_dir):
    json_resources_df = spark_session.read.text(json_resources_dir)
    return bdls.from_resource_json(json_resources_df, 'value')


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


def test_json_resources_from_df(spark_session, resource_bundles):
    assert resource_bundles.count() == 1583
    assert bdls.extract_entry(spark_session, resource_bundles, 'Patient').count() == 9


def test_resource_nesting(spark_session, resource_bundles):
    # default nesting level is 0
    quest_def = bdls.extract_entry(spark_session, resource_bundles, 'Questionnaire',
                                   maxNestingLevel=0).head()
    assert ('item' in quest_def) and (not 'item' in quest_def['item'][0])

    # max nesting level set to 0
    quest_0 = bdls.extract_entry(spark_session, resource_bundles, 'Questionnaire',
                                 maxNestingLevel=0).head()
    assert ('item' in quest_0) and (not 'item' in quest_0['item'][0])

    # max nesting level set to 1
    quest_0 = bdls.extract_entry(spark_session, resource_bundles, 'Questionnaire',
                                 maxNestingLevel=1).head()
    assert ('item' in quest_0) and ('item' in quest_0['item'][0]) and (
        not 'item' in quest_0['item'][0]['item'][0])


def test_extension_support(spark_session, resource_bundles):
    # by default extension are off
    patient_def = bdls.extract_entry(spark_session, resource_bundles, 'Patient').head()
    assert not '_extension' in patient_def

    # extensions disabled
    patient_off = bdls.extract_entry(spark_session, resource_bundles, 'Patient',
                                     enableExtensions=False).head()
    assert not '_extension' in patient_off

    # extensions enabled
    patient_on = bdls.extract_entry(spark_session, resource_bundles, 'Patient',
                                    enableExtensions=True).head()
    assert '_extension' in patient_on


def _get_extension_value_keys(row):
    """
    Extracts the extension value[x] fields from a row
    """
    return [valueKey for valueKey in list(row['_extension'].values())[0][0].asDict().keys() if
            valueKey.startswith('value')]


def test_open_types(spark_session, resource_bundles):
    # by default no open types
    patient_def = bdls.extract_entry(spark_session, resource_bundles, 'Patient',
                                     enableExtensions=True).head()
    assert '_extension' in patient_def
    assert not _get_extension_value_keys(patient_def)

    # explicit empty open types
    patient_none = bdls.extract_entry(spark_session, resource_bundles, 'Patient',
                                      enableExtensions=True, enabledOpenTypes=[]).head()
    assert '_extension' in patient_none
    assert not _get_extension_value_keys(patient_none)

    # some open types present
    patient_some = bdls.extract_entry(spark_session, resource_bundles, 'Patient',
                                      enableExtensions=True,
                                      enabledOpenTypes=['boolean', 'integer', 'string',
                                                        'Address']).head()
    assert ['valueAddress', 'valueBoolean', 'valueInteger',
            'valueString'] == _get_extension_value_keys(patient_some)
