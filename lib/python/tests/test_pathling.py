import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pytest import fixture

from pathling import PathlingContext
from pathling.etc import find_jar as find_pathling_jar
from pathling.fhir import MimeType

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
def xml_bundles_dir():
    return os.path.join(PROJECT_DIR,
                        'encoders/src/test/resources/data/bundles/R4/xml/')


@fixture(scope="module")
def def_pathling(spark_session):
    return PathlingContext.of(spark_session)


@fixture(scope="module")
def json_resources_df(spark_session, json_resources_dir):
    return spark_session.read.text(json_resources_dir)


@fixture(scope="module")
def json_bundles_df(spark_session, json_bundles_dir):
    return spark_session.read.text(json_bundles_dir, wholetext=True)


@fixture(scope="module")
def xml_bundles_df(spark_session, xml_bundles_dir):
    return spark_session.read.text(xml_bundles_dir, wholetext=True)


def test_encode_json_bundles(def_pathling, json_bundles_df):
    assert def_pathling.encodeBundle(json_bundles_df, 'Patient').count() == 5
    assert def_pathling.encodeBundle(json_bundles_df, 'Condition', column="value").count() == 107


def test_encode_json_resouces(def_pathling, json_resources_df):
    assert def_pathling.encode(json_resources_df, 'Patient').count() == 9
    assert def_pathling.encode(json_resources_df, 'Condition', inputType=MimeType.FHIR_JSON).count() == 71


def test_encode_xml_bundles(def_pathling, xml_bundles_df):
    assert def_pathling.encodeBundle(xml_bundles_df, 'Patient', MimeType.FHIR_XML).count() == 5
    assert def_pathling.encodeBundle(xml_bundles_df, 'Condition', MimeType.FHIR_XML, "value").count() == 107


def test_element_nesting(spark_session, json_resources_df):
    ptl_def = PathlingContext.of(spark_session)
    ptl_0 = PathlingContext.of(spark_session, maxNestingLevel=0)
    ptl_1 = PathlingContext.of(spark_session, maxNestingLevel=1)

    # default nesting level is 0
    quest_def = ptl_def.encode(json_resources_df, 'Questionnaire').head()
    assert ('item' in quest_def) and (not 'item' in quest_def['item'][0])

    # max nesting level set to 0
    quest_0 = ptl_0.encode(json_resources_df, 'Questionnaire').head()
    assert ('item' in quest_0) and (not 'item' in quest_0['item'][0])

    # max nesting level set to 1
    quest_1 = ptl_1.encode(json_resources_df, 'Questionnaire').head()
    assert ('item' in quest_1) and ('item' in quest_1['item'][0]) and (
        not 'item' in quest_1['item'][0]['item'][0])


def test_extension_support(spark_session, json_resources_df):
    # by default extension are off

    ptl_def = PathlingContext.of(spark_session)
    ptl_ext_off = PathlingContext.of(spark_session, enableExtensions=False)
    ptl_ext_on = PathlingContext.of(spark_session, enableExtensions=True)

    patient_def = ptl_def.encode(json_resources_df, 'Patient').head()
    assert not '_extension' in patient_def

    # extensions disabled
    patient_off = ptl_ext_off.encode(json_resources_df, 'Patient').head()
    assert not '_extension' in patient_off

    # extensions enabled
    patient_on = ptl_ext_on.encode(json_resources_df, 'Patient').head()
    assert '_extension' in patient_on


def _get_extension_value_keys(row):
    """
    Extracts the extension value[x] fields from a row
    """
    return [valueKey for valueKey in list(row['_extension'].values())[0][0].asDict().keys() if
            valueKey.startswith('value')]


def test_open_types(spark_session, json_resources_df):
    ptl_def = PathlingContext.of(spark_session, enableExtensions=True)
    ptl_none = PathlingContext.of(spark_session, enableExtensions=True, enabledOpenTypes=[])
    ptl_some = PathlingContext.of(spark_session, enableExtensions=True,
                                  enabledOpenTypes=['boolean', 'integer', 'string',
                                                    'Address'])

    # by default no open types
    patient_def = ptl_def.encode(json_resources_df, 'Patient').head()
    assert '_extension' in patient_def
    assert not _get_extension_value_keys(patient_def)

    # explicit empty open types
    patient_none = ptl_none.encode(json_resources_df, 'Patient').head()
    assert '_extension' in patient_none
    assert not _get_extension_value_keys(patient_none)

    # some open types present
    patient_some = ptl_some.encode(json_resources_df, 'Patient').head()
    assert ['valueAddress', 'valueBoolean', 'valueInteger',
            'valueString'] == _get_extension_value_keys(patient_some)
