#  Copyright 2023 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
from pyspark.sql import SparkSession
from pytest import fixture
from tempfile import mkdtemp

from pathling import PathlingContext
from pathling._version import __java_version__
from pathling.fhir import MimeType

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)


@fixture(scope="module")
def spark_session(request):
    """
    Fixture for creating a Spark Session available for all tests in this
    testing session.
    """

    gateway_log = logging.getLogger("java_gateway")
    gateway_log.setLevel(logging.ERROR)

    # Get the shaded JAR for testing purposes.
    spark = (
        SparkSession.builder.appName("pathling-test")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            f"au.csiro.pathling:library-runtime:{__java_version__}",
        )
        .config("spark.sql.warehouse.dir", mkdtemp())
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    request.addfinalizer(lambda: spark.stop())

    return spark


# Bundles Tests
@fixture(scope="module")
def json_bundles_dir():
    return os.path.join(
        PROJECT_DIR, "encoders/src/test/resources/data/bundles/R4/json/"
    )


@fixture(scope="module")
def json_resources_dir():
    return os.path.join(
        PROJECT_DIR, "encoders/src/test/resources/data/resources/R4/json/"
    )


@fixture(scope="module")
def xml_bundles_dir():
    return os.path.join(PROJECT_DIR, "encoders/src/test/resources/data/bundles/R4/xml/")


@fixture(scope="module")
def def_pathling(spark_session):
    return PathlingContext.create(spark_session)


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
    assert def_pathling.encode_bundle(json_bundles_df, "Patient").count() == 5
    assert (
        def_pathling.encode_bundle(json_bundles_df, "Condition", column="value").count()
        == 107
    )


def test_encode_json_resources(def_pathling, json_resources_df):
    assert def_pathling.encode(json_resources_df, "Patient").count() == 9
    assert (
        def_pathling.encode(
            json_resources_df, "Condition", input_type=MimeType.FHIR_JSON
        ).count()
        == 71
    )


def test_encode_xml_bundles(def_pathling, xml_bundles_df):
    assert (
        def_pathling.encode_bundle(xml_bundles_df, "Patient", MimeType.FHIR_XML).count()
        == 5
    )
    assert (
        def_pathling.encode_bundle(
            xml_bundles_df, "Condition", MimeType.FHIR_XML, "value"
        ).count()
        == 107
    )


def test_element_nesting(spark_session, json_resources_df):
    pathling_def = PathlingContext.create(spark_session)
    pathling_0 = PathlingContext.create(spark_session, max_nesting_level=0)
    pathling_1 = PathlingContext.create(spark_session, max_nesting_level=1)

    # default nesting level is 3
    quest_def = pathling_def.encode(json_resources_df, "Questionnaire").head()
    assert (
        ("item" in quest_def)
        and ("item" in quest_def["item"][0])
        and ("item" in quest_def["item"][0]["item"][0])
        and ("item" in quest_def["item"][0]["item"][0]["item"][0])
        and ("item" not in quest_def["item"][0]["item"][0]["item"][0]["item"][0])
    )

    # max nesting level set to 0
    quest_0 = pathling_0.encode(json_resources_df, "Questionnaire").head()
    assert ("item" in quest_0) and ("item" not in quest_0["item"][0])

    # max nesting level set to 1
    quest_1 = pathling_1.encode(json_resources_df, "Questionnaire").head()
    assert (
        ("item" in quest_1)
        and ("item" in quest_1["item"][0])
        and ("item" not in quest_1["item"][0]["item"][0])
    )


def test_extension_support(spark_session, json_resources_df):
    # by default extension are off

    pathling_def = PathlingContext.create(spark_session)
    pathling_ext_off = PathlingContext.create(spark_session, enable_extensions=False)
    pathling_ext_on = PathlingContext.create(spark_session, enable_extensions=True)

    patient_def = pathling_def.encode(json_resources_df, "Patient").head()
    assert "_extension" not in patient_def

    # extensions disabled
    patient_off = pathling_ext_off.encode(json_resources_df, "Patient").head()
    assert "_extension" not in patient_off

    # extensions enabled
    patient_on = pathling_ext_on.encode(json_resources_df, "Patient").head()
    assert "_extension" in patient_on


def _get_extension_value_keys(row):
    """
    Extracts the extension value[x] fields from a row
    """
    return [
        valueKey
        for valueKey in list(row["_extension"].values())[0][0].asDict().keys()
        if valueKey.startswith("value")
    ]


def test_open_types(spark_session, json_resources_df):
    pathling_def = PathlingContext.create(spark_session, enable_extensions=True)
    pathling_none = PathlingContext.create(
        spark_session, enable_extensions=True, enabled_open_types=[]
    )
    pathling_some = PathlingContext.create(
        spark_session,
        enable_extensions=True,
        enabled_open_types=["boolean", "integer", "string", "Address"],
    )

    # Test defaults.
    patient_def = pathling_def.encode(json_resources_df, "Patient").head()
    assert "_extension" in patient_def
    assert [
        "valueAddress",
        "valueBoolean",
        "valueCode",
        "valueCodeableConcept",
        "valueCoding",
        "valueDateTime",
        "valueDate",
        "valueDecimal",
        "valueDecimal_scale",
        "valueIdentifier",
        "valueInteger",
        "valueReference",
        "valueString",
    ] == _get_extension_value_keys(patient_def)

    # Explicit empty open types.
    patient_none = pathling_none.encode(json_resources_df, "Patient").head()
    assert "_extension" in patient_none
    assert not _get_extension_value_keys(patient_none)

    # Some open types present.
    patient_some = pathling_some.encode(json_resources_df, "Patient").head()
    assert [
        "valueAddress",
        "valueBoolean",
        "valueInteger",
        "valueString",
    ] == _get_extension_value_keys(patient_some)
