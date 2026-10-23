#
# Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pytest import fixture

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


# Documents of several types, each one per line, as encode reads them. The Bundle is another type
# and is left out, as the Observation is, when Patients are requested.
MIXED_JSON = [
    '{"resourceType":"Patient","id":"p1","gender":"male"}',
    '{"resourceType":"Observation","id":"o1","status":"final","code":{"text":"x"}}',
    '{"resourceType":"Patient","id":"p2","gender":"female"}',
    '{"resourceType":"Bundle","id":"b1","type":"collection","entry":[{"resource":'
    '{"resourceType":"Patient","id":"p3"}}]}',
]

MIXED_XML = [
    '<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><gender value="male"/></Patient>',
    '<Observation xmlns="http://hl7.org/fhir"><id value="o1"/><status value="final"/>'
    '<code><text value="x"/></code></Observation>',
    '<Patient xmlns="http://hl7.org/fhir"><id value="p2"/><gender value="female"/></Patient>',
    '<Bundle xmlns="http://hl7.org/fhir"><id value="b1"/><type value="collection"/>'
    '<entry><resource><Patient><id value="p3"/></Patient></resource></entry></Bundle>',
]


def _documents(spark_session, documents):
    return spark_session.createDataFrame([(d,) for d in documents], ["value"])


def test_encode_keeps_only_the_requested_type_from_json(spark_session, def_pathling):
    # Only the documents of the requested type are stored, as before the switch (T070, SC-008).
    documents = _documents(spark_session, MIXED_JSON)
    patients = def_pathling.encode(documents, "Patient", MimeType.FHIR_JSON)
    assert sorted(row["id"] for row in patients.select("id").collect()) == ["p1", "p2"]
    observations = def_pathling.encode(documents, "Observation")
    assert [row["id"] for row in observations.select("id").collect()] == ["o1"]
    # A type that is absent from the input gives an empty result, not a failure.
    assert def_pathling.encode(documents, "Condition").count() == 0


def test_annotate_keeps_an_annotation_pathling_does_not_define(
    spark_session, def_pathling
):
    # No kind of annotation is defined yet, so annotating stored resources changes nothing, and a
    # field marked as an annotation is kept, as the default options say.
    documents = _documents(spark_session, MIXED_JSON)
    patients = def_pathling.encode(documents, "Patient").withColumn(
        "__unknown", lit("u")
    )
    annotated = def_pathling.annotate(patients, "Patient")
    assert annotated.schema == patients.schema


def test_encode_keeps_only_the_requested_type_from_xml(spark_session, def_pathling):
    documents = _documents(spark_session, MIXED_XML)
    patients = def_pathling.encode(documents, "Patient", MimeType.FHIR_XML)
    assert sorted(row["id"] for row in patients.select("id").collect()) == ["p1", "p2"]
    observations = def_pathling.encode(documents, "Observation", MimeType.FHIR_XML)
    assert [row["id"] for row in observations.select("id").collect()] == ["o1"]
    assert def_pathling.encode(documents, "Condition", MimeType.FHIR_XML).count() == 0


# The nesting, extension and open-type options bound the schema of the dense mode only, which does
# not exist yet, so they bound nothing in the fitted schema that encoding stores: depth comes from
# the data, extensions are stored inline because they are present, and open types resolve as
# observed (contracts/library-api.md, decision 69). The options are still accepted.


def test_element_nesting(spark_session, json_resources_df):
    pathling_def = PathlingContext.create(spark_session)
    pathling_0 = PathlingContext.create(spark_session, max_nesting_level=0)
    pathling_1 = PathlingContext.create(spark_session, max_nesting_level=1)

    quest_def_df = pathling_def.encode(json_resources_df, "Questionnaire")
    quest_def = quest_def_df.head()
    # The levels of items the data carries are kept, whatever the nesting level.
    assert (
        ("item" in quest_def)
        and ("item" in quest_def["item"][0])
        and ("item" in quest_def["item"][0]["item"][0])
        and ("item" in quest_def["item"][0]["item"][0]["item"][0])
    )

    # The options bound nothing: the schema is the one the defaults give.
    for pathling in (pathling_0, pathling_1):
        quest = pathling.encode(json_resources_df, "Questionnaire")
        assert quest.schema == quest_def_df.schema
        assert quest.head() == quest_def


def test_extension_support(spark_session, json_resources_df):
    pathling_def = PathlingContext.create(spark_session)
    pathling_ext_off = PathlingContext.create(spark_session, enable_extensions=False)
    pathling_ext_on = PathlingContext.create(spark_session, enable_extensions=True)

    patients_def = pathling_def.encode(json_resources_df, "Patient")
    patient_def = patients_def.head()
    # The extensions are stored inline, and there is no extension map.
    assert "_extension" not in patient_def
    assert "extension" in patient_def

    for pathling in (pathling_ext_off, pathling_ext_on):
        patients = pathling.encode(json_resources_df, "Patient")
        assert patients.schema == patients_def.schema
        assert "_extension" not in patients.head()


def _get_extension_value_keys(row):
    """
    Extracts the extension value[x] fields from a row
    """
    return sorted(
        valueKey
        for valueKey in row["extension"][0].asDict().keys()
        if valueKey.startswith("value")
    )


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

    # The extensions hold every value type the data carries, whether or not it is among the open
    # types, and a decimal has no scale companion.
    patient_def = pathling_def.encode(json_resources_df, "Patient").head()
    value_keys = _get_extension_value_keys(patient_def)
    assert {"valueAddress", "valueDecimal", "valueString"} <= set(value_keys)
    assert "valueInteger" not in value_keys
    assert "valueDecimal_scale" not in value_keys

    for pathling in (pathling_none, pathling_some):
        patient = pathling.encode(json_resources_df, "Patient").head()
        assert value_keys == _get_extension_value_keys(patient)
