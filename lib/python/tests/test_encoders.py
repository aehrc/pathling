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
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pytest import fixture, mark, raises

from pathling import PathlingContext
from pathling._version import __java_version__
from pathling.fhir import MimeType
from tests.schema_equality import same_schema

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
    # By default annotate only fills annotations that are missing, so those encode already added
    # are kept as they are, and so is a field marked as an annotation that Pathling does not define.
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


# XML is read and written without HAPI on the runtime jar (ticket 32): through Spark's XML reader
# and writer, with a narrative held to L2 and the references between a bundle's entries resolved.
NARRATIVE_XML = (
    '<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><text><status value="generated"/>'
    '<div xmlns="http://www.w3.org/1999/xhtml"><p>Line one&#xa;line two &amp; more</p></div>'
    '</text><gender value="male"/></Patient>'
)

TRANSACTION_XML = (
    '<Bundle xmlns="http://hl7.org/fhir"><type value="transaction"/>'
    '<entry><fullUrl value="urn:uuid:p1"/><resource><Patient><id value="p1"/></Patient>'
    "</resource></entry>"
    '<entry><fullUrl value="urn:uuid:o1"/><resource><Observation><id value="o1"/>'
    '<status value="final"/><code><text value="x"/></code>'
    '<subject><reference value="urn:uuid:p1"/></subject></Observation></resource></entry>'
    "</Bundle>"
)


def test_reads_and_writes_xml_without_hapi(spark_session, def_pathling):
    encoded = def_pathling.encode(
        _documents(spark_session, [NARRATIVE_XML]), "Patient", MimeType.FHIR_XML
    )
    decoded = DataFrame(
        def_pathling._jpc.decode(encoded._jdf, "Patient", MimeType.FHIR_XML),
        spark_session,
    ).collect()
    # The line feed in the narrative is kept, and so the document is written as it was read.
    assert [row[0] for row in decoded] == [NARRATIVE_XML.replace("&#xa;", "\n")]


def test_resolves_references_in_an_xml_bundle(spark_session, def_pathling):
    observations = def_pathling.encode_bundle(
        _documents(spark_session, [TRANSACTION_XML]), "Observation", MimeType.FHIR_XML
    )
    assert [row["subject"]["reference"] for row in observations.collect()] == [
        "Patient/p1"
    ]


# Encoding with a schema defined up front (T031, FR-002, FR-012, FR-013, US2). The schema has
# gender, birthDate and name.family. The first Patient carries active and name.given, which the
# schema lacks, and lacks birthDate. The second lacks gender and name.
SCHEMA_DONOR_JSON = (
    '{"resourceType":"Patient","id":"d","gender":"male","birthDate":"2000-01-01",'
    '"name":[{"family":"Smith"}]}'
)

PATIENT_JSON = [
    '{"resourceType":"Patient","id":"p1","active":true,"gender":"female",'
    '"name":[{"family":"Ash","given":["Ann"]}]}',
    '{"resourceType":"Patient","id":"p2","birthDate":"1990-01-01"}',
]

PATIENT_XML = [
    '<Patient xmlns="http://hl7.org/fhir"><id value="p1"/><active value="true"/>'
    '<name><family value="Ash"/><given value="Ann"/></name>'
    '<gender value="female"/></Patient>',
    '<Patient xmlns="http://hl7.org/fhir"><id value="p2"/>'
    '<birthDate value="1990-01-01"/></Patient>',
]

BUNDLE_JSON = (
    '{"resourceType":"Bundle","type":"collection","entry":['
    + ",".join('{"resource":' + p + "}" for p in PATIENT_JSON)
    + "]}"
)

BUNDLE_XML = (
    '<Bundle xmlns="http://hl7.org/fhir"><type value="collection"/>'
    + "".join("<entry><resource>" + p + "</resource></entry>" for p in PATIENT_XML)
    + "</Bundle>"
)

# Each case names the method, the MIME type and the text it reads, one document or bundle per line.
SCHEMA_CASES = {
    "encode JSON": ("encode", MimeType.FHIR_JSON, PATIENT_JSON),
    "encode XML": ("encode", MimeType.FHIR_XML, PATIENT_XML),
    "encode_bundle JSON": ("encode_bundle", MimeType.FHIR_JSON, [BUNDLE_JSON]),
    "encode_bundle XML": ("encode_bundle", MimeType.FHIR_XML, [BUNDLE_XML]),
}

REFUSAL = (
    "Encoding a streaming dataset needs a schema: resources are otherwise stored with a schema"
    " inferred from the data, which a stream cannot provide. Pass a schema, for example one"
    " taken from an existing table's schemas(), or encode each batch within foreachBatch"
    " instead."
)


@fixture(scope="module")
def patient_schema(spark_session, def_pathling):
    """The defined Patient schema, taken from an encoded table, as a user would take it."""
    return def_pathling.encode(
        _documents(spark_session, [SCHEMA_DONOR_JSON]), "Patient"
    ).schema


def _stream(spark_session, tmp_path, lines):
    """Return lines of text as a streaming dataframe, read from a directory of their own."""
    directory = tmp_path / f"stream-{uuid4().hex}"
    directory.mkdir()
    (directory / "input.txt").write_text("\n".join(lines) + "\n")
    return spark_session.readStream.text(str(directory))


def _drain(spark_session, stream):
    """Run a stream to the end of its input into a memory sink and return what it wrote."""
    name = f"drained_{uuid4().hex}"
    stream.writeStream.format("memory").queryName(name).trigger(
        availableNow=True
    ).start().awaitTermination()
    return spark_session.table(name)


def _sorted_json(df):
    return sorted(df.toJSON().collect())


@mark.parametrize("case", SCHEMA_CASES.keys())
def test_stream_encoded_with_schema_is_a_stream_with_the_schema(
    spark_session, def_pathling, patient_schema, tmp_path, case
):
    # A streaming encode with a schema returns a stream with the schema, whose rows are those of
    # a batch encode of the same text (FR-012, US2 scenarios 1 and 2).
    method, mime_type, lines = SCHEMA_CASES[case]
    encode = getattr(def_pathling, method)
    stream = _stream(spark_session, tmp_path, lines)

    encoded = encode(stream, "Patient", mime_type, schema=patient_schema)

    assert encoded.isStreaming
    assert same_schema(patient_schema, encoded.schema)
    batch = encode(
        _documents(spark_session, lines), "Patient", mime_type, schema=patient_schema
    )
    assert _sorted_json(_drain(spark_session, encoded)) == _sorted_json(batch)


@mark.parametrize("method", ["encode", "encode_bundle"])
@mark.parametrize("schema_given", [False, True])
def test_stream_encoded_without_schema_is_refused(
    spark_session, def_pathling, tmp_path, method, schema_given
):
    # A streaming encode without a schema is refused with UnsupportedOperationException and a
    # message telling the user to pass a schema or encode each batch within foreachBatch (FR-013,
    # US2 scenario 3). An explicit None is no schema.
    lines = PATIENT_JSON if method == "encode" else [BUNDLE_JSON]
    stream = _stream(spark_session, tmp_path, lines)
    arguments = {"schema": None} if schema_given else {}
    with raises(Exception) as refusal:
        getattr(def_pathling, method)(stream, "Patient", **arguments)
    assert "UnsupportedOperationException" in str(refusal.value)
    assert REFUSAL in str(refusal.value)


@mark.parametrize("method", ["encode", "encode_bundle"])
def test_encode_with_something_other_than_a_schema_is_refused(
    spark_session, def_pathling, patient_schema, method
):
    # A schema argument that is not a StructType, such as the schema's JSON form, is refused with
    # an error naming the resource type.
    lines = PATIENT_JSON if method == "encode" else [BUNDLE_JSON]
    with raises(TypeError, match="Patient"):
        getattr(def_pathling, method)(
            _documents(spark_session, lines), "Patient", schema=patient_schema.json()
        )


@mark.parametrize("case", SCHEMA_CASES.keys())
def test_batch_encoded_with_schema_gives_the_schemas_output(
    spark_session, def_pathling, patient_schema, case
):
    # A batch encode with a schema gives the schema, with the values the documents hold for its
    # fields, and without the content it has no field for (FR-002).
    method, mime_type, lines = SCHEMA_CASES[case]
    encoded = getattr(def_pathling, method)(
        _documents(spark_session, lines), "Patient", mime_type, schema=patient_schema
    )

    assert not encoded.isStreaming
    assert same_schema(patient_schema, encoded.schema)
    rows = sorted(
        (row["id"], row["gender"], row["birthDate"], row["name"])
        for row in encoded.select("id", "gender", "birthDate", "name.family")
        .toDF("id", "gender", "birthDate", "name")
        .collect()
    )
    assert rows == [("p1", "female", None, ["Ash"]), ("p2", None, "1990-01-01", None)]


@mark.parametrize("method", ["encode", "encode_bundle"])
def test_encode_with_schema_reads_the_given_column(
    spark_session, def_pathling, patient_schema, method
):
    # With a schema, the documents are read from the named column, as they are without one.
    lines = PATIENT_JSON if method == "encode" else [BUNDLE_JSON]
    documents = spark_session.createDataFrame(
        [("x", d) for d in lines], ["other", "resource"]
    )
    encoded = getattr(def_pathling, method)(
        documents, "Patient", column="resource", schema=patient_schema
    )
    assert same_schema(patient_schema, encoded.schema)
    assert sorted(row["id"] for row in encoded.select("id").collect()) == ["p1", "p2"]
