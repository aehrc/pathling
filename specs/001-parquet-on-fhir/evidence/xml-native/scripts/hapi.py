"""How HAPI writes and reads FHIR XML, and how Spark reads what HAPI writes.

Usage: python -I hapi.py <library-runtime jar>

The jar supplies HAPI; any released library-runtime jar will do. Writes its fixtures to a temporary
directory. Needs PySpark 4.0.
"""

import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

H = 'xmlns=\\"http://www.w3.org/1999/xhtml\\"'
PATIENTS = [
    '{"resourceType":"Patient","id":"p1","text":{"status":"generated","div":"<div '
    + H
    + ">\\n  <p>line one</p>"
    '\\n  <p>a &lt;b&gt; &amp; c</p>\\n</div>"},'
    '"contained":[{"resourceType":"Observation","id":"c-obs","status":"final","code":{"text":"x"}},'
    '{"resourceType":"Patient","id":"c-pat"}],'
    '"name":[{"family":"Smith","given":["John"]}],"address":[{"text":"1 Main St\\nSpringfield"}],'
    '"birthDate":"1970-01-01","_birthDate":{"extension":[{"url":"http://ex/t","valueString":"x"}]}}',
    '{"resourceType":"Patient","id":"p2","name":[{"family":"Doe"}]}',
]
OBSERVATIONS = [
    '{"resourceType":"Observation","id":"o1","status":"final","code":{"text":"t"},'
    '"contained":[{"resourceType":"Patient","id":"c-pat-in-obs"}],"valueQuantity":{"value":1.50}}'
]
NARRATIVE = (
    '<Patient xmlns="http://hl7.org/fhir"><text><status value="generated"/>'
    '<div xmlns="http://www.w3.org/1999/xhtml">\n  <p class="x">a &lt;b&gt; &amp; c &#169;</p><!-- note -->\n'
    "  <table><tr><td><![CDATA[1 < 2]]></td></tr></table>\n</div></text></Patient>"
)


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraClassPath", sys.argv[1])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    ctx = spark._jvm.ca.uhn.fhir.context.FhirContext.forR4()
    json_parser = ctx.newJsonParser()

    print("== 1. A string with markup characters, written by HAPI and read by Spark")
    xml = ctx.newXmlParser().encodeResourceToString(
        json_parser.parseResource(
            '{"resourceType":"Patient","name":[{"given":["Piotr<x> & \\"y\\""]}]}'
        )
    )
    spark.createDataFrame([(xml,)], "value STRING").createOrReplaceTempView("d")
    read = (
        spark.sql(
            "SELECT from_xml(value, 'STRUCT<name: ARRAY<STRUCT<given: ARRAY<STRUCT<_value: STRING>>>>>')"
            ".name[0].given[0]._value AS v FROM d"
        )
        .first()
        .v
    )
    print(f"   HAPI: {xml}\n   Spark: {read}")

    print("== 2. How HAPI reads a narrative")
    resource = ctx.newXmlParser().parseResource(NARRATIVE)
    div = resource.getText().getDiv()
    print(f"   type: {div.getClass().getName()}")
    print(
        f"   children: {[str(c.getNodeType()) + ':' + (c.getName() or repr(c.getContent())) for c in div.getChildNodes()]}"
    )
    print(f"   JSON: {json_parser.encodeResourceToString(resource)}")
    print(f"   XML: {ctx.newXmlParser().encodeResourceToString(resource)}")

    print(
        "== 3. Files written as Pathling writes XML: HAPI, pretty printing off, one document per line"
    )
    writer = ctx.newXmlParser().setPrettyPrint(False)
    with tempfile.TemporaryDirectory() as tmp:
        for resource_type, docs in [
            ("Patient", PATIENTS),
            ("Observation", OBSERVATIONS),
        ]:
            lines = [
                writer.encodeResourceToString(json_parser.parseResource(d))
                for d in docs
            ]
            Path(tmp, resource_type).mkdir()
            Path(tmp, resource_type, "part-00000.xml").write_text(
                "\n".join(lines) + "\n"
            )
        for i, line in enumerate(
            Path(tmp, "Patient", "part-00000.xml").read_text().splitlines()
        ):
            print(f"   line {i + 1}: {line[:180]}{'...' if len(line) > 180 else ''}")
        schema = (
            "id STRUCT<_value: STRING>, text STRUCT<div: STRING>, address ARRAY<STRUCT<text: STRUCT<_value: STRING>>>, "
            "birthDate STRUCT<_value: STRING, extension: ARRAY<STRUCT<_url: STRING>>>"
        )
        reader = (
            spark.read.option("rowTag", "Patient")
            .option("ignoreSurroundingSpaces", "false")
            .schema(schema)
        )
        print("   spark.read.xml, rowTag=Patient, the Patient directory:")
        for r in reader.xml(str(Path(tmp, "Patient"))).collect():
            print(f"      {r}")
        both = reader.xml([str(Path(tmp, "Patient")), str(Path(tmp, "Observation"))])
        print(
            f"   spark.read.xml, rowTag=Patient, both directories: ids {[r.id._value for r in both.collect()]}"
        )
        lines = spark.read.text(str(Path(tmp, "Patient")))
        lines.createOrReplaceTempView("lines")
        count = (
            spark.sql(
                "SELECT count(from_xml(value, 'STRUCT<id: STRUCT<_value: STRING>>').id._value) AS n FROM lines"
            )
            .first()
            .n
        )
        print(
            f"   spark.read.text + from_xml: {lines.count()} lines, {count} resources with an id"
        )

    print("== 4. The narrative example from the JSON form, as HAPI writes it in XML")
    example = (
        '{"resourceType":"Patient","text":{"status":"generated","div":"<div ' + H + ">"
        '<p>Jane Smith, female, born 1980-03-15</p></div>"},"name":[{"family":"Smith","given":["Jane"]}],'
        '"gender":"female","birthDate":"1980-03-15"}'
    )
    pretty = (
        ctx.newXmlParser()
        .setPrettyPrint(True)
        .encodeResourceToString(json_parser.parseResource(example))
    )
    print("   " + pretty.replace("\n", "\n   "))


if __name__ == "__main__":
    main()
