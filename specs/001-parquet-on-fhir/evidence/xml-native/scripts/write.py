"""Writes FHIR XML from a resource in the layout's shape with Spark's XML writer.

Usage: python -I write.py <output directory>

Writes to_xml.xml, the to_xml output with the narrative substituted, for write_check.py to
compare against the source with HAPI. HAPI is not on this classpath, because the Woodstox it brings
clashes with the one Spark's XML writer uses. Needs PySpark 4.0.
"""

import json
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

SOURCE = {
    "resourceType": "Patient",
    "id": "p1",
    "text": {
        "status": "generated",
        "div": '<div xmlns="http://www.w3.org/1999/xhtml"><p>Jane &lt;Smith&gt; &amp; co</p></div>',
    },
    "extension": [
        {"url": "http://ex/race", "valueCoding": {"system": "urn:r", "code": "2106-3"}}
    ],
    "active": True,
    "name": [
        {
            "family": "Smith",
            "given": ["Jane", "Q"],
            "_given": [
                None,
                {"id": "g2", "extension": [{"url": "http://ex/q", "valueString": "x"}]},
            ],
        }
    ],
    "address": [{"text": '1 Main St\nSpringfield <b> & "q"'}],
    "birthDate": "1970-01-01",
    "_birthDate": {
        "extension": [{"url": "http://ex/t", "valueDateTime": "1970-01-01T05:00:00Z"}]
    },
    "photo": [{"data": "aGVsbG8gd29ybGQ="}],
}


def v(e: str) -> str:
    """Wraps a value as a primitive element, whose value is written as an attribute."""
    return f"named_struct('_value', {e})"


# What a generator would emit to turn the layout's shape into FHIR XML's: attributes are fields
# prefixed with an underscore, and the narrative is a placeholder replaced after serialisation.
XML_SHAPE = f"""SELECT named_struct(
  '_xmlns', 'http://hl7.org/fhir',
  'id', {v("id")},
  'text', named_struct('status', {v("text.status")}, 'div', '__DIV__'),
  'extension', transform(extension, e -> named_struct('_url', e.url, 'valueCoding',
      named_struct('system', {v("e.valueCoding.system")}, 'code', {v("e.valueCoding.code")}))),
  'active', {v("CAST(active AS STRING)")},
  'name', transform(name, n -> named_struct('family', {v("n.family")},
      'given', transform(arrays_zip(n.given, n._given), g -> named_struct('_id', g._given.id, '_value', g.given,
          'extension', transform(g._given.extension, x -> named_struct('_url', x.url, 'valueString', {v("x.valueString")})))))),
  'address', transform(address, a -> named_struct('text', {v("a.text")})),
  'birthDate', named_struct('_value', birthDate, 'extension', transform(_birthDate.extension,
      x -> named_struct('_url', x.url, 'valueDateTime', {v("x.valueDateTime")}))),
  'photo', transform(photo, p -> named_struct('data', {v("p.data")}))) AS r, text.div AS div
FROM layout"""


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    spark.read.json(
        spark.sparkContext.parallelize([json.dumps(SOURCE)])
    ).createOrReplaceTempView("layout")
    spark.sql(XML_SHAPE).createOrReplaceTempView("x")

    print("== 1. to_xml, with the narrative substituted")
    xml = (
        spark.sql(
            "SELECT replace(to_xml(r, map('rowTag', 'Patient')), '<div>__DIV__</div>', div) AS xml FROM x"
        )
        .first()
        .xml
    )
    print(xml)
    (out / "to_xml.xml").write_text(xml)
    (out / "source.json").write_text(json.dumps(SOURCE))

    print("== 2. to_xml with indent set to the empty string")
    one_line = (
        spark.sql(
            "SELECT replace(to_xml(r, map('rowTag', 'Patient', 'indent', '')), '<div>__DIV__</div>', div) AS xml FROM x"
        )
        .first()
        .xml
    )
    print(repr(one_line[:160]) + " ...")

    print("== 3. df.write.xml")
    files = out / "write-xml"
    shutil.rmtree(files, ignore_errors=True)
    spark.sql("SELECT r.* FROM x").coalesce(1).write.option("rowTag", "Patient").xml(
        str(files)
    )
    written = "".join(f.read_text() for f in files.glob("*.xml"))
    print("\n".join(written.splitlines()[:8]) + "\n...")


if __name__ == "__main__":
    main()
