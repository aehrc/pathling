"""How the XML file source selects rows with rowTag, and how from_xml avoids it.

Usage: python -I rowtag.py

Writes its fixtures to a temporary directory. Needs PySpark 4.0.
"""

import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract

H = 'xmlns="http://www.w3.org/1999/xhtml"'
CASES = {
    "01_plain": '<Patient><id value="01"/></Patient>',
    "02_attrs_newline": '<Patient\n   xmlns="http://hl7.org/fhir"><id value="02"/></Patient>',
    "03_self_closing": "<Bundle><Patient/></Bundle>",
    "04_longer_name": '<Patients><id value="04-Patients"/></Patients>'
    '<PatientX><id value="04-PatientX"/></PatientX>',
    "05_lower_case": '<patient><id value="05"/></patient>',
    "06_in_comment": '<Observation><!-- <Patient><id value="06"/></Patient> --></Observation>',
    "07_in_cdata": '<Observation><x><![CDATA[<Patient><id value="07"/></Patient>]]></x></Observation>',
    "08_nested_same_type": '<Patient><id value="08-outer"/><contained><Patient><id value="08-inner"/>'
    '</Patient></contained><gender value="male"/></Patient>',
    "09_contained_in_other_type": '<Observation><contained><Patient><id value="09-contained"/>'
    "</Patient></contained></Observation>",
    "10_in_parameters": '<Parameters><parameter><resource><Patient><id value="10-param"/>'
    "</Patient></resource></parameter></Parameters>",
    "11_bundle_entry": '<Bundle><entry><fullUrl value="urn:uuid:a"/><resource><Patient>'
    '<id value="11-entry"/></Patient></resource></entry></Bundle>',
    "12_namespace_prefix": '<f:Patient xmlns:f="http://hl7.org/fhir"><f:id value="12"/></f:Patient>',
    "13_two_roots": '<Patient><id value="13a"/></Patient><Patient><id value="13b"/></Patient>',
    "14_escaped_text": '<Observation><valueString value="&lt;Patient&gt;&lt;id value=&quot;14&quot;/&gt;'
    '&lt;/Patient&gt;"/></Observation>',
}

# Matches the name of a document's root element, after an optional declaration and comments, and
# captures any namespace prefix separately so that a prefixed document can be refused.
ROOT = r"'^\\s*(?:<\\?xml[^>]*\\?>\\s*)?(?:<!--.*?-->\\s*)*<(?:([A-Za-z_][\\w.-]*):)?([A-Za-z]+)[\\s>/]'"


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    with tempfile.TemporaryDirectory() as tmp:
        for name, xml in CASES.items():
            Path(tmp, f"{name}.xml").write_text(xml)

        print("== 1. spark.read.xml with rowTag=Patient, one file per case")
        rows = (
            spark.read.option("rowTag", "Patient")
            .schema("id STRUCT<_value: STRING>, gender STRUCT<_value: STRING>")
            .xml(tmp)
            .select(
                regexp_extract(input_file_name(), r"([^/]+)\.xml$", 1).alias("file"),
                "id._value",
            )
            .collect()
        )
        found = {}
        for r in rows:
            found.setdefault(r.file, []).append(r._value)
        for name in CASES:
            print(f"   {name:28} -> {found.get(name, 'no row')}")

        print("== 2. The same, with ignoreNamespace=true, for the prefixed document")
        prefixed = (
            spark.read.option("rowTag", "Patient")
            .option("ignoreNamespace", "true")
            .schema("id STRUCT<_value: STRING>")
            .xml(str(Path(tmp, "12_namespace_prefix.xml")))
            .collect()
        )
        print(f"   rows: {prefixed}")

    print("== 3. from_xml over one document per string, with the root element filtered")
    spark.createDataFrame(
        [(k, v) for k, v in CASES.items()], "case STRING, value STRING"
    ).createOrReplaceTempView("docs")
    for r in spark.sql(
        f"""SELECT case,
          regexp_extract(value, {ROOT}, 1) AS prefix, regexp_extract(value, {ROOT}, 2) AS root,
          from_xml(value, 'STRUCT<id: STRUCT<_value: STRING>>').id._value AS id
        FROM docs ORDER BY case"""
    ).collect():
        kept = r.root == "Patient" and r.prefix == ""
        verdict = (
            "kept"
            if kept
            else ("refused: prefixed" if r.prefix else "skipped: other type")
        )
        print(
            f"   {r.case:28} root={r.root:12} prefix={r.prefix or '-':3} id={r.id!s:12} {verdict}"
        )


if __name__ == "__main__":
    main()
