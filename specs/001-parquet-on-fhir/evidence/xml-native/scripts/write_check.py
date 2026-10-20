"""Parses the XML that write.py wrote with HAPI and compares it with the source resource.

Usage: python -I write_check.py <library-runtime jar> <directory write.py wrote to>

Runs in its own JVM, apart from Spark's XML writer, because the Woodstox the jar brings clashes
with the one that writer uses. Needs PySpark 4.0.
"""

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraClassPath", sys.argv[1])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    ctx = spark._jvm.ca.uhn.fhir.context.FhirContext.forR4()
    out = Path(sys.argv[2])
    xml = (out / "to_xml.xml").read_text()
    source = (out / "source.json").read_text()
    back = json.loads(
        ctx.newJsonParser().encodeResourceToString(
            ctx.newXmlParser().parseResource(xml)
        )
    )
    want = json.loads(
        ctx.newJsonParser().encodeResourceToString(
            ctx.newJsonParser().parseResource(source)
        )
    )
    print(f"== HAPI parses Spark's XML; equal to the source: {back == want}")
    for key in want:
        if back.get(key) != want.get(key):
            print(
                f"   differs: {key}\n      got:  {back.get(key)}\n      want: {want.get(key)}"
            )


if __name__ == "__main__":
    main()
