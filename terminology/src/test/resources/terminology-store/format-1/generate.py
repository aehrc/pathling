# /// script
# requires-python = ">=3.9"
# dependencies = ["pathling==9.9.0"]
# ///
"""
Generates a terminology store in format version 1 by importing test fixtures with Pathling 9.9.0,
the last release that wrote that format. The store is committed so that the migration tests run
against what a real earlier release wrote, rather than a reconstruction of it. The jars are
resolved from Maven Central alone, into a fresh Ivy cache, so that a locally built artifact with
the same version cannot stand in for the release.

Run from the repository root:

    uv run terminology/src/test/resources/terminology-store/format-1/generate.py

Author: John Grimes
"""

import shutil
import tempfile
from pathlib import Path

from pathling import PathlingContext
from pyspark.sql import SparkSession

# Paths are relative to the repository root, so the sources recorded in the manifest are too.
HERE = Path("terminology/src/test/resources/terminology-store/format-1")
RESOURCES = HERE.parent.parent
STORE = HERE / "store"

IVY_SETTINGS = """<ivysettings>
  <settings defaultResolver="central"/>
  <resolvers>
    <ibiblio name="central" m2compatible="true" root="https://repo1.maven.org/maven2/"/>
  </resolvers>
</ivysettings>
"""

with tempfile.TemporaryDirectory() as ivy:
    settings = Path(ivy) / "ivysettings.xml"
    settings.write_text(IVY_SETTINGS)
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages",
            "au.csiro.pathling:library-runtime:9.9.0,io.delta:delta-spark_2.13:4.0.0",
        )
        .config("spark.jars.ivy", ivy)
        .config("spark.jars.ivySettings", str(settings))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    shutil.rmtree(STORE, ignore_errors=True)
    pc = PathlingContext.create(spark)
    pc.import_snomed(str(RESOURCES / "rf2-mini" / "international-20240601"), str(STORE))
    for source in [
        RESOURCES / "conceptmap" / "sct-to-icd10.ConceptMap.json",
        RESOURCES / "fhir-import" / "bundle-mixed.json",
        HERE / "faulted.ConceptMap.json",
    ]:
        pc.import_fhir_terminology(str(source), str(STORE))
    spark.stop()

# Hadoop's checksum files are ignored by Git, so they are removed to make the store read the same
# locally as from a clean checkout.
for checksum in STORE.rglob(".*.crc"):
    checksum.unlink()
