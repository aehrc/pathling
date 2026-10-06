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

"""End-to-end tests for terminology import through the Python API.

The FHIR animal-species fixtures are imported through ``pc.import_fhir_terminology``, a local-mode
context is created over the resulting store, and ``member_of`` is evaluated over a DataFrame with no
network access (quickstart scenario 2).
"""

import hashlib
import json
import logging
import os
import tarfile
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pytest import fixture

from pathling import PathlingContext
from pathling._version import __delta_version__, __java_version__, __scala_version__
from pathling.functions import to_coding
from pathling.udfs import member_of

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)
FHIR_FIXTURES = os.path.join(
    PROJECT_ROOT, "terminology", "src", "test", "resources", "fhir-fixtures", "json"
)
ANIMAL_SPECIES = "http://example.org/fhir/CodeSystem/animal-species"
MAMMALS = "http://example.org/fhir/ValueSet/mammals-enumerated"
PACKAGE_NAME = "fixtures"
PACKAGE_VERSION = "1.0.0"


def build_package(directory, name=PACKAGE_NAME, version=PACKAGE_VERSION):
    """Builds a FHIR NPM package from the checked-in JSON fixtures.

    The package holds a ``package.json`` naming the package and version, plus
    every fixture resource, so an import records the package identity.

    :param directory: the directory the tarball and its manifest are written to.
    :param name: the package name recorded in ``package.json``.
    :param version: the package version recorded in ``package.json``.
    :return: the path to the built ``.tgz``.
    """
    manifest_path = os.path.join(directory, "package.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump({"name": name, "version": version, "type": "fhir.ig"}, handle)
    package_path = os.path.join(directory, f"{name}-{version}.tgz")
    with tarfile.open(package_path, "w:gz") as tar:
        tar.add(manifest_path, arcname="package/package.json")
        for entry in sorted(os.listdir(FHIR_FIXTURES)):
            if entry.endswith(".json"):
                tar.add(os.path.join(FHIR_FIXTURES, entry), arcname=f"package/{entry}")
    return package_path


def sha256_hex(path):
    """Computes the SHA-256 of a file as a lower-case hexadecimal string.

    :param path: the file to digest.
    :return: the hexadecimal digest.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


@fixture(scope="module")
def spark_session(request):
    """Creates a Spark session backed by the built library-runtime JAR."""
    logging.getLogger("java_gateway").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.appName("pathling-terminology-import-test")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            f"au.csiro.pathling:library-runtime:{__java_version__},"
            f"io.delta:delta-spark_{__scala_version__}:{__delta_version__}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", mkdtemp())
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())
    return spark


def test_import_fhir_and_member_of(spark_session):
    """Import FHIR content, then test value set membership in local mode."""
    store = os.path.join(mkdtemp(), "store")
    PathlingContext.create(spark_session).import_fhir_terminology(FHIR_FIXTURES, store)

    # Creating the local-mode context registers the terminology UDFs against the local store.
    PathlingContext.create(
        spark_session, terminology_mode="local", terminology_storage_path=store
    )
    df = spark_session.createDataFrame([("dog",), ("sparrow",)], ["code"])
    result = df.select(
        "code",
        member_of(to_coding(F.col("code"), ANIMAL_SPECIES), MAMMALS).alias("member"),
    )
    membership = {row["code"]: row["member"] for row in result.collect()}
    assert membership["dog"] is True
    assert membership["sparrow"] is False


class _JpcSpy:
    """Captures the arguments of ``importFhirTerminology`` calls on the JVM context.

    Standing in for the JVM ``PathlingContext``, this records what the Python
    binding built so the options object can be inspected, without an import
    actually running.
    """

    def __init__(self):
        self.calls = []

    # Named to match the JVM method the binding invokes.
    def importFhirTerminology(self, source, storage_path, options):  # noqa: N802
        """Records one call and returns nothing, as the JVM method does."""
        self.calls.append((source, storage_path, options))


def test_import_fhir_terminology_skipped_records_status(spark_session):
    """A package imported with verification off records skipped provenance."""
    work = mkdtemp()
    package = build_package(work)
    store = os.path.join(work, "store")
    PathlingContext.create(spark_session).import_fhir_terminology(
        package, store, verify_package=False
    )

    manifest = spark_session.read.format("delta").load(os.path.join(store, "manifest"))
    rows = manifest.select(
        "package_verification", "package_name", "package_version", "source_sha256"
    ).collect()
    assert rows
    for row in rows:
        assert row["package_verification"] == "skipped"
        assert row["package_name"] == PACKAGE_NAME
        assert row["package_version"] == PACKAGE_VERSION
        assert row["source_sha256"] == sha256_hex(package)


def test_import_fhir_terminology_passes_registry(spark_session):
    """The verification arguments are carried on a built FhirImportOptions."""
    pc = PathlingContext.create(spark_session)
    spy = _JpcSpy()
    pc._jpc = spy

    pc.import_fhir_terminology(
        "source.tgz",
        "store",
        verify_package=False,
        package_registry="https://packages.example.com",
    )
    source, storage_path, options = spy.calls[0]
    assert (source, storage_path) == ("source.tgz", "store")
    assert options.getPackageRegistry() == "https://packages.example.com"
    assert options.isVerifyPackage() is False

    # The defaults are left to the library rather than restated as options.
    pc.import_fhir_terminology("source.tgz", "store")
    assert spy.calls[1][2] is None
