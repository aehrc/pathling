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

import logging
import os
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
