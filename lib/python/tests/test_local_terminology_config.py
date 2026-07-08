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

"""Tests for the local terminology configuration parameters of ``PathlingContext.create``.

The validation cases raise before the Spark cold start, so they run without a session; the mapping
case checks that local mode selects the local terminology service factory over an imported store.
"""

import logging
import os
from tempfile import mkdtemp

import pytest
from pyspark.sql import SparkSession

from pathling import PathlingContext
from pathling._version import __delta_version__, __java_version__, __scala_version__

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)
FHIR_FIXTURES = os.path.join(
    PROJECT_ROOT, "terminology", "src", "test", "resources", "fhir-fixtures", "json"
)


@pytest.fixture(scope="module")
def spark_session(request):
    """Creates a Spark session backed by the built library-runtime JAR."""
    logging.getLogger("java_gateway").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.appName("pathling-local-terminology-config-test")
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


def test_local_mode_requires_storage_path():
    """Local mode without a storage path raises before any JVM call."""
    with pytest.raises(ValueError, match="terminology_storage_path is required"):
        PathlingContext.create(terminology_mode="local")


def test_invalid_terminology_mode_raises():
    """An unrecognised terminology mode raises a clear error."""
    with pytest.raises(ValueError, match="terminology_mode must be"):
        PathlingContext.create(
            terminology_mode="hybrid", terminology_storage_path="/tmp/store"
        )


def test_local_mode_selects_local_factory(spark_session):
    """Local mode over an imported store builds a local terminology service factory."""
    store = os.path.join(mkdtemp(), "store")
    PathlingContext.create(spark_session).import_fhir_terminology(FHIR_FIXTURES, store)

    pc = PathlingContext.create(
        spark_session,
        terminology_mode="local",
        terminology_storage_path=store,
        default_snomed_edition="900000000000207008",
        expansion_cache_size=50,
    )
    factory_class = pc._jpc.getTerminologyServiceFactory().getClass().getSimpleName()
    assert factory_class == "LocalTerminologyServiceFactory"
