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

import pytest
from pyspark.sql import SparkSession
from pytest import fixture

import pathling.context as context_module
from pathling import PathlingContext
from pathling._spark_defaults import DELTA_COORDINATE, PACKAGES_KEY
from pathling._version import __java_version__

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
        SparkSession.builder.appName("pathling-config-test")
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


def test_default_configurations(spark_session):
    """Test that default configurations can be retrieved correctly."""
    # Create PathlingContext with all default parameters
    pc = PathlingContext.create(spark_session)

    # Get Java PathlingContext instance
    jpc = pc._jpc

    # Retrieve EncodingConfiguration
    encoding_config = jpc.getEncodingConfiguration()
    assert encoding_config.getMaxNestingLevel() == 3
    assert not encoding_config.isEnableExtensions()
    # Default open types should match STANDARD_OPEN_TYPES
    open_types = set(encoding_config.getOpenTypes())
    expected_types = {
        "boolean",
        "code",
        "date",
        "dateTime",
        "decimal",
        "integer",
        "string",
        "Coding",
        "CodeableConcept",
        "Address",
        "Identifier",
        "Reference",
    }
    assert open_types == expected_types

    # Retrieve QueryConfiguration
    query_config = jpc.getQueryConfiguration()
    assert not query_config.isExplainQueries()
    assert query_config.getMaxUnboundTraversalDepth() == 10


def test_custom_configurations(spark_session):
    """Test that custom configurations round-trip correctly."""
    # Create PathlingContext with all non-default values
    pc = PathlingContext.create(
        spark_session,
        max_nesting_level=5,
        enable_extensions=True,
        enabled_open_types=["string", "boolean"],
        explain_queries=True,
        max_unbound_traversal_depth=20,
    )

    # Get Java PathlingContext instance
    jpc = pc._jpc

    # Retrieve EncodingConfiguration and verify custom values
    encoding_config = jpc.getEncodingConfiguration()
    assert encoding_config.getMaxNestingLevel() == 5
    assert encoding_config.isEnableExtensions()
    open_types = set(encoding_config.getOpenTypes())
    assert open_types == {"string", "boolean"}

    # Retrieve QueryConfiguration and verify custom values
    query_config = jpc.getQueryConfiguration()
    assert query_config.isExplainQueries()
    assert query_config.getMaxUnboundTraversalDepth() == 20


def test_spark_conf_with_explicit_session_raises(spark_session):
    """spark_conf cannot be combined with an explicitly supplied SparkSession."""
    with pytest.raises(ValueError, match="spark_conf"):
        PathlingContext.create(spark_session, spark_conf={"spark.driver.memory": "8g"})


class _StopBeforeJvm(Exception):
    """Sentinel raised by the fake session builder so create() stops before
    touching ``spark._jvm`` (which would require a running Spark)."""


def _capture_create(monkeypatch) -> dict:
    """Replaces the session builder with a stub that captures its configuration
    and then raises a sentinel so ``create()`` stops before touching the JVM.

    Also patches ``SparkSession.getActiveSession`` to return ``None`` so
    ``create()`` takes the session-build path.

    :param monkeypatch: the pytest monkeypatch fixture.
    :return: a dict populated with the ``extra_configs`` passed to the builder.
    """
    captured = {}

    def fake_build(extra_configs=None):
        captured.update(extra_configs or {})
        raise _StopBeforeJvm

    monkeypatch.setattr(context_module, "_build_spark_session", fake_build)
    monkeypatch.setattr(SparkSession, "getActiveSession", classmethod(lambda cls: None))
    return captured


def test_create_passes_spark_conf_to_session_builder(monkeypatch):
    """spark_conf is validated, merged and handed to _build_spark_session."""
    captured = _capture_create(monkeypatch)

    with pytest.raises(_StopBeforeJvm):
        PathlingContext.create(spark_conf={"spark.driver.memory": "8g"})

    assert captured == {"spark.driver.memory": "8g"}


def test_create_spark_conf_package_override_warns(monkeypatch):
    """Overriding a managed package coordinate warns and replaces the coordinate."""
    captured = _capture_create(monkeypatch)

    with pytest.warns(UserWarning, match=DELTA_COORDINATE):
        with pytest.raises(_StopBeforeJvm):
            PathlingContext.create(
                spark_conf={PACKAGES_KEY: f"{DELTA_COORDINATE}:3.9.9"}
            )

    assert f"{DELTA_COORDINATE}:3.9.9" in captured[PACKAGES_KEY]


def test_create_spark_conf_with_active_session_raises(monkeypatch):
    """spark_conf raises when an already-active SparkSession would be reused."""
    monkeypatch.setattr(
        SparkSession, "getActiveSession", classmethod(lambda cls: object())
    )

    with pytest.raises(ValueError, match="already-active"):
        PathlingContext.create(spark_conf={"spark.driver.memory": "8g"})
