#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pytest import fixture

from pathling import PathlingContext
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
