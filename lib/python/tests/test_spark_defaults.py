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

"""Unit tests for the managed Spark defaults extraction.

These tests confirm that the PySpark-free defaults module is the single source
of truth for the managed coordinates, the Delta extension, and the Delta
catalog, and that its values match those declared in ``_version.py``. They do
not start Spark.

Author: John Grimes.
"""

import pytest

from pathling._spark_defaults import (
    CATALOG_KEY,
    DELTA_CATALOG,
    MANAGED_COORDINATES,
    managed_spark_defaults,
    merge_spark_conf,
    validate_spark_conf,
)
from pathling._version import (
    __delta_version__,
    __java_version__,
    __scala_version__,
)


def test_managed_defaults_match_version_module():
    """The managed defaults reproduce the exact coordinates from _version.py."""
    defaults = managed_spark_defaults()

    # The packages string must list both managed coordinates at the versions
    # declared in _version.py, matching the historical inline literal exactly
    # (including the trailing comma).
    expected_packages = (
        f"au.csiro.pathling:library-runtime:{__java_version__},"
        f"io.delta:delta-spark_{__scala_version__}:{__delta_version__},"
    )
    assert defaults["spark.jars.packages"] == expected_packages

    # The Delta SQL extension and catalog are fixed class names.
    assert defaults["spark.sql.extensions"] == (
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    assert defaults["spark.sql.catalog.spark_catalog"] == (
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )


def test_managed_coordinate_identities():
    """The managed coordinate identities are the group:artifact pairs."""
    assert "au.csiro.pathling:library-runtime" in MANAGED_COORDINATES
    assert f"io.delta:delta-spark_{__scala_version__}" in MANAGED_COORDINATES
    # Exactly the two coordinates Pathling manages, no more.
    assert len(MANAGED_COORDINATES) == 2


# ========== validate_spark_conf ==========


def test_validate_spark_conf_accepts_plain_string_map():
    """A plain string map with spark.-prefixed keys validates unchanged."""
    user_map = {"spark.driver.memory": "8g", "spark.executor.memory": "4g"}
    assert validate_spark_conf(user_map) == user_map


def test_validate_spark_conf_rejects_non_spark_key():
    """A key that does not begin with spark. raises ValueError naming the key."""
    with pytest.raises(ValueError, match="executor.memory"):
        validate_spark_conf({"executor.memory": "4g"})


def test_validate_spark_conf_rejects_non_string_value():
    """A non-string value raises TypeError naming the key."""
    with pytest.raises(TypeError, match="spark.executor.memory"):
        validate_spark_conf({"spark.executor.memory": 4})


# ========== merge_spark_conf (shared implementation) ==========


def test_shared_merge_plain_key_passes_through():
    """A non-managed key is passed through unchanged."""
    assert merge_spark_conf({"spark.executor.memory": "4g"}) == {
        "spark.executor.memory": "4g"
    }


def test_shared_merge_catalog_mismatch_raises_value_error():
    """Setting the catalog to a non-Delta value raises ValueError (not CliError)."""
    with pytest.raises(ValueError, match=CATALOG_KEY):
        merge_spark_conf({CATALOG_KEY: "com.example.OtherCatalog"})
    # The managed Delta catalog itself is a no-op and is dropped.
    assert merge_spark_conf({CATALOG_KEY: DELTA_CATALOG}) == {}
