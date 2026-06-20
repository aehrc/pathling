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

"""Unit tests for Spark configuration parsing, validation, and merging.

These tests exercise the pure logic in :mod:`pathling.cli.sparkconf` without
starting Spark: flag parsing, key/value validation and coercion, secret
resolution, and the merge of the user map against Pathling's managed defaults.

Author: John Grimes.
"""

import pytest

from pathling._spark_defaults import (
    CATALOG_KEY,
    DELTA_CATALOG,
    DELTA_COORDINATE,
    DELTA_EXTENSION,
    EXTENSIONS_KEY,
    LIBRARY_RUNTIME_COORDINATE,
    PACKAGES_KEY,
)
from pathling.cli.errors import EXIT_USAGE, CliError
from pathling.cli.sparkconf import (
    merge_spark_conf,
    parse_spark_conf_flags,
    resolve_spark_conf,
    validate_and_coerce,
)


def _coords(packages: str) -> list:
    """Splits a comma-separated packages string, dropping empty entries."""
    return [item for item in packages.split(",") if item]


# ========== parse_spark_conf_flags (T022) ==========


def test_parse_flags_splits_on_first_equals():
    """A flag splits on the first =, so the value may itself contain =."""
    parsed = parse_spark_conf_flags(
        [
            "spark.executor.memory=4g",
            "spark.driver.extraJavaOptions=-Dfoo=bar",
        ]
    )

    assert parsed["spark.executor.memory"] == "4g"
    assert parsed["spark.driver.extraJavaOptions"] == "-Dfoo=bar"


def test_parse_flags_last_occurrence_wins():
    """When a key is repeated, the last occurrence wins."""
    assert parse_spark_conf_flags(["spark.x=1", "spark.x=2"]) == {"spark.x": "2"}


def test_parse_flags_missing_equals_is_usage_error():
    """An argument with no = is a usage error (exit code 2)."""
    with pytest.raises(CliError) as exc_info:
        parse_spark_conf_flags(["spark.executor.memory"])

    assert exc_info.value.exit_code == EXIT_USAGE
    assert "spark.executor.memory" in exc_info.value.message


# ========== validate_and_coerce (T008) ==========


def test_validate_rejects_non_spark_key():
    """A key that does not begin with spark. is a usage error naming the key."""
    with pytest.raises(CliError) as exc_info:
        validate_and_coerce("executor.memory", "4g")

    assert exc_info.value.exit_code == EXIT_USAGE
    assert "executor.memory" in exc_info.value.message


def test_validate_rejects_non_scalar_value():
    """A list or table value is a usage error naming the key."""
    with pytest.raises(CliError) as exc_info:
        validate_and_coerce("spark.jars.packages", ["a:b:1", "c:d:2"])
    assert exc_info.value.exit_code == EXIT_USAGE
    assert "spark.jars.packages" in exc_info.value.message

    with pytest.raises(CliError):
        validate_and_coerce("spark.nested", {"a": 1})


def test_validate_coerces_scalars_to_strings():
    """Integers, floats, and booleans coerce to their Spark string form."""
    assert validate_and_coerce("spark.sql.shuffle.partitions", 16) == "16"
    assert validate_and_coerce("spark.some.ratio", 1.5) == "1.5"
    assert validate_and_coerce("spark.sql.adaptive.enabled", True) == "true"
    assert validate_and_coerce("spark.sql.adaptive.enabled", False) == "false"


def test_validate_passes_string_through_unchanged():
    """A string value is returned unchanged."""
    assert validate_and_coerce("spark.executor.memory", "4g") == "4g"


# ========== resolve_spark_conf (T009) ==========


def test_resolve_reads_file_secret_and_passes_literal_through(tmp_path):
    """A @file value is read via the secret mechanism; a literal passes through."""
    secret_file = tmp_path / "s3-key"
    secret_file.write_text("  super-secret\n", encoding="utf-8")

    resolved = resolve_spark_conf(
        {
            "spark.hadoop.fs.s3a.secret.key": f"@{secret_file}",
            "spark.executor.memory": "4g",
        },
        {},
    )

    assert resolved["spark.hadoop.fs.s3a.secret.key"] == "super-secret"
    assert resolved["spark.executor.memory"] == "4g"


# ========== merge_spark_conf: passthrough (T010) ==========


def test_merge_empty_map_is_empty():
    """An empty user map merges to an empty effective map."""
    assert merge_spark_conf({}) == {}


def test_merge_plain_key_passes_through():
    """A non-managed key is passed through unchanged."""
    assert merge_spark_conf({"spark.executor.memory": "4g"}) == {
        "spark.executor.memory": "4g"
    }


# ========== merge_spark_conf: packages (T011, T012) ==========


def test_merge_packages_unions_with_managed_defaults():
    """A user coordinate is unioned with both managed coordinates, deduplicated."""
    warnings = []
    result = merge_spark_conf(
        {PACKAGES_KEY: "org.apache.hadoop:hadoop-aws:3.4.1"},
        on_warning=warnings.append,
    )

    coords = _coords(result[PACKAGES_KEY])
    # Both managed coordinates remain present, plus the user's addition.
    assert any(c.startswith(f"{LIBRARY_RUNTIME_COORDINATE}:") for c in coords)
    assert any(c.startswith(f"{DELTA_COORDINATE}:") for c in coords)
    assert "org.apache.hadoop:hadoop-aws:3.4.1" in coords
    # No duplicate group:artifact entries.
    group_artifacts = [":".join(c.split(":")[:2]) for c in coords]
    assert len(group_artifacts) == len(set(group_artifacts))
    # Adding a plain package does not warn.
    assert warnings == []


def test_merge_packages_version_override_warns_once():
    """Overriding a managed coordinate version applies it and warns once."""
    warnings = []
    result = merge_spark_conf(
        {PACKAGES_KEY: f"{DELTA_COORDINATE}:3.9.9"},
        on_warning=warnings.append,
    )

    coords = _coords(result[PACKAGES_KEY])
    # The Delta coordinate appears exactly once, at the user's version, with the
    # default version replaced rather than duplicated.
    delta_entries = [c for c in coords if c.startswith(f"{DELTA_COORDINATE}:")]
    assert delta_entries == [f"{DELTA_COORDINATE}:3.9.9"]
    # The library-runtime coordinate is still present at its managed version.
    assert any(c.startswith(f"{LIBRARY_RUNTIME_COORDINATE}:") for c in coords)
    # Exactly one warning, naming the overridden coordinate.
    assert len(warnings) == 1
    assert DELTA_COORDINATE in warnings[0]


def test_merge_packages_same_version_is_deduplicated_without_warning():
    """Supplying a managed coordinate at the default version does not warn."""
    from pathling._spark_defaults import managed_spark_defaults

    # Extract the managed Delta coordinate at its exact default version.
    managed_delta = next(
        c
        for c in _coords(managed_spark_defaults()[PACKAGES_KEY])
        if c.startswith(f"{DELTA_COORDINATE}:")
    )
    warnings = []
    result = merge_spark_conf({PACKAGES_KEY: managed_delta}, on_warning=warnings.append)

    coords = _coords(result[PACKAGES_KEY])
    # The coordinate appears exactly once and no warning is emitted.
    assert coords.count(managed_delta) == 1
    assert warnings == []


# ========== merge_spark_conf: extensions (T013) ==========


def test_merge_extensions_retains_delta_and_unions():
    """User extensions are unioned with the Delta extension, deduplicated."""
    result = merge_spark_conf({EXTENSIONS_KEY: "com.example.MyExtension"})

    extensions = result[EXTENSIONS_KEY].split(",")
    assert DELTA_EXTENSION in extensions
    assert "com.example.MyExtension" in extensions


def test_merge_extensions_deduplicates_delta():
    """Supplying the Delta extension again keeps it present exactly once."""
    result = merge_spark_conf(
        {EXTENSIONS_KEY: f"{DELTA_EXTENSION},com.example.MyExtension"}
    )

    extensions = result[EXTENSIONS_KEY].split(",")
    assert extensions.count(DELTA_EXTENSION) == 1
    assert "com.example.MyExtension" in extensions


# ========== merge_spark_conf: catalog (T014) ==========


def test_merge_catalog_delta_value_is_dropped():
    """Setting the catalog to the Delta value is a no-op (dropped)."""
    assert merge_spark_conf({CATALOG_KEY: DELTA_CATALOG}) == {}


def test_merge_catalog_other_value_is_error():
    """Setting the catalog to any other value names the key and expected value."""
    with pytest.raises(CliError) as exc_info:
        merge_spark_conf({CATALOG_KEY: "com.example.OtherCatalog"})

    assert exc_info.value.exit_code == EXIT_USAGE
    assert CATALOG_KEY in exc_info.value.message
    assert DELTA_CATALOG in exc_info.value.message
