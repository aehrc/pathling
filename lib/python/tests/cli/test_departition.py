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

"""Unit tests for the departition helper.

These exercise :func:`pathling.cli.departition.departition` against locally
constructed directories that mimic Spark's part-file output (data parts plus
``_SUCCESS`` and ``.crc`` markers). A Spark session is required only to reach
the JVM gateway and Hadoop configuration, so the shared session fixture is
reused; no DataFrame is written.

Author: John Grimes.
"""

from pathlib import Path

import pytest
from pytest import fixture

from pathling.cli.departition import departition
from pathling.cli.errors import CliError


@fixture(scope="module")
def spark(pathling_ctx):
    """Provides the Spark session from the shared Pathling context.

    :param pathling_ctx: the shared Pathling context fixture.
    :return: the underlying Spark session.
    """
    return pathling_ctx.spark


def _make_output_dir(
    directory: Path, part_files: dict, *, markers: bool = True
) -> Path:
    """Builds a directory mimicking Spark's part-file output.

    :param directory: the directory to create and populate.
    :param part_files: a mapping of part file name to its text content.
    :param markers: when True, add ``_SUCCESS`` and ``.crc`` checksum markers.
    :return: the populated directory.
    """
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in part_files.items():
        (directory / name).write_text(content)
        if markers:
            # Spark's local filesystem writes a hidden ".<name>.crc" checksum.
            (directory / f".{name}.crc").write_text("crc")
    if markers:
        (directory / "_SUCCESS").write_text("")
        (directory / "._SUCCESS.crc").write_text("crc")
    return directory


# ========== Single data file ==========


def test_single_part_file_moved_to_exact_target(spark, tmp_path):
    """A single part file is moved verbatim to the exact target path."""
    source = _make_output_dir(
        tmp_path / "_tmp", {"part-00000-abc-c000.csv": "id,name\n1,Smith\n"}
    )
    target = tmp_path / "out.csv"

    departition(spark, source, target, "csv")

    assert target.is_file()
    assert target.read_text() == "id,name\n1,Smith\n"


def test_source_directory_removed_after_departition(spark, tmp_path):
    """The source directory is deleted once the data file has been moved."""
    source = _make_output_dir(tmp_path / "_tmp", {"part-00000-abc-c000.csv": "data\n"})
    target = tmp_path / "out.csv"

    departition(spark, source, target, "csv")

    assert not source.exists()


# ========== Markers ==========


def test_markers_ignored_and_removed(spark, tmp_path):
    """``_SUCCESS`` and ``.crc`` markers are never picked and are removed."""
    source = _make_output_dir(
        tmp_path / "_tmp",
        {"part-00000-abc-c000.csv": "real\n"},
        markers=True,
    )
    target = tmp_path / "out.csv"

    departition(spark, source, target, "csv")

    # The target holds the data part, not a marker, and the source (with all
    # its markers) is gone.
    assert target.read_text() == "real\n"
    assert not source.exists()


# ========== More than one data file ==========


def test_multiple_data_files_raises_clear_error(spark, tmp_path):
    """More than one data part file is an error rather than a silent choice."""
    source = _make_output_dir(
        tmp_path / "_tmp",
        {
            "part-00000-abc-c000.csv": "a\n",
            "part-00001-abc-c000.csv": "b\n",
        },
    )
    target = tmp_path / "out.csv"

    with pytest.raises(CliError) as exc_info:
        departition(spark, source, target, "csv")

    # The message names the problem so the user understands why it failed.
    assert "more than one" in exc_info.value.message.lower()


# ========== Empty result (zero data files) ==========


def test_zero_data_files_produces_empty_target(spark, tmp_path):
    """An empty result (no data part) yields an empty single target file."""
    # Only markers, no data part file - as Spark produces for the text formats
    # when there are no rows.
    source = _make_output_dir(tmp_path / "_tmp", {}, markers=True)
    target = tmp_path / "out.csv"

    departition(spark, source, target, "csv")

    assert target.is_file()
    assert target.read_text() == ""
    assert not source.exists()


# ========== Extensions ==========


@pytest.mark.parametrize("extension", ["csv", "json", "parquet"])
def test_works_for_each_part_extension(spark, tmp_path, extension):
    """The helper selects the data file by the writer's part extension."""
    source = _make_output_dir(
        tmp_path / "_tmp",
        {f"part-00000-abc-c000.{extension}": "payload"},
    )
    target = tmp_path / f"out.{extension}"

    departition(spark, source, target, extension)

    assert target.read_text() == "payload"


# ========== Nested target directory ==========


def test_nested_target_directory_is_created(spark, tmp_path):
    """A target under a non-existent parent directory is created as needed."""
    source = _make_output_dir(
        tmp_path / "_tmp", {"part-00000-abc-c000.csv": "nested\n"}
    )
    target = tmp_path / "deeply" / "nested" / "out.csv"

    departition(spark, source, target, "csv")

    assert target.is_file()
    assert target.read_text() == "nested\n"
