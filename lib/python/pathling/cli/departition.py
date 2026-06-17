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

"""Departitioning of Spark directory output into a single file.

Spark's distributed writers produce a directory of part files rather than a
single file. To restore the single-file experience expected from the command
line, this module moves the single data part file out of that directory to the
exact path the user requested and removes the directory. The operation runs
over the Hadoop ``FileSystem`` associated with the target path, so it works
uniformly for local, S3, HDFS, and other Hadoop-compatible destinations,
keeping the move on a single filesystem.

Author: John Grimes.
"""

from pathlib import Path
from typing import Union

from py4j.java_gateway import is_instance_of

from pathling.cli.errors import CliError


def _filesystem(spark, reference_path: Union[str, Path]):
    """Resolves the Hadoop ``FileSystem`` for a path, unwrapping checksums.

    The filesystem is resolved from ``reference_path`` so that operations stay
    on the one filesystem (a same-filesystem rename rather than a
    cross-filesystem copy), whether the destination is local, S3, HDFS, or GCS.
    On a checksummed filesystem - notably the local filesystem - the underlying
    raw filesystem is used, so that renames neither create nor move the sibling
    ``.crc`` checksum files that would otherwise be left beside the single-file
    output.

    :param spark: the Spark session, used to reach the JVM gateway and Hadoop
           configuration.
    :param reference_path: the path whose filesystem is required.
    :return: a tuple of the (possibly raw) ``FileSystem`` and the JVM
             ``org.apache.hadoop.fs.Path`` class.
    """
    path_class = spark._jvm.org.apache.hadoop.fs.Path
    reference = path_class(str(reference_path))
    fs = reference.getFileSystem(spark._jsc.hadoopConfiguration())
    if is_instance_of(
        spark._sc._gateway, fs, "org.apache.hadoop.fs.ChecksumFileSystem"
    ):
        fs = fs.getRawFileSystem()
    return fs, path_class


def remove_path(spark, path: Union[str, Path]) -> None:
    """Removes a file or directory over its Hadoop ``FileSystem``, if it exists.

    Used to clear an existing target before an overwrite and to clean up the
    temporary departition directory. The operation is idempotent: a path that
    does not exist is left untouched.

    :param spark: the Spark session, used to reach the JVM gateway and Hadoop
           configuration.
    :param path: the path to remove.
    """
    fs, path_class = _filesystem(spark, path)
    target = path_class(str(path))
    if fs.exists(target):
        fs.delete(target, True)


def departition(
    spark,
    source_dir: Union[str, Path],
    target_path: Union[str, Path],
    part_extension: str,
) -> None:
    """Moves the single data part file from a Spark output directory to a path.

    Lists ``source_dir`` over the Hadoop ``FileSystem`` of ``target_path``,
    selects the data part files (those ending in ``part_extension``, ignoring
    Spark markers such as ``_SUCCESS`` and ``.crc`` checksums), and:

    - with exactly one data file, moves it to ``target_path``;
    - with no data files (an empty result), creates an empty ``target_path``;
    - with more than one data file, raises an error rather than choosing one.

    The source directory is always removed afterwards.

    :param spark: the Spark session, used to reach the JVM gateway and Hadoop
           configuration.
    :param source_dir: the Spark output directory to departition.
    :param target_path: the destination path for the single data file.
    :param part_extension: the part-file extension to select, without a leading
           dot (e.g. ``"csv"``, ``"json"``, ``"parquet"``).
    :raises CliError: when the source directory contains more than one data
            file.
    """
    fs, path_class = _filesystem(spark, target_path)
    source = path_class(str(source_dir))
    target = path_class(str(target_path))

    suffix = "." + part_extension
    data_files = [
        status.getPath()
        for status in fs.listStatus(source)
        if status.getPath().getName().endswith(suffix)
    ]

    if len(data_files) > 1:
        # repartition(1) should always yield a single data file; more than one
        # means something unexpected, so fail rather than silently pick one.
        names = ", ".join(sorted(file.getName() for file in data_files))
        raise CliError(
            f"Expected a single '{part_extension}' file after departitioning "
            f"but found more than one: {names}."
        )

    try:
        # Ensure the target's parent exists before the move, since a Hadoop
        # rename does not create intermediate directories.
        parent = target.getParent()
        if parent is not None:
            fs.mkdirs(parent)

        if not data_files:
            # An empty result produces no data part; still write a valid, empty
            # single file at the target path.
            fs.create(target, True).close()
        elif not fs.rename(data_files[0], target):
            raise CliError(f"Failed to move the result file to {target_path}.")
    finally:
        # Always remove the temporary Spark output directory and its markers.
        fs.delete(source, True)
