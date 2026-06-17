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

"""Output rendering and progress reporting for the command line interface.

Tabular results render to a human-readable table by default, with CSV and
NDJSON alternatives for piping, and file output (CSV, NDJSON, Parquet, and
Delta) via ``-o``. File output is produced by Spark's native writers and, by
default, departitioned to a single file at the requested path. Data is written
to stdout; progress, status, and the write confirmation go to stderr so that
piped output stays clean.

Author: John Grimes.
"""

import csv
import io
import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import click
from rich.console import Console
from rich.table import Table

from pathling.cli.departition import departition, remove_path
from pathling.cli.errors import EXIT_USAGE, CliError

# The default cap on rows collected for stdout table rendering.
DEFAULT_LIMIT = 1000


class OutputFormat:
    """The recognised output formats."""

    TABLE = "table"
    CSV = "csv"
    NDJSON = "ndjson"
    PARQUET = "parquet"
    DELTA = "delta"


# Formats that may be written to a file with ``-o``.
FILE_FORMATS = (
    OutputFormat.CSV,
    OutputFormat.NDJSON,
    OutputFormat.PARQUET,
    OutputFormat.DELTA,
)

# Formats that require ``-o`` because they cannot render to stdout.
PATH_REQUIRED_FORMATS = (OutputFormat.PARQUET, OutputFormat.DELTA)

# Maps each departitioned file format to the extension Spark gives its part
# files, used to select the single data file from the temporary output
# directory.
_PART_EXTENSIONS = {
    OutputFormat.CSV: "csv",
    OutputFormat.NDJSON: "json",
    OutputFormat.PARQUET: "parquet",
}

# The complete set of output formats accepted by the ``--format`` option, shared
# by every command that produces tabular output.
OUTPUT_FORMAT_CHOICE = click.Choice(
    [
        OutputFormat.TABLE,
        OutputFormat.CSV,
        OutputFormat.NDJSON,
        OutputFormat.PARQUET,
        OutputFormat.DELTA,
    ]
)


def output_options(func):
    """Applies the shared output options to a command callback.

    These options form the common output surface of every command that emits a
    result DataFrame - ``--format``, ``-o``/``--output``, ``--limit``,
    ``--overwrite``, and ``--departition/--no-departition`` - and are resolved
    together by :func:`resolve_output` and consumed by :func:`write_output`.

    :param func: the command callback to decorate.
    :return: the decorated callback.
    """
    options = [
        click.option(
            "--format",
            "output_format",
            type=OUTPUT_FORMAT_CHOICE,
            help="Output format (default: table; inferred from -o extension).",
        ),
        click.option("-o", "--output", "output", help="Write results to this path."),
        click.option(
            "--limit",
            default=DEFAULT_LIMIT,
            show_default=True,
            help="Row cap for table output.",
        ),
        click.option(
            "--overwrite", is_flag=True, help="Replace an existing output path."
        ),
        click.option(
            "--departition/--no-departition",
            "departition",
            default=True,
            show_default=True,
            help="Write file output as a single file (or a Spark directory of "
            "part files).",
        ),
    ]
    for option in reversed(options):
        func = option(func)
    return func


# Maps file extensions to output formats for inference.
_EXTENSION_FORMATS = {
    ".csv": OutputFormat.CSV,
    ".ndjson": OutputFormat.NDJSON,
    ".jsonl": OutputFormat.NDJSON,
    ".parquet": OutputFormat.PARQUET,
}


@dataclass
class OutputSpec:
    """Describes where and how results leave the CLI.

    :param path: the output path, or None to write to stdout.
    :param format: one of the :class:`OutputFormat` values.
    :param limit: the row cap for stdout table rendering.
    :param overwrite: whether an existing output path may be replaced.
    :param departition: whether file output is departitioned to a single file
           (the default) rather than left as a Spark directory of part files.
    """

    path: Optional[Path]
    format: str
    limit: int = DEFAULT_LIMIT
    overwrite: bool = False
    departition: bool = True


def infer_format_from_extension(path: Path) -> Optional[str]:
    """Infers an output format from a file extension.

    :param path: the output path.
    :return: the inferred format, or None when the extension is unrecognised.
    """
    return _EXTENSION_FORMATS.get(path.suffix.lower())


def resolve_output(
    output_path: Optional[str],
    format_flag: Optional[str],
    limit: int = DEFAULT_LIMIT,
    overwrite: bool = False,
    departition: bool = True,
) -> OutputSpec:
    """Resolves and validates output options.

    :param output_path: the ``-o`` path, or None for stdout.
    :param format_flag: the ``--format`` value, or None to default/infer.
    :param limit: the stdout table row cap.
    :param overwrite: whether replacing an existing output path is allowed.
    :param departition: whether file output is departitioned to a single file.
    :return: the resolved :class:`OutputSpec`.
    :raises CliError: when the combination of options is invalid.
    """
    path = Path(output_path) if output_path else None

    if format_flag is not None:
        fmt = format_flag
    elif path is not None:
        if path.suffix.lower() == ".json":
            # The JSON-array format has been removed; point the user at NDJSON
            # rather than silently treating a .json file as newline-delimited.
            raise CliError(
                "The JSON-array output format has been removed. Use --format "
                "ndjson, or a .ndjson filename, for newline-delimited JSON.",
                exit_code=EXIT_USAGE,
            )
        inferred = infer_format_from_extension(path)
        if inferred is None:
            raise CliError(
                f"Could not infer an output format from '{path}'. "
                "Use --format csv|ndjson|parquet|delta.",
                exit_code=EXIT_USAGE,
            )
        fmt = inferred
    else:
        fmt = OutputFormat.TABLE

    if fmt == OutputFormat.TABLE and path is not None:
        raise CliError(
            "The table format cannot be written to a file. Use --format "
            "csv|ndjson|parquet|delta with -o, or drop -o for a table.",
            exit_code=EXIT_USAGE,
        )
    if fmt in PATH_REQUIRED_FORMATS and path is None:
        raise CliError(
            f"The {fmt} format requires an output path. Add -o <path>.",
            exit_code=EXIT_USAGE,
        )

    return OutputSpec(
        path=path,
        format=fmt,
        limit=limit,
        overwrite=overwrite,
        departition=departition,
    )


def check_overwrite(path: Path, overwrite: bool) -> None:
    """Fails when an output path already exists and overwrite was not requested.

    :param path: the output path to check.
    :param overwrite: whether overwriting is permitted.
    :raises CliError: when the path exists and overwrite is False.
    """
    if path.exists() and not overwrite:
        raise CliError(
            f"Output path already exists: {path}. Pass --overwrite to replace it."
        )


def _cell(value: object) -> str:
    """Formats a value as a table cell string.

    :param value: the cell value.
    :return: an empty string for None, otherwise the string form.
    """
    return "" if value is None else str(value)


def render_table(columns: Sequence[str], rows: Sequence[Sequence]) -> str:
    """Renders rows as a human-readable table with a row-count caption.

    :param columns: the column names.
    :param rows: the row values.
    :return: the rendered table as a string.
    """
    table = Table(caption=f"{len(rows)} rows", caption_justify="left")
    for column in columns:
        table.add_column(str(column))
    for row in rows:
        table.add_row(*[_cell(value) for value in row])
    buffer = io.StringIO()
    # Disable markup and highlighting so that data values containing square
    # brackets (e.g. Spark error codes or array-like strings) render verbatim
    # rather than being interpreted as Rich markup.
    Console(file=buffer, width=120, markup=False, highlight=False).print(table)
    return buffer.getvalue()


def render_csv(columns: Sequence[str], rows: Sequence[Sequence]) -> str:
    """Renders rows as CSV with a header line.

    :param columns: the column names.
    :param rows: the row values.
    :return: the CSV text.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(list(columns))
    for row in rows:
        writer.writerow(["" if value is None else value for value in row])
    return buffer.getvalue()


def _records(columns: Sequence[str], rows: Sequence[Sequence]) -> List[dict]:
    """Builds a list of column-keyed dicts from rows.

    :param columns: the column names.
    :param rows: the row values.
    :return: a list of record dicts preserving column order.
    """
    return [dict(zip(columns, row)) for row in rows]


def render_ndjson(columns: Sequence[str], rows: Sequence[Sequence]) -> str:
    """Renders rows as newline-delimited JSON objects.

    :param columns: the column names.
    :param rows: the row values.
    :return: the NDJSON text.
    """
    return "\n".join(
        json.dumps(record, default=str) for record in _records(columns, rows)
    )


def render_rows(columns: Sequence[str], rows: Sequence[Sequence], fmt: str) -> str:
    """Renders rows in the requested stdout format.

    :param columns: the column names.
    :param rows: the row values.
    :param fmt: one of table, csv, or ndjson.
    :return: the rendered text.
    :raises CliError: when the format cannot render to stdout.
    """
    if fmt == OutputFormat.TABLE:
        return render_table(columns, rows)
    if fmt == OutputFormat.CSV:
        return render_csv(columns, rows)
    if fmt == OutputFormat.NDJSON:
        return render_ndjson(columns, rows)
    raise CliError(
        f"The {fmt} format cannot be rendered to stdout.", exit_code=EXIT_USAGE
    )


def stderr_console() -> Console:
    """Creates a Rich console that writes to stderr.

    Markup and highlighting are disabled so that arbitrary message text - error
    messages, file paths, and JVM error codes that may contain square brackets -
    is printed verbatim and never misinterpreted as Rich markup (which would
    otherwise raise a ``MarkupError``).

    :return: a console bound to stderr for progress and status messages.
    """
    return Console(stderr=True, markup=False, highlight=False)


@contextmanager
def progress_status(console: Console, message: str, verbose: bool = False):
    """Shows a status spinner on stderr for a long-running stage.

    :param console: the stderr console.
    :param message: the status message to display.
    :param verbose: when True, print a plain line instead of a spinner so that
           it does not interfere with verbose log output.
    """
    if verbose:
        console.print(message)
        yield
        return
    with console.status(message):
        yield


def write_output(df, spec: OutputSpec, console: Console) -> None:
    """Writes a result DataFrame to stdout or a file per the output spec.

    For stdout, the table format is capped at ``spec.limit`` rows; other
    formats stream the full result. File output is produced by Spark's native
    writers (see :func:`_write_file`) and confirmed with a single stderr line
    naming the format and path.

    :param df: the result Spark DataFrame.
    :param spec: the resolved output specification.
    :param console: the stderr console for the confirmation message.
    :raises CliError: when the output path exists without ``--overwrite``.
    """
    columns = list(df.columns)

    if spec.path is None:
        if spec.format == OutputFormat.TABLE:
            rows = [list(row) for row in df.limit(spec.limit).collect()]
        else:
            rows = [list(row) for row in df.collect()]
        text = render_rows(columns, rows, spec.format)
        # Strip any trailing newline so print's own newline does not introduce
        # a spurious blank line (which would parse as an empty CSV row).
        print(text.rstrip("\n"))
        return

    check_overwrite(spec.path, spec.overwrite)
    _write_file(df, spec)
    console.print(f"Wrote {spec.format} output to {spec.path}.")


def _write_file(df, spec: OutputSpec) -> None:
    """Writes a result DataFrame to a file using Spark's native writers.

    CSV, NDJSON, and Parquet are written by Spark rather than collected onto
    the driver, so a result larger than driver memory can be written. By
    default the output is departitioned to a single file: the frame is
    repartitioned to one partition, written to a uniquely named temporary
    directory beside the target (on the same filesystem, so the final move is a
    rename), and the single data part file is moved to the target path before
    the temporary directory is removed. Delta is always written as a Spark
    table directory and is never departitioned.

    No row count is reported: a Spark write returns none, and obtaining one
    would require collecting the result or re-executing the query (re-running
    terminology UDFs against the server).

    :param df: the result Spark DataFrame.
    :param spec: the resolved output specification with a non-None path.
    :raises CliError: when departitioning finds more than one data part file.
    """
    path = spec.path
    spark = df.sparkSession

    if spec.format == OutputFormat.DELTA:
        writer = df.write.format("delta")
        if spec.overwrite:
            writer = writer.mode("overwrite")
        writer.save(str(path))
        return

    # Replace any existing target up front when overwriting, so a directory
    # write or a departition move into place finds a clear path (Delta handles
    # its own overwrite above).
    if spec.overwrite:
        remove_path(spark, path)

    if not spec.departition:
        # Leave Spark's native directory of part files at the target, written
        # with full parallelism (no repartition to a single partition).
        _write_spark_directory(df, spec.format, path)
        return

    part_extension = _PART_EXTENSIONS[spec.format]
    # A uniquely named, hidden sibling of the target keeps the temporary output
    # on the same filesystem so departitioning moves rather than copies.
    temp_dir = path.parent / f".{path.name}.departition-{uuid.uuid4().hex}"
    try:
        _write_spark_directory(df.repartition(1), spec.format, temp_dir)
        departition(spark, temp_dir, path, part_extension)
    finally:
        # Always remove the temporary directory, including on a write failure.
        remove_path(spark, temp_dir)


def _write_spark_directory(frame, fmt: str, target_dir) -> None:
    """Writes a frame to a Spark directory of part files in the given format.

    :param frame: the Spark DataFrame to write (already repartitioned as
           required by the caller).
    :param fmt: the file format - CSV, NDJSON, or Parquet.
    :param target_dir: the directory Spark writes its part files into.
    """
    writer = frame.write.mode("overwrite")
    if fmt == OutputFormat.CSV:
        writer.option("header", "true").csv(str(target_dir))
    elif fmt == OutputFormat.NDJSON:
        # Retain null-valued fields so every record carries the same keys,
        # matching the prior driver-side behaviour.
        writer.option("ignoreNullFields", "false").json(str(target_dir))
    else:
        writer.parquet(str(target_dir))
