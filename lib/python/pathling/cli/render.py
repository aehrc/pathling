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

Tabular results render to a human-readable table by default, with CSV, JSON,
and NDJSON alternatives for piping, and file output (including Parquet and
Delta) via ``-o``. Data is written to stdout; progress, status, and the
row-count confirmation for file output go to stderr so that piped output stays
clean.

Author: John Grimes.
"""

import csv
import io
import json
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

from rich.console import Console
from rich.table import Table

from pathling.cli.errors import EXIT_USAGE, CliError

# The default cap on rows collected for stdout table rendering.
DEFAULT_LIMIT = 1000


class OutputFormat:
    """The recognised output formats."""

    TABLE = "table"
    CSV = "csv"
    JSON = "json"
    NDJSON = "ndjson"
    PARQUET = "parquet"
    DELTA = "delta"


# Formats that may be written to a file with ``-o``.
FILE_FORMATS = (
    OutputFormat.CSV,
    OutputFormat.JSON,
    OutputFormat.NDJSON,
    OutputFormat.PARQUET,
    OutputFormat.DELTA,
)

# Formats that require ``-o`` because they cannot render to stdout.
PATH_REQUIRED_FORMATS = (OutputFormat.PARQUET, OutputFormat.DELTA)

# Maps file extensions to output formats for inference.
_EXTENSION_FORMATS = {
    ".csv": OutputFormat.CSV,
    ".json": OutputFormat.JSON,
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
    """

    path: Optional[Path]
    format: str
    limit: int = DEFAULT_LIMIT
    overwrite: bool = False


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
) -> OutputSpec:
    """Resolves and validates output options.

    :param output_path: the ``-o`` path, or None for stdout.
    :param format_flag: the ``--format`` value, or None to default/infer.
    :param limit: the stdout table row cap.
    :param overwrite: whether replacing an existing output path is allowed.
    :return: the resolved :class:`OutputSpec`.
    :raises CliError: when the combination of options is invalid.
    """
    path = Path(output_path) if output_path else None

    if format_flag is not None:
        fmt = format_flag
    elif path is not None:
        inferred = infer_format_from_extension(path)
        if inferred is None:
            raise CliError(
                f"Could not infer an output format from '{path}'. "
                "Use --format csv|json|ndjson|parquet|delta.",
                exit_code=EXIT_USAGE,
            )
        fmt = inferred
    else:
        fmt = OutputFormat.TABLE

    if fmt == OutputFormat.TABLE and path is not None:
        raise CliError(
            "The table format cannot be written to a file. Use --format "
            "csv|json|ndjson|parquet|delta with -o, or drop -o for a table.",
            exit_code=EXIT_USAGE,
        )
    if fmt in PATH_REQUIRED_FORMATS and path is None:
        raise CliError(
            f"The {fmt} format requires an output path. Add -o <path>.",
            exit_code=EXIT_USAGE,
        )

    return OutputSpec(path=path, format=fmt, limit=limit, overwrite=overwrite)


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


def render_json(columns: Sequence[str], rows: Sequence[Sequence]) -> str:
    """Renders rows as a JSON array of objects.

    :param columns: the column names.
    :param rows: the row values.
    :return: the JSON text.
    """
    return json.dumps(_records(columns, rows), default=str, indent=2)


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
    :param fmt: one of table, csv, json, or ndjson.
    :return: the rendered text.
    :raises CliError: when the format cannot render to stdout.
    """
    if fmt == OutputFormat.TABLE:
        return render_table(columns, rows)
    if fmt == OutputFormat.CSV:
        return render_csv(columns, rows)
    if fmt == OutputFormat.JSON:
        return render_json(columns, rows)
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
    formats stream the full result. File output is written as a single file for
    CSV/JSON/NDJSON/Parquet and as a Delta table directory for Delta.

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
    count = _write_file(df, spec)
    console.print(f"Wrote {count} rows to {spec.path}.")


def _write_file(df, spec: OutputSpec) -> int:
    """Writes a result DataFrame to a file in the resolved format.

    :param df: the result Spark DataFrame.
    :param spec: the resolved output specification with a non-None path.
    :return: the number of rows written.
    """
    path = spec.path
    if spec.format == OutputFormat.DELTA:
        writer = df.write.format("delta")
        if spec.overwrite:
            writer = writer.mode("overwrite")
        writer.save(str(path))
        return df.count()

    # Single-file formats are materialised on the driver via pandas so the
    # output is a single file rather than a directory of Spark part files. The
    # row count is taken from the materialised frame to avoid a second query
    # execution (which would re-run terminology UDFs against the server).
    pdf = df.toPandas()
    if spec.format == OutputFormat.CSV:
        pdf.to_csv(path, index=False)
    elif spec.format == OutputFormat.JSON:
        pdf.to_json(path, orient="records", indent=2)
    elif spec.format == OutputFormat.NDJSON:
        pdf.to_json(path, orient="records", lines=True)
    elif spec.format == OutputFormat.PARQUET:
        pdf.to_parquet(path, index=False)
    return len(pdf)
