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

"""The Pathling terminology commands.

Each command reads a tabular dataset (CSV, Parquet, or Delta), builds codings
from a named code column plus either a fixed system URI or a per-row system
column, calls the corresponding library terminology function, appends the
result column(s), and emits the augmented dataset per the shared output
options.

Author: John Grimes.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, Sequence, Tuple

import click

from pathling.cli import session
from pathling.cli.errors import (
    EXIT_USAGE,
    CliError,
    is_connection_error,
    unwrap_java_exception,
)
from pathling.cli.render import (
    default_delimiter_for_path,
    output_options,
    progress_status,
    resolve_output,
    tab_inference_notice,
    write_output,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from pathling import PathlingContext
    from pathling.cli.main import CliContext


def _common_options(func: Callable) -> Callable:
    """Applies the dataset argument, coding options, and output options.

    :param func: the command callback to decorate.
    :return: the decorated callback.
    """
    func = output_options(func)
    options = [
        click.argument("dataset"),
        click.option(
            "--from",
            "from_format",
            type=click.Choice(("csv", "parquet", "delta")),
            help="Input format (default: auto-detected from the dataset path).",
        ),
        click.option(
            "--input-header/--no-input-header",
            "input_header",
            default=True,
            show_default=True,
            help="Treat the first line of a CSV input as a header (default: enabled).",
        ),
        click.option(
            "--code-column", "code_column", required=True, help="Code column name."
        ),
        click.option("--system", "system", help="Fixed code system URI."),
        click.option(
            "--system-column", "system_column", help="Per-row system column name."
        ),
        click.option(
            "--system-version", "system_version", help="Optional code system version."
        ),
        click.option(
            "--result-column", "result_column", help="Override the result column name."
        ),
    ]
    for option in reversed(options):
        func = option(func)
    return func


def _detect_tabular_format(path: Path) -> str:
    """Detects the tabular format of a dataset from its name and layout.

    Detection inspects only the path's suffix and, for directories, the names
    of its immediate entries; it never reads file contents. A file ending in
    ``.csv`` or ``.tsv`` (case-insensitive) resolves to ``csv``; a file ending
    in ``.parquet`` resolves to ``parquet``. A directory containing a
    ``_delta_log`` entry resolves to ``delta``; otherwise a directory containing
    at least one ``.parquet`` file resolves to ``parquet``. Anything else is a
    usage error suggesting ``--from``.

    :param path: the dataset :class:`Path`, which is assumed to exist.
    :return: one of ``csv``, ``parquet``, or ``delta``.
    :raises CliError: with EXIT_USAGE when the format cannot be determined.
    """
    if path.is_dir():
        names = [entry.name for entry in path.iterdir()]
        if "_delta_log" in names:
            return "delta"
        if any(name.lower().endswith(".parquet") for name in names):
            return "parquet"
        contents = ", ".join(sorted(names)) if names else "no entries"
        raise CliError(
            f"Could not determine the format of directory {path} "
            f"(found: {contents}); it has no _delta_log entry and no .parquet "
            "files. Specify it with --from csv|parquet|delta.",
            exit_code=EXIT_USAGE,
        )
    suffix = path.suffix.lower()
    # A .tsv file is treated as CSV; its extension governs the delimiter rather
    # than the format, defaulting it to a tab (see default_delimiter_for_path).
    if suffix in (".csv", ".tsv"):
        return "csv"
    if suffix == ".parquet":
        return "parquet"
    raise CliError(
        f"Could not determine the format of {path} from its suffix "
        f"'{path.suffix}'. Specify it with --from csv|parquet|delta.",
        exit_code=EXIT_USAGE,
    )


def _read_dataset(
    pc: PathlingContext,
    dataset: str,
    from_format: Optional[str],
    delimiter: str = ",",
    input_header: bool = True,
) -> DataFrame:
    """Reads a tabular dataset into a Spark DataFrame using a resolved format.

    The format and the delimiter are both resolved earlier, before the Spark
    session starts (see :func:`_execute`), so this function only dispatches to
    the matching reader. CSV is read with no schema inference; the delimiter and
    header options apply to CSV inputs only, since Parquet and Delta carry their
    own schema. Parquet and Delta accept both single-file and directory inputs.

    :param pc: the Pathling context.
    :param dataset: the path to the dataset file or directory.
    :param from_format: the resolved format, one of ``csv``, ``parquet``, or
           ``delta``.
    :param delimiter: the resolved CSV field separator, a concrete character;
           applied to ``csv`` inputs only.
    :param input_header: whether the first line of a CSV input is a header row;
           applied to ``csv`` inputs only. When False, Spark assigns positional
           column names ``_c0``, ``_c1``, ... referenced via ``--code-column``.
    :return: the loaded DataFrame.
    """
    path = str(dataset)
    if from_format == "csv":
        return pc.spark.read.csv(
            path, header=input_header, inferSchema=False, sep=delimiter
        )
    if from_format == "parquet":
        return pc.spark.read.parquet(path)
    return pc.spark.read.format("delta").load(path)


def _validate_columns(
    df: DataFrame, required: Sequence[Optional[str]], dataset: str
) -> None:
    """Validates that the named columns exist, listing the actual columns.

    :param df: the dataset DataFrame.
    :param required: the column names that must be present.
    :param dataset: the dataset path for the error message.
    :raises CliError: when any required column is missing.
    """
    # This check is inherently late: validating a column name requires the
    # dataset schema, which is only available after the Spark session has read
    # the source. Cheaper inputs (the dataset path and the system options) are
    # already validated before the Spark cold start; the column check is as early
    # as it can be without a bespoke per-format schema pre-read (FR-019).
    missing = [name for name in required if name and name not in df.columns]
    if missing:
        available = ", ".join(df.columns)
        raise CliError(
            f"Column(s) not found in {dataset}: {', '.join(missing)}. "
            f"Available columns: {available}.",
            exit_code=EXIT_USAGE,
        )


def _coding_column(
    pc: PathlingContext,
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    version: Optional[str],
    *,
    code: Optional[str] = None,
) -> Column:
    """Builds a Coding struct column from a code source and a system source.

    The struct's field names and order are derived from the library's own Coding
    builder rather than hand-listed, so the CLI cannot drift from the library
    Coding schema (FR-018). Fields other than the code, system, and version are
    set to null, exactly as the library does for codings built from columns.

    :param pc: the Pathling context, used to resolve the library Coding schema.
    :param code_column: the per-row code column name, used when ``code`` is None.
    :param system: a fixed system URI, or None.
    :param system_column: a per-row system column name, or None.
    :param version: an optional code system version.
    :param code: a fixed literal code applied to every row, or None to read the
           code per row from ``code_column``. This mirrors the fixed-versus-column
           handling of the system part.
    :return: a Spark Column containing a Coding struct.
    :raises CliError: when the system source is missing or ambiguous.
    """
    from pyspark.sql.functions import col, lit, struct

    from pathling.functions import to_coding

    # Resolve the canonical Coding field names and order from the library's own
    # builder; analysing the schema does not trigger a Spark job.
    reference = to_coding(lit(None), "")
    field_names = (
        pc.spark.range(1).select(reference.alias("c")).schema["c"].dataType.fieldNames()
    )

    system_col = lit(system) if system else col(system_column)
    code_col = lit(code) if code is not None else col(code_column)
    overrides = {
        "system": system_col,
        "version": lit(version),
        "code": code_col,
    }
    return struct(*[overrides.get(name, lit(None)).alias(name) for name in field_names])


def _require_exactly_one(
    first: Optional[str],
    first_name: str,
    second: Optional[str],
    second_name: str,
    neither_message: str,
) -> None:
    """Validates that exactly one of a pair of options is provided.

    :param first: the first option's value, falsey when the option is absent.
    :param first_name: the first option's flag name, used in the error message.
    :param second: the second option's value, falsey when the option is absent.
    :param second_name: the second option's flag name, used in the error message.
    :param neither_message: the error message to raise when neither is provided;
           it should name both options so the user knows the valid choices.
    :raises CliError: with EXIT_USAGE when both or neither option is provided.
    """
    if first and second:
        raise CliError(
            f"{first_name} and {second_name} are mutually exclusive. Provide one.",
            exit_code=EXIT_USAGE,
        )
    if not first and not second:
        raise CliError(neither_message, exit_code=EXIT_USAGE)


def _validate_coding_source(
    dataset: str, system: Optional[str], system_column: Optional[str]
) -> None:
    """Validates the dataset path and code system options before Spark starts.

    :param dataset: the dataset path.
    :param system: a fixed system URI, or None.
    :param system_column: a per-row system column name, or None.
    :raises CliError: when the dataset is missing or the system options are
            absent or mutually exclusive.
    """
    if not Path(dataset).exists():
        raise CliError(
            f"Dataset does not exist: {dataset}. Check the path.",
            exit_code=EXIT_USAGE,
        )
    _require_exactly_one(
        system,
        "--system",
        system_column,
        "--system-column",
        "A code system is required. Provide --system <uri> or --system-column <name>.",
    )


def _execute(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    system: Optional[str],
    system_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    build: Callable[[PathlingContext, DataFrame], DataFrame],
) -> None:
    """Runs a terminology operation and emits the augmented dataset.

    :param obj: the CLI context object.
    :param dataset: the dataset path.
    :param from_format: the explicit ``--from`` value, or None to auto-detect.
    :param system: a fixed system URI, or None.
    :param system_column: a per-row system column name, or None.
    :param output_format: the ``--format`` value, or None.
    :param output: the ``-o`` path, or None.
    :param limit: the table row cap.
    :param overwrite: whether to replace an existing output path.
    :param departition: whether file output is departitioned to a single file.
    :param delimiter: the CSV field separator applied to both the input read and
           the output write, or None to resolve each side independently from its
           own path.
    :param header: whether CSV output includes a header row.
    :param input_header: whether the first line of a CSV input is a header row.
    :param build: a callback ``(pc, df) -> result_df`` performing the operation.
    :raises CliError: for validation and unreachable-server failures.
    """
    config = obj.config
    console = obj.console

    # Validate cheap inputs before paying the Spark cold start. Resolving the
    # input format here means an unknown or undeterminable format fails fast,
    # before the multi-second Spark cold start (FR-005). An explicit --from
    # wins; otherwise the format is detected from the path (FR-002/FR-003).
    _validate_coding_source(dataset, system, system_column)
    resolved_format = from_format or _detect_tabular_format(Path(dataset))
    # An omitted --delimiter takes its input-side default from the dataset path,
    # independently of the output side, so a .tsv input is read as tab-separated
    # without the user naming the separator.
    input_delimiter = (
        default_delimiter_for_path(Path(dataset)) if delimiter is None else delimiter
    )
    output_spec = resolve_output(
        output, output_format, limit, overwrite, departition, delimiter, header
    )
    # Announce an inferred tab only where the delimiter is actually consulted:
    # Parquet and Delta inputs carry their own schema and ignore it entirely. This
    # comes after the output options are resolved, so an invalid combination fails
    # rather than announcing a read that never happens - and still before the
    # multi-second Spark cold start, rather than after it.
    if delimiter is None and resolved_format == "csv" and input_delimiter == "\t":
        console.print(tab_inference_notice("Reading", dataset))
    pc = session.create_context(config, console)
    df = _read_dataset(pc, dataset, resolved_format, input_delimiter, input_header)

    try:
        with progress_status(
            console, "Running terminology operation...", config.verbose
        ):
            result_df = build(pc, df)
            write_output(result_df, output_spec, console)
    except CliError:
        raise
    except Exception as exc:  # noqa: BLE001 - enrich connection failures.
        # In local mode no server is contacted, so the server-URL enrichment is
        # skipped and the failure is left to the central handler, which names
        # the store path instead (FR-011).
        if config.tx_store is None and is_connection_error(exc):
            raise CliError(
                f"Could not reach the terminology server at {config.tx_server}: "
                f"{unwrap_java_exception(exc)}. Set the server with --tx-server "
                "<url> or the 'tx-server' config key."
            ) from exc
        raise


# ========== member-of ==========


@click.command(name="member-of")
@_common_options
@click.option("--value-set", "value_set", required=True, help="Value set URI.")
@click.pass_obj
def member_of(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    value_set: str,
) -> None:
    """Test codes for membership of a value set.

    Example:

        pathling member-of codes.csv --code-column code \\
            --system http://snomed.info/sct --value-set <uri>
    """
    name = result_column or "member_of"

    def build(pc: PathlingContext, df: DataFrame) -> DataFrame:
        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(pc, code_column, system, system_column, system_version)
        return df.withColumn(name, udfs.member_of(coding, value_set))

    _execute(
        obj,
        dataset,
        from_format,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        build,
    )


# ========== translate ==========


@click.command(name="translate")
@_common_options
@click.option("--concept-map", "concept_map", required=True, help="Concept map URI.")
@click.option("--reverse", is_flag=True, help="Reverse the translation direction.")
@click.option(
    "--equivalence", "equivalences", multiple=True, help="Equivalence (repeatable)."
)
@click.pass_obj
def translate(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    concept_map: str,
    reverse: bool,
    equivalences: Tuple[str, ...],
) -> None:
    """Translate codes using a concept map.

    Example:

        pathling translate codes.csv --code-column code \\
            --system http://snomed.info/sct --concept-map <uri> --reverse
    """
    base = result_column or "translated"
    system_name = f"{base}_system"
    code_name = f"{base}_code"

    def build(pc: PathlingContext, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col, explode_outer

        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(pc, code_column, system, system_column, system_version)
        translation = udfs.translate(
            coding,
            concept_map,
            reverse=reverse,
            equivalences=list(equivalences) or None,
        )
        with_translation = df.withColumn("_translation", explode_outer(translation))
        return with_translation.select(
            *df.columns,
            col("_translation.system").alias(system_name),
            col("_translation.code").alias(code_name),
        )

    _execute(
        obj,
        dataset,
        from_format,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        build,
    )


# ========== subsumes / subsumed-by ==========


def _second_coding_options(func: Callable) -> Callable:
    """Adds the second coding options used by subsumes and subsumed-by.

    :param func: the command callback to decorate.
    :return: the decorated callback.
    """
    options = [
        click.option(
            "--other-code",
            "other_code",
            help="Fixed target code applied to every row.",
        ),
        click.option(
            "--other-code-column",
            "other_code_column",
            help="Second code column.",
        ),
        click.option("--other-system", "other_system", help="Second fixed system URI."),
        click.option(
            "--other-system-column",
            "other_system_column",
            help="Second per-row system column.",
        ),
    ]
    for option in reversed(options):
        func = option(func)
    return func


def _run_subsumption(
    obj: CliContext,
    operation: str,
    default_name: str,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    other_code: Optional[str],
    other_code_column: Optional[str],
    other_system: Optional[str],
    other_system_column: Optional[str],
) -> None:
    """Shared implementation for subsumes and subsumed-by.

    :param obj: the CLI context object.
    :param operation: the udf attribute name (``subsumes`` or ``subsumed_by``).
    :param default_name: the default result column name.
    :param dataset: the dataset path.
    :param from_format: the explicit ``--from`` value, or None to auto-detect.
    :param code_column: the left code column.
    :param system: the left fixed system URI, or None.
    :param system_column: the left per-row system column, or None.
    :param system_version: the optional code system version.
    :param result_column: the result column override, or None.
    :param output_format: the output format, or None.
    :param output: the output path, or None.
    :param limit: the table row cap.
    :param overwrite: whether to replace an existing output path.
    :param departition: whether file output is departitioned to a single file.
    :param delimiter: the CSV field separator for the input read and output
           write, or None to resolve each side from its own path.
    :param header: whether CSV output includes a header row.
    :param input_header: whether the first line of a CSV input is a header row.
    :param other_code: a fixed target code applied to every row, or None.
    :param other_code_column: the right code column, or None when a fixed target
           code is supplied.
    :param other_system: the right fixed system URI, or None.
    :param other_system_column: the right per-row system column, or None.
    :raises CliError: when the target code or system options are absent or
            mutually exclusive.
    """
    # Validate the target options before paying the Spark cold start.
    _require_exactly_one(
        other_code,
        "--other-code",
        other_code_column,
        "--other-code-column",
        "A target code is required. Provide --other-code <code> or "
        "--other-code-column <name>.",
    )
    _require_exactly_one(
        other_system,
        "--other-system",
        other_system_column,
        "--other-system-column",
        "A target system is required. Provide --other-system <uri> or "
        "--other-system-column <name>.",
    )

    name = result_column or default_name

    def build(pc: PathlingContext, df: DataFrame) -> DataFrame:
        from pathling import udfs

        _validate_columns(
            df,
            [code_column, system_column, other_code_column, other_system_column],
            dataset,
        )
        left = _coding_column(pc, code_column, system, system_column, system_version)
        right = _coding_column(
            pc,
            other_code_column,
            other_system,
            other_system_column,
            system_version,
            code=other_code,
        )
        return df.withColumn(name, getattr(udfs, operation)(left, right))

    _execute(
        obj,
        dataset,
        from_format,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        build,
    )


@click.command(name="subsumes")
@_common_options
@_second_coding_options
@click.pass_obj
def subsumes(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    other_code: Optional[str],
    other_code_column: Optional[str],
    other_system: Optional[str],
    other_system_column: Optional[str],
) -> None:
    """Test subsumption against another code column or a fixed target coding.

    Compare a column of codes against either a second code column or a single
    fixed target coding supplied with ``--other-code``.

    Examples:

        pathling subsumes codes.csv --code-column a --system http://snomed.info/sct \\
            --other-code-column b --other-system http://snomed.info/sct

        pathling subsumes codes.csv --code-column code --system http://snomed.info/sct \\
            --other-code 73211009 --other-system http://snomed.info/sct
    """
    _run_subsumption(
        obj,
        "subsumes",
        "subsumes",
        dataset,
        from_format,
        code_column,
        system,
        system_column,
        system_version,
        result_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        other_code,
        other_code_column,
        other_system,
        other_system_column,
    )


@click.command(name="subsumed-by")
@_common_options
@_second_coding_options
@click.pass_obj
def subsumed_by(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    other_code: Optional[str],
    other_code_column: Optional[str],
    other_system: Optional[str],
    other_system_column: Optional[str],
) -> None:
    """Test reverse subsumption against another code column or a fixed target.

    Compare a column of codes against either a second code column or a single
    fixed target coding supplied with ``--other-code``.

    Examples:

        pathling subsumed-by codes.csv --code-column a --system http://snomed.info/sct \\
            --other-code-column b --other-system http://snomed.info/sct

        pathling subsumed-by codes.csv --code-column code --system http://snomed.info/sct \\
            --other-code 73211009 --other-system http://snomed.info/sct
    """
    _run_subsumption(
        obj,
        "subsumed_by",
        "subsumed_by",
        dataset,
        from_format,
        code_column,
        system,
        system_column,
        system_version,
        result_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        other_code,
        other_code_column,
        other_system,
        other_system_column,
    )


# ========== display ==========


@click.command(name="display")
@_common_options
@click.option("--accept-language", "accept_language", help="Preferred language(s).")
@click.pass_obj
def display(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    accept_language: Optional[str],
) -> None:
    """Look up display names for codes.

    Example:

        pathling display codes.csv --code-column code --system http://loinc.org
    """
    name = result_column or "display"

    def build(pc: PathlingContext, df: DataFrame) -> DataFrame:
        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(pc, code_column, system, system_column, system_version)
        return df.withColumn(name, udfs.display(coding, accept_language))

    _execute(
        obj,
        dataset,
        from_format,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        build,
    )


# ========== property-of ==========


@click.command(name="property-of")
@_common_options
@click.option("--property", "property_code", required=True, help="Property code.")
@click.option(
    "--property-type",
    "property_type",
    default="string",
    show_default=True,
    help="Property type.",
)
@click.option("--accept-language", "accept_language", help="Preferred language(s).")
@click.pass_obj
def property_of(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    property_code: str,
    property_type: str,
    accept_language: Optional[str],
) -> None:
    """Look up properties for codes.

    Example:

        pathling property-of codes.csv --code-column code \\
            --system http://snomed.info/sct --property parent --property-type code
    """
    name = result_column or "property"

    def build(pc: PathlingContext, df: DataFrame) -> DataFrame:
        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(pc, code_column, system, system_column, system_version)
        return df.withColumn(
            name,
            udfs.property_of(coding, property_code, property_type, accept_language),
        )

    _execute(
        obj,
        dataset,
        from_format,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        build,
    )


# ========== designation ==========


@click.command(name="designation")
@_common_options
@click.option("--use", "use", help="Designation use as 'system|code'.")
@click.option("--language", "language", help="Designation language.")
@click.pass_obj
def designation(
    obj: CliContext,
    dataset: str,
    from_format: Optional[str],
    code_column: str,
    system: Optional[str],
    system_column: Optional[str],
    system_version: Optional[str],
    result_column: Optional[str],
    output_format: Optional[str],
    output: Optional[str],
    limit: int,
    overwrite: bool,
    departition: bool,
    delimiter: Optional[str],
    header: bool,
    input_header: bool,
    use: Optional[str],
    language: Optional[str],
) -> None:
    """Look up designations for codes.

    Example:

        pathling designation codes.csv --code-column code \\
            --system http://snomed.info/sct --language en
    """
    name = result_column or "designation"

    def build(pc: PathlingContext, df: DataFrame) -> DataFrame:
        from pathling import udfs
        from pathling.coding import Coding

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(pc, code_column, system, system_column, system_version)
        use_coding = None
        if use:
            if "|" not in use:
                raise CliError(
                    f"Invalid --use '{use}'. Use the form 'system|code'.",
                    exit_code=EXIT_USAGE,
                )
            use_system, use_code = use.split("|", 1)
            use_coding = Coding(use_system, use_code)
        return df.withColumn(name, udfs.designation(coding, use_coding, language))

    _execute(
        obj,
        dataset,
        from_format,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        departition,
        delimiter,
        header,
        input_header,
        build,
    )


# All terminology commands, in display order.
TERMINOLOGY_COMMANDS = (
    member_of,
    translate,
    subsumes,
    subsumed_by,
    display,
    property_of,
    designation,
)
