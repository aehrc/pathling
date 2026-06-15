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

Each command reads a tabular dataset (CSV or Parquet), builds codings from a
named code column plus either a fixed system URI or a per-row system column,
calls the corresponding library terminology function, appends the result
column(s), and emits the augmented dataset per the shared output options.

Author: John Grimes.
"""

from pathlib import Path

import click

from pathling.cli import session
from pathling.cli.errors import (
    EXIT_USAGE,
    CliError,
    is_connection_error,
    unwrap_java_exception,
)
from pathling.cli.render import (
    output_options,
    progress_status,
    resolve_output,
    write_output,
)


def _common_options(func):
    """Applies the dataset argument, coding options, and output options.

    :param func: the command callback to decorate.
    :return: the decorated callback.
    """
    func = output_options(func)
    options = [
        click.argument("dataset"),
        click.option(
            "--code-column", "code_column", required=True, help="Code column name."
        ),
        click.option("--system", "system", help="Fixed code system URI."),
        click.option(
            "--system-column", "system_column", help="Per-row system column name."
        ),
        click.option(
            "--coding-version", "coding_version", help="Optional coding version."
        ),
        click.option(
            "--result-column", "result_column", help="Override the result column name."
        ),
    ]
    for option in reversed(options):
        func = option(func)
    return func


def _read_dataset(pc, dataset):
    """Reads a CSV or Parquet dataset into a Spark DataFrame.

    :param pc: the Pathling context.
    :param dataset: the path to the dataset file.
    :return: the loaded DataFrame.
    :raises CliError: when the path is missing or the type is unsupported.
    """
    path = Path(dataset)
    if not path.exists():
        raise CliError(
            f"Dataset does not exist: {path}. Check the path.", exit_code=EXIT_USAGE
        )
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pc.spark.read.csv(str(path), header=True, inferSchema=False)
    if suffix == ".parquet":
        return pc.spark.read.parquet(str(path))
    raise CliError(
        f"Unsupported dataset type '{suffix}'. Use a .csv or .parquet file.",
        exit_code=EXIT_USAGE,
    )


def _validate_columns(df, required, dataset):
    """Validates that the named columns exist, listing the actual columns.

    :param df: the dataset DataFrame.
    :param required: the column names that must be present.
    :param dataset: the dataset path for the error message.
    :raises CliError: when any required column is missing.
    """
    missing = [name for name in required if name and name not in df.columns]
    if missing:
        available = ", ".join(df.columns)
        raise CliError(
            f"Column(s) not found in {dataset}: {', '.join(missing)}. "
            f"Available columns: {available}.",
            exit_code=EXIT_USAGE,
        )


def _coding_column(code_column, system, system_column, version):
    """Builds a Coding struct column from a code column and a system source.

    :param code_column: the code column name.
    :param system: a fixed system URI, or None.
    :param system_column: a per-row system column name, or None.
    :param version: an optional coding version.
    :return: a Spark Column containing a Coding struct.
    :raises CliError: when the system source is missing or ambiguous.
    """
    from pyspark.sql.functions import col, lit, struct

    system_col = lit(system) if system else col(system_column)
    return struct(
        lit(None).alias("id"),
        system_col.alias("system"),
        lit(version).alias("version"),
        col(code_column).alias("code"),
        lit(None).alias("display"),
        lit(None).alias("userSelected"),
    )


def _validate_coding_source(dataset, system, system_column):
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
    if system and system_column:
        raise CliError(
            "--system and --system-column are mutually exclusive. Provide one.",
            exit_code=EXIT_USAGE,
        )
    if not system and not system_column:
        raise CliError(
            "A code system is required. Provide --system <uri> or "
            "--system-column <name>.",
            exit_code=EXIT_USAGE,
        )


def _execute(
    obj, dataset, system, system_column, output_format, output, limit, overwrite, build
):
    """Runs a terminology operation and emits the augmented dataset.

    :param obj: the CLI context object.
    :param dataset: the dataset path.
    :param system: a fixed system URI, or None.
    :param system_column: a per-row system column name, or None.
    :param output_format: the ``--format`` value, or None.
    :param output: the ``-o`` path, or None.
    :param limit: the table row cap.
    :param overwrite: whether to replace an existing output path.
    :param build: a callback ``(pc, df) -> result_df`` performing the operation.
    :raises CliError: for validation and unreachable-server failures.
    """
    config = obj.config
    console = obj.console

    # Validate cheap inputs before paying the Spark cold start.
    _validate_coding_source(dataset, system, system_column)
    output_spec = resolve_output(output, output_format, limit, overwrite)
    pc = session.create_context(config, console)
    df = _read_dataset(pc, dataset)

    try:
        with progress_status(
            console, "Running terminology operation...", config.verbose
        ):
            result_df = build(pc, df)
            write_output(result_df, output_spec, console)
    except CliError:
        raise
    except Exception as exc:  # noqa: BLE001 - enrich connection failures.
        if is_connection_error(exc):
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
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    value_set,
):
    """Test codes for membership of a value set.

    Example:

        pathling member-of codes.csv --code-column code \\
            --system http://snomed.info/sct --value-set <uri>
    """
    name = result_column or "member_of"

    def build(pc, df):
        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(code_column, system, system_column, coding_version)
        return df.withColumn(name, udfs.member_of(coding, value_set))

    _execute(
        obj,
        dataset,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
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
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    concept_map,
    reverse,
    equivalences,
):
    """Translate codes using a concept map.

    Example:

        pathling translate codes.csv --code-column code \\
            --system http://snomed.info/sct --concept-map <uri> --reverse
    """
    base = result_column or "translated"
    system_name = f"{base}_system"
    code_name = f"{base}_code"

    def build(pc, df):
        from pyspark.sql.functions import col, explode_outer

        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(code_column, system, system_column, coding_version)
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
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        build,
    )


# ========== subsumes / subsumed-by ==========


def _second_coding_options(func):
    """Adds the second coding options used by subsumes and subsumed-by.

    :param func: the command callback to decorate.
    :return: the decorated callback.
    """
    options = [
        click.option(
            "--other-code-column",
            "other_code_column",
            required=True,
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
    obj,
    operation,
    default_name,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    other_code_column,
    other_system,
    other_system_column,
):
    """Shared implementation for subsumes and subsumed-by.

    :param obj: the CLI context object.
    :param operation: the udf attribute name (``subsumes`` or ``subsumed_by``).
    :param default_name: the default result column name.
    :param dataset: the dataset path.
    :param code_column: the left code column.
    :param system: the left fixed system URI, or None.
    :param system_column: the left per-row system column, or None.
    :param coding_version: the optional coding version.
    :param result_column: the result column override, or None.
    :param output_format: the output format, or None.
    :param output: the output path, or None.
    :param limit: the table row cap.
    :param overwrite: whether to replace an existing output path.
    :param other_code_column: the right code column.
    :param other_system: the right fixed system URI, or None.
    :param other_system_column: the right per-row system column, or None.
    """
    name = result_column or default_name

    def build(pc, df):
        from pathling import udfs

        _validate_columns(
            df,
            [code_column, system_column, other_code_column, other_system_column],
            dataset,
        )
        left = _coding_column(code_column, system, system_column, coding_version)
        right = _coding_column(
            other_code_column, other_system, other_system_column, coding_version
        )
        return df.withColumn(name, getattr(udfs, operation)(left, right))

    _execute(
        obj,
        dataset,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        build,
    )


@click.command(name="subsumes")
@_common_options
@_second_coding_options
@click.pass_obj
def subsumes(
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    other_code_column,
    other_system,
    other_system_column,
):
    """Test subsumption between two code columns.

    Example:

        pathling subsumes codes.csv --code-column a --system http://snomed.info/sct \\
            --other-code-column b --other-system http://snomed.info/sct
    """
    _run_subsumption(
        obj,
        "subsumes",
        "subsumes",
        dataset,
        code_column,
        system,
        system_column,
        coding_version,
        result_column,
        output_format,
        output,
        limit,
        overwrite,
        other_code_column,
        other_system,
        other_system_column,
    )


@click.command(name="subsumed-by")
@_common_options
@_second_coding_options
@click.pass_obj
def subsumed_by(
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    other_code_column,
    other_system,
    other_system_column,
):
    """Test reverse subsumption between two code columns.

    Example:

        pathling subsumed-by codes.csv --code-column a --system http://snomed.info/sct \\
            --other-code-column b --other-system http://snomed.info/sct
    """
    _run_subsumption(
        obj,
        "subsumed_by",
        "subsumed_by",
        dataset,
        code_column,
        system,
        system_column,
        coding_version,
        result_column,
        output_format,
        output,
        limit,
        overwrite,
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
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    accept_language,
):
    """Look up display names for codes.

    Example:

        pathling display codes.csv --code-column code --system http://loinc.org
    """
    name = result_column or "display"

    def build(pc, df):
        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(code_column, system, system_column, coding_version)
        return df.withColumn(name, udfs.display(coding, accept_language))

    _execute(
        obj,
        dataset,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
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
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    property_code,
    property_type,
    accept_language,
):
    """Look up properties for codes.

    Example:

        pathling property-of codes.csv --code-column code \\
            --system http://snomed.info/sct --property parent --property-type code
    """
    name = result_column or "property"

    def build(pc, df):
        from pathling import udfs

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(code_column, system, system_column, coding_version)
        return df.withColumn(
            name,
            udfs.property_of(coding, property_code, property_type, accept_language),
        )

    _execute(
        obj,
        dataset,
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
        build,
    )


# ========== designation ==========


@click.command(name="designation")
@_common_options
@click.option("--use", "use", help="Designation use as 'system|code'.")
@click.option("--language", "language", help="Designation language.")
@click.pass_obj
def designation(
    obj,
    dataset,
    code_column,
    system,
    system_column,
    coding_version,
    result_column,
    output_format,
    output,
    limit,
    overwrite,
    use,
    language,
):
    """Look up designations for codes.

    Example:

        pathling designation codes.csv --code-column code \\
            --system http://snomed.info/sct --language en
    """
    name = result_column or "designation"

    def build(pc, df):
        from pathling import udfs
        from pathling.coding import Coding

        _validate_columns(df, [code_column, system_column], dataset)
        coding = _coding_column(code_column, system, system_column, coding_version)
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
        system,
        system_column,
        output_format,
        output,
        limit,
        overwrite,
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
