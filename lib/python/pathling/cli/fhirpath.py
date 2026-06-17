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

"""The ``pathling fhirpath`` command.

Evaluates a FHIRPath expression against either a whole data source (one row of
``id`` and ``result`` per resource) or a single FHIR resource JSON file (one
``type`` and ``result`` row per result item). Supports context expressions,
named variables, and a FHIR search ``--filter`` in data source mode.

Author: John Grimes.
"""

import click

from pathling.cli import session
from pathling.cli.errors import EXIT_USAGE, CliError
from pathling.cli.io import (
    FROM_CHOICES,
    SourceFormat,
    read_single_resource,
    read_source,
    resolve_source,
)
from pathling.cli.render import (
    output_options,
    progress_status,
    resolve_output,
    write_output,
)


def _parse_variables(var_specs) -> dict:
    """Parses repeated ``name=value`` variable specifications.

    :param var_specs: the raw ``--var`` values.
    :return: a dict of variable names to string values.
    :raises CliError: when a specification is not in ``name=value`` form.
    """
    variables = {}
    for spec in var_specs:
        if "=" not in spec:
            raise CliError(
                f"Invalid --var '{spec}'. Use --var name=value.",
                exit_code=EXIT_USAGE,
            )
        name, value = spec.split("=", 1)
        variables[name] = value
    return variables


@click.command(name="fhirpath")
@click.argument("source")
@click.option(
    "--from",
    "from_format",
    type=click.Choice(FROM_CHOICES),
    help="Input format (auto-detected when omitted).",
)
@click.option(
    "-e", "--expression", "expression", required=True, help="FHIRPath expression."
)
@click.option(
    "-t", "--type", "resource_type", help="Subject resource type (data source mode)."
)
@click.option("--context", "context_expression", help="Optional context expression.")
@click.option(
    "--var", "variables", multiple=True, help="Named variable, name=value (repeatable)."
)
@click.option(
    "--filter", "filter_expr", help="FHIR search expression (data source mode)."
)
@output_options
@click.pass_obj
def fhirpath(
    obj,
    source,
    from_format,
    expression,
    resource_type,
    context_expression,
    variables,
    filter_expr,
    output_format,
    output,
    limit,
    overwrite,
    departition,
):
    """Evaluate a FHIRPath expression against FHIR data.

    Examples:

        pathling fhirpath data/ -t Patient -e 'name.family'

        pathling fhirpath patient.json -e 'name.given.first()'
    """
    config = obj.config
    console = obj.console

    spec = resolve_source(source, from_format, allow_resource=True)
    output_spec = resolve_output(output, output_format, limit, overwrite, departition)
    parsed_variables = _parse_variables(variables)

    # --context and --var only apply to single-resource evaluation; reject them
    # in data source mode rather than silently ignoring them (and before any
    # Spark session is started).
    if spec.format != SourceFormat.RESOURCE and (
        context_expression or parsed_variables
    ):
        raise CliError(
            "--context and --var are only supported in single-resource mode "
            "(when SOURCE is a single FHIR resource JSON file).",
            exit_code=EXIT_USAGE,
        )

    pc = session.create_context(config, console)

    if spec.format == SourceFormat.RESOURCE:
        _run_single_resource(
            pc,
            spec,
            expression,
            resource_type,
            context_expression,
            parsed_variables,
            output_spec,
            console,
            config.verbose,
        )
    else:
        _run_data_source(
            pc,
            spec,
            expression,
            resource_type,
            filter_expr,
            output_spec,
            console,
            config.verbose,
        )


def _run_data_source(
    pc, spec, expression, resource_type, filter_expr, output_spec, console, verbose
) -> None:
    """Evaluates an expression over every resource in a data source.

    :param pc: the Pathling context.
    :param spec: the resolved data source spec.
    :param expression: the FHIRPath expression.
    :param resource_type: the subject resource type; required here.
    :param filter_expr: an optional FHIR search filter, or None.
    :param output_spec: the resolved output spec.
    :param console: the stderr console.
    :param verbose: whether verbose mode is active.
    :raises CliError: when the resource type is missing.
    """
    if not resource_type:
        raise CliError(
            "Data source mode requires the subject resource type. "
            "Add -t/--type, e.g. -t Patient.",
            exit_code=EXIT_USAGE,
        )

    data_source = read_source(pc, spec)
    resources = data_source.read(resource_type)
    if filter_expr:
        resources = resources.filter(pc.search_to_column(resource_type, filter_expr))

    with progress_status(console, "Evaluating FHIRPath...", verbose):
        result_column = pc.fhirpath_to_column(resource_type, expression)
        result = resources.select(
            resources["id"].alias("id"), result_column.cast("string").alias("result")
        )
        write_output(result, output_spec, console)


def _run_single_resource(
    pc,
    spec,
    expression,
    resource_type,
    context_expression,
    variables,
    output_spec,
    console,
    verbose,
) -> None:
    """Evaluates an expression against a single FHIR resource file.

    :param pc: the Pathling context.
    :param spec: the resolved resource spec.
    :param expression: the FHIRPath expression.
    :param resource_type: an optional resource type override, or None.
    :param context_expression: an optional context expression, or None.
    :param variables: the parsed named variables.
    :param output_spec: the resolved output spec.
    :param console: the stderr console.
    :param verbose: whether verbose mode is active.
    """
    detected_type, resource_json = read_single_resource(spec.path)
    effective_type = resource_type or detected_type

    with progress_status(console, "Evaluating FHIRPath...", verbose):
        result = pc.evaluate_fhirpath(
            effective_type,
            resource_json,
            expression,
            context_expression=context_expression,
            variables=variables or None,
        )
        from pyspark.sql.types import StringType, StructField, StructType

        rows = [
            (item["type"], None if item["value"] is None else str(item["value"]))
            for item in result["results"]
        ]
        # An explicit schema is required so that an empty result still has the
        # expected columns rather than failing schema inference. The payload
        # column is named "result" to match data source mode.
        schema = StructType(
            [
                StructField("type", StringType(), True),
                StructField("result", StringType(), True),
            ]
        )
        result_df = pc.spark.createDataFrame(rows, schema)
        write_output(result_df, output_spec, console)
