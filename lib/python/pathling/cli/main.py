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

"""The root command group for the Pathling command line interface.

This module defines the global options, resolves configuration, registers every
subcommand, and installs a single central error handler that turns exceptions
(including unwrapped JVM exceptions) into concise messages with appropriate exit
codes. Heavy imports (PySpark, the JVM-backed library) are deferred to command
execution so that ``--help`` and ``--version`` stay fast.

Author: John Grimes.
"""

import sys
from dataclasses import dataclass
from pathlib import Path

import click
from rich.console import Console

from pathling._version import __version__
from pathling.cli import console as console_module
from pathling.cli import convert as convert_module
from pathling.cli import export as export_module
from pathling.cli import fhirpath as fhirpath_module
from pathling.cli import run as run_module
from pathling.cli import terminology as terminology_module
from pathling.cli import view as view_module
from pathling.cli.config import CliConfig, resolve_config
from pathling.cli.errors import EXIT_RUNTIME, CliError, friendly_message
from pathling.cli.render import stderr_console


@dataclass
class CliContext:
    """The object carried on the Click context for every command.

    :param config: the resolved global configuration.
    :param console: the stderr console for progress and error output.
    """

    config: CliConfig
    console: Console


def _console_from(ctx: click.Context) -> Console:
    """Returns the stderr console from the context, or a fresh one.

    :param ctx: the Click context.
    :return: a console for writing error output.
    """
    if isinstance(ctx.obj, CliContext):
        return ctx.obj.console
    return stderr_console()


def _verbose_from(ctx: click.Context) -> bool:
    """Returns whether verbose mode is active for the context.

    :param ctx: the Click context.
    :return: True when verbose output was requested.
    """
    return isinstance(ctx.obj, CliContext) and ctx.obj.config.verbose


class PathlingCli(click.Group):
    """The root group with centralised error handling and exit codes."""

    def invoke(self, ctx: click.Context):
        """Invokes a command, mapping errors to friendly messages and codes.

        :param ctx: the Click context.
        :return: the command's return value on success.
        """
        try:
            return super().invoke(ctx)
        except click.exceptions.Exit:
            # Normal exits from eager options such as --help and --version
            # carry their own exit code; let them propagate untouched.
            raise
        except (KeyboardInterrupt, click.exceptions.Abort):
            _console_from(ctx).print("\nAborted.", style="yellow")
            sys.exit(EXIT_RUNTIME)
        except click.ClickException:
            # Let Click render usage errors (exit code 2) itself.
            raise
        except CliError as exc:
            _console_from(ctx).print(exc.message, style="red")
            sys.exit(exc.exit_code)
        except Exception as exc:  # noqa: BLE001 - the central runtime handler.
            message = friendly_message(exc, verbose=_verbose_from(ctx))
            _console_from(ctx).print(message, style="red")
            sys.exit(EXIT_RUNTIME)


@click.group(cls=PathlingCli, context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__, prog_name="pathling")
@click.option("--tx-server", help="Terminology server URL (config key: tx-server).")
@click.option("--tx-client-id", help="Terminology auth client ID.")
@click.option("--tx-client-secret", help="Terminology auth client secret.")
@click.option("--tx-token-endpoint", help="Terminology auth token endpoint.")
@click.option("--tx-scope", help="Terminology auth scope.")
@click.option(
    "--fhir-version", help="FHIR version (config key: fhir-version; default R4)."
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path),
    help="Path to the TOML config file "
    "(default: $XDG_CONFIG_HOME/pathling/config.toml).",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable Spark/JVM logging and full stack traces on error.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    tx_server,
    tx_client_id,
    tx_client_secret,
    tx_token_endpoint,
    tx_scope,
    fhir_version,
    config_path,
    verbose,
):
    """Pathling: a command line interface for FHIR analytics.

    Run a SQL on FHIR view, evaluate FHIRPath, convert FHIR data between
    formats, bulk export from a FHIR server, run terminology operations
    over a dataset of codes, or script and explore interactively with the
    run and console commands. Configuration may be set with global flags or a
    TOML config file at $XDG_CONFIG_HOME/pathling/config.toml (flags take
    precedence).
    """
    console = stderr_console()
    config = resolve_config(
        tx_server=tx_server,
        tx_client_id=tx_client_id,
        tx_client_secret=tx_client_secret,
        tx_token_endpoint=tx_token_endpoint,
        tx_scope=tx_scope,
        fhir_version=fhir_version,
        verbose=verbose,
        config_path=config_path,
        on_warning=lambda message: console.print(message, style="yellow"),
        on_notice=lambda message: console.print(message, style="dim"),
    )
    ctx.obj = CliContext(config=config, console=console)


# Register the data commands.
cli.add_command(convert_module.convert)
cli.add_command(view_module.view)
cli.add_command(fhirpath_module.fhirpath)
cli.add_command(export_module.export)

# Register the scripting commands.
cli.add_command(run_module.run)
cli.add_command(console_module.console)

# Register the terminology commands.
for _terminology_command in terminology_module.TERMINOLOGY_COMMANDS:
    cli.add_command(_terminology_command)


if __name__ == "__main__":
    cli()
