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

"""Tests that every command's --help is documented per the CLI contract.

The CLI contract (contracts/cli.md) requires every command to be documented
with at least one usage example, and the root help to describe the config file.

Author: John Grimes.
"""

import pytest

from pathling.cli.main import cli

COMMAND_NAMES = sorted(cli.commands.keys())


def test_all_commands_registered():
    """The commands from the contracts are all registered."""
    assert COMMAND_NAMES == sorted(
        [
            "convert",
            "view",
            "fhirpath",
            "export",
            "member-of",
            "translate",
            "subsumes",
            "subsumed-by",
            "display",
            "property-of",
            "designation",
            "run",
            "console",
        ]
    )


@pytest.mark.parametrize("command_name", COMMAND_NAMES)
def test_command_help_has_example(runner, command_name):
    """Each command's --help includes at least one usage example."""
    result = runner.invoke(cli, [command_name, "--help"])

    assert result.exit_code == 0
    assert "example" in result.output.lower()
    # The example always invokes the tool by name.
    assert "pathling" in result.output


def test_run_help_mentions_sources_and_arguments(runner):
    """`run --help` documents the code sources and argument passing."""
    result = runner.invoke(cli, ["run", "--help"])

    assert result.exit_code == 0
    assert "SCRIPT" in result.output
    assert "-c" in result.output
    assert "spark" in result.output
    assert "pathling" in result.output
    assert "sys.argv" in result.output


def test_console_help_mentions_variables(runner):
    """`console --help` documents the in-scope variables and how to exit."""
    result = runner.invoke(cli, ["console", "--help"])

    assert result.exit_code == 0
    assert "spark" in result.output
    assert "pathling" in result.output
    assert "exit" in result.output.lower()


@pytest.mark.parametrize(
    ("command_name", "url"),
    [
        ("run", "https://pathling.csiro.au/docs/python/pathling.html"),
        ("console", "https://pathling.csiro.au/docs/python/pathling.html"),
        ("view", "https://sql-on-fhir.org/ig/StructureDefinition-ViewDefinition.html"),
        ("fhirpath", "https://pathling.csiro.au/docs/fhirpath"),
    ],
)
def test_help_links_to_reference_docs(runner, command_name, url):
    """Each command's help points to its relevant reference documentation."""
    result = runner.invoke(cli, [command_name, "--help"])

    assert result.exit_code == 0
    assert url in result.output


def test_root_help_describes_config_file(runner):
    """The root help describes the config file location and the --config flag."""
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "config.toml" in result.output
    assert "--config" in result.output
