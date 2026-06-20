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

"""Unit tests for the portable Click runner builder (US3 / FR-010).

Click 8.2 removed the ``mix_stderr`` parameter from ``CliRunner.__init__``. The
extracted ``make_cli_runner`` helper must construct a runner on either Click
version, passing ``mix_stderr=False`` only where the parameter is accepted, so
the suite collects and runs on Python 3.9 (Click 8.1) and 3.10+ (Click 8.2+).

Author: John Grimes.
"""

from click.testing import CliRunner

from tests.cli.conftest import make_cli_runner


class _RunnerWithMixStderr:
    """A stand-in whose constructor accepts ``mix_stderr`` (Click 8.1 shape)."""

    def __init__(self, mix_stderr=True):
        self.mix_stderr = mix_stderr


class _RunnerWithoutMixStderr:
    """A stand-in whose constructor omits ``mix_stderr`` (Click 8.2 shape)."""

    def __init__(self):
        # A distinctive sentinel proves the parameter was never supplied.
        self.mix_stderr = "unset"


def test_default_builds_real_cli_runner():
    """With no override, the helper builds a genuine Click ``CliRunner``."""
    runner = make_cli_runner()

    assert isinstance(runner, CliRunner)


def test_passes_mix_stderr_false_when_supported():
    """When the constructor accepts ``mix_stderr``, it is passed as False."""
    runner = make_cli_runner(_RunnerWithMixStderr)

    assert runner.mix_stderr is False


def test_omits_mix_stderr_when_not_supported():
    """When the constructor lacks ``mix_stderr``, it is constructed without it.

    A constructor that does not accept the parameter would raise ``TypeError``
    if the helper passed it; reaching the sentinel proves it was omitted.
    """
    runner = make_cli_runner(_RunnerWithoutMixStderr)

    assert isinstance(runner, _RunnerWithoutMixStderr)
    assert runner.mix_stderr == "unset"
