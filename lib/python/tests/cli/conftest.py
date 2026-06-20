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

"""Shared fixtures for the command line interface tests.

The session-scoped Spark/``PathlingContext`` fixture (``pathling_ctx``), the
mock server fixture (``mock_server``), and the test data directory fixture
(``test_data_dir``) are inherited from the parent ``tests/conftest.py``. This
module adds a Click ``CliRunner`` and a helper that makes commands use the
shared mock-backed context instead of starting a fresh Spark session.

Author: John Grimes.
"""

import inspect

from click.testing import CliRunner
from pytest import fixture


def make_cli_runner(runner_cls=CliRunner):
    """Builds a Click ``CliRunner``, keeping stdout and stderr separate where
    supported.

    Click 8.2 removed the ``mix_stderr`` parameter from ``CliRunner.__init__``.
    This passes ``mix_stderr=False`` only when the constructor still accepts it,
    so the fixture builds on Click 8.1 (Python 3.9) and 8.2+ (Python 3.10+).

    :param runner_cls: the runner class to construct; overridable so the
           feature-detection branches can be tested without a particular Click
           version installed. Defaults to :class:`click.testing.CliRunner`.
    :return: a constructed runner instance.
    """
    if "mix_stderr" in inspect.signature(runner_cls.__init__).parameters:
        return runner_cls(mix_stderr=False)
    return runner_cls()


@fixture
def runner():
    """Provides a Click ``CliRunner`` with stdout and stderr kept separate.

    :return: a configured :class:`CliRunner`.
    """
    return make_cli_runner()


@fixture
def patched_context(monkeypatch, pathling_ctx):
    """Makes commands reuse the shared mock-backed ``PathlingContext``.

    Commands call ``session.create_context`` to obtain a context; this fixture
    replaces that with a factory returning the session-scoped ``pathling_ctx``,
    which is wired to the JVM mock terminology service, so terminology commands
    run without a live server and without paying a per-test Spark cold start.

    :param monkeypatch: the pytest monkeypatch fixture.
    :param pathling_ctx: the shared Pathling context fixture.
    :return: the shared :class:`PathlingContext`.
    """

    def _factory(config, console=None):
        return pathling_ctx

    monkeypatch.setattr("pathling.cli.session.create_context", _factory)
    return pathling_ctx
