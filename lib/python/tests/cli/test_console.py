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

"""Tests for the ``pathling console`` command.

The banner builder is unit tested as a plain function; the IPython embedding is
tested with ``IPython.start_ipython`` monkeypatched, asserting the namespace,
configuration, and eager startup order. Real REPL behaviour is verified by
manual demonstration per the feature quickstart.

Author: John Grimes.
"""

import IPython

from pathling._version import __version__
from pathling.cli.console import build_banner
from pathling.cli.main import cli

# ========== Banner builder ==========


def test_banner_contains_version_variables_and_exit_hint():
    """The banner names the version, both variables, and how to exit."""
    banner = build_banner()

    assert __version__ in banner
    assert "spark" in banner
    assert "pathling" in banner
    assert "exit" in banner.lower()


# ========== IPython embedding ==========


def test_console_starts_ipython_with_namespace(runner, patched_context, monkeypatch):
    """The console passes argv=[], the exact namespace, and the banner."""
    captured = {}

    def fake_start_ipython(argv=None, user_ns=None, config=None, **kwargs):
        captured["argv"] = argv
        captured["user_ns"] = user_ns
        captured["config"] = config

    monkeypatch.setattr(IPython, "start_ipython", fake_start_ipython)

    result = runner.invoke(cli, ["console"])

    # A clean return from IPython exits 0.
    assert result.exit_code == 0, result.stderr
    # IPython must not consume the process's own argv.
    assert captured["argv"] == []
    # The namespace contains exactly spark and pathling, correctly bound.
    assert captured["user_ns"] == {
        "spark": patched_context.spark,
        "pathling": patched_context,
    }
    # The configuration carries the banner.
    assert captured["config"].TerminalInteractiveShell.banner1 == build_banner()


def test_console_creates_context_before_starting_ipython(
    runner, pathling_ctx, monkeypatch
):
    """The environment is created eagerly, before the REPL starts."""
    calls = []

    def factory(config, console=None):
        calls.append("create_context")
        return pathling_ctx

    def fake_start_ipython(argv=None, user_ns=None, config=None, **kwargs):
        calls.append("start_ipython")

    monkeypatch.setattr("pathling.cli.session.create_context", factory)
    monkeypatch.setattr(IPython, "start_ipython", fake_start_ipython)

    result = runner.invoke(cli, ["console"])

    assert result.exit_code == 0, result.stderr
    assert calls == ["create_context", "start_ipython"]
