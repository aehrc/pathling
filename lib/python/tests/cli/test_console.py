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

import pathling
from pathling._version import __version__
from pathling.cli.console import build_banner
from pathling.cli.main import cli

# ========== Banner builder ==========


def test_banner_contains_version_variables_and_exit_hint():
    """The banner names the version, both variables, and how to exit."""
    banner = build_banner()

    assert __version__ in banner
    assert "spark" in banner
    assert "pc" in banner
    assert "exit" in banner.lower()


def test_banner_names_pc_as_the_context_not_pathling():
    """The banner names pc as the context and does not call pathling one (FR-015).

    The name in scope is what a user copies into their next command, so the
    banner must not point them at a name that holds the module.
    """
    banner = build_banner()

    assert "pc (PathlingContext)" in banner
    assert "pathling (PathlingContext)" not in banner


def test_banner_mentions_preimported_functions_and_tx_display():
    """The banner announces the pre-imported functions and names tx_display
    (FR-008)."""
    banner = build_banner()

    # A couple of representative pre-imported names and the tx_display note.
    assert "member_of" in banner
    assert "tx_display" in banner


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
    user_ns = captured["user_ns"]
    # spark and pc remain correctly bound.
    assert user_ns["spark"] is patched_context.spark
    assert user_ns["pc"] is patched_context
    # The namespace is the public API minus display, plus spark, pc, the pathling
    # module, and tx_display (INV-2), derived from __all__ rather than a
    # hard-coded list.
    expected = (set(pathling.__all__) - {"display"}) | {
        "spark",
        "pc",
        "pathling",
        "tx_display",
    }
    assert set(user_ns) == expected
    # The configuration carries the banner.
    assert captured["config"].TerminalInteractiveShell.banner1 == build_banner()


def test_console_binds_the_module_not_the_context_to_pathling(
    runner, patched_context, monkeypatch
):
    """``pathling`` is the module, so typing ``import pathling`` leaves pc intact.

    The context is reachable only as ``pc`` (FR-012, FR-013, FR-014).
    """
    captured = {}

    def fake_start_ipython(argv=None, user_ns=None, config=None, **kwargs):
        captured["user_ns"] = user_ns

    monkeypatch.setattr(IPython, "start_ipython", fake_start_ipython)

    result = runner.invoke(cli, ["console"])

    assert result.exit_code == 0, result.stderr
    user_ns = captured["user_ns"]
    assert user_ns["pathling"] is pathling
    assert user_ns["pathling"] is not patched_context
    # Rebinding the name to the module - what `import pathling` does - leaves the
    # context reachable, which is the whole point of the rename.
    user_ns["pathling"] = pathling
    assert user_ns["pc"] is patched_context


def test_console_namespace_display_split(runner, patched_context, monkeypatch):
    """display is absent so IPython's built-in wins; tx_display is Pathling's
    terminology display (INV-4, FR-005)."""
    captured = {}

    def fake_start_ipython(argv=None, user_ns=None, config=None, **kwargs):
        captured["user_ns"] = user_ns

    monkeypatch.setattr(IPython, "start_ipython", fake_start_ipython)

    result = runner.invoke(cli, ["console"])

    assert result.exit_code == 0, result.stderr
    user_ns = captured["user_ns"]
    # Pathling's display is not bound, leaving IPython's built-in reachable.
    assert "display" not in user_ns
    # The terminology display is available under tx_display.
    assert user_ns["tx_display"] is pathling.display


def test_console_namespace_binds_public_functions(runner, patched_context, monkeypatch):
    """Other public names are bound under their own names, as in run (FR-002)."""
    captured = {}

    def fake_start_ipython(argv=None, user_ns=None, config=None, **kwargs):
        captured["user_ns"] = user_ns

    monkeypatch.setattr(IPython, "start_ipython", fake_start_ipython)

    result = runner.invoke(cli, ["console"])

    assert result.exit_code == 0, result.stderr
    user_ns = captured["user_ns"]
    assert user_ns["member_of"] is pathling.member_of
    assert user_ns["to_coding"] is pathling.to_coding
    assert user_ns["Coding"] is pathling.Coding


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
