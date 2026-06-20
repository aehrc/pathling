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

"""Tests for the ``pathling run`` command.

These tests drive the command through Click's ``CliRunner`` against the shared
Spark fixture, asserting Python-interpreter parity: namespace binding, script
semantics (``__name__``, ``__file__``, ``sys.path``), argument passing,
traceback fidelity, and exit codes.

Author: John Grimes.
"""

import sys

from pytest import fixture

from pathling.cli.main import cli


@fixture
def forbidden_context(monkeypatch):
    """Fails the test if any command attempts to start the environment.

    Used by usage-error tests to prove that validation happens before the
    (expensive) session startup.

    :param monkeypatch: the pytest monkeypatch fixture.
    """

    def _fail(config, console=None):
        raise AssertionError("create_context must not be called for usage errors")

    monkeypatch.setattr("pathling.cli.session.create_context", _fail)


def _write_script(tmp_path, body, name="script.py"):
    """Writes a script body to a file under tmp_path and returns its path.

    :param tmp_path: the test's temporary directory.
    :param body: the Python source for the script.
    :param name: the file name for the script.
    :return: the path to the written script as a string.
    """
    path = tmp_path / name
    path.write_text(body)
    return str(path)


# ========== Namespace binding ==========


def test_spark_and_pathling_are_bound(runner, patched_context, tmp_path):
    """The script sees the context's Spark session and the context itself."""
    script = _write_script(
        tmp_path,
        "print(id(spark))\nprint(id(pathling))\n",
    )

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == str(id(patched_context.spark))
    assert lines[1] == str(id(patched_context))


def test_stdout_passes_through_unmodified(runner, patched_context, tmp_path):
    """The script's stdout appears on the CLI's stdout unmodified."""
    script = _write_script(tmp_path, "print('hello from user code')\n")

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 0, result.stderr
    assert "hello from user code\n" in result.stdout


# ========== Interpreter script semantics ==========


def test_main_module_semantics(runner, patched_context, tmp_path):
    """The script runs as __main__ with __file__ set to the script path."""
    script = _write_script(
        tmp_path,
        "print(__name__)\nprint(__file__)\n",
    )

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == "__main__"
    assert lines[1] == script


def test_script_directory_is_on_sys_path(runner, patched_context, tmp_path):
    """sys.path[0] is the script's directory during execution."""
    script = _write_script(tmp_path, "import sys\nprint(sys.path[0])\n")

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[0] == str(tmp_path)


def test_sibling_module_import(runner, patched_context, tmp_path):
    """A script can import a module that sits next to it."""
    (tmp_path / "sibling.py").write_text("VALUE = 'from sibling'\n")
    script = _write_script(tmp_path, "import sibling\nprint(sibling.VALUE)\n")

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 0, result.stderr
    assert "from sibling" in result.stdout


# ========== Error handling and exit codes ==========


def test_uncaught_exception_prints_clean_traceback(runner, patched_context, tmp_path):
    """An uncaught exception prints a standard traceback and exits 1."""
    script = _write_script(
        tmp_path,
        "def boom():\n    raise ValueError('user error')\nboom()\n",
    )

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 1
    assert "Traceback (most recent call last)" in result.stderr
    assert "ValueError: user error" in result.stderr
    # The traceback names the user's script file, not CLI internals.
    assert script in result.stderr
    assert "cli/run.py" not in result.stderr
    assert "click" not in result.stderr


def test_syntax_error_names_script_and_exits_1(runner, patched_context, tmp_path):
    """A syntax error names the script file and exits 1."""
    script = _write_script(tmp_path, "def broken(\n")

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 1
    assert "SyntaxError" in result.stderr
    assert script in result.stderr
    # The interpreter prints no traceback header or frames for a syntax
    # error, so neither may the CLI.
    assert "Traceback (most recent call last)" not in result.stderr
    assert "cli/run.py" not in result.stderr


def test_sys_exit_code_propagates_without_traceback(runner, patched_context, tmp_path):
    """sys.exit(3) exits with code 3 and prints no traceback."""
    script = _write_script(tmp_path, "import sys\nsys.exit(3)\n")

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 3
    assert "Traceback" not in result.stderr
    assert "Traceback" not in result.stdout


def test_sys_exit_message_prints_and_exits_1(runner, patched_context, tmp_path):
    """sys.exit('msg') prints the message and exits 1."""
    script = _write_script(tmp_path, "import sys\nsys.exit('the exit message')\n")

    result = runner.invoke(cli, ["run", script])

    assert result.exit_code == 1
    assert "the exit message" in result.output + result.stderr
    assert "Traceback" not in result.stderr


# ========== Inline code and stdin sources ==========


def test_inline_code_executes_with_namespace(runner, patched_context):
    """-c code executes with both variables bound."""
    result = runner.invoke(cli, ["run", "-c", "print(id(spark)); print(id(pathling))"])

    assert result.exit_code == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == str(id(patched_context.spark))
    assert lines[1] == str(id(patched_context))


def test_inline_code_traceback_names_string(runner, patched_context):
    """An exception in -c code names <string> in the traceback."""
    result = runner.invoke(cli, ["run", "-c", "raise ValueError('inline')"])

    assert result.exit_code == 1
    assert '"<string>"' in result.stderr
    assert "ValueError: inline" in result.stderr


def test_inline_syntax_error_names_string(runner, patched_context):
    """A syntax error in -c code names <string>."""
    result = runner.invoke(cli, ["run", "-c", "def broken("])

    assert result.exit_code == 1
    assert "SyntaxError" in result.stderr
    assert '"<string>"' in result.stderr
    # The interpreter prints no traceback header or frames for a syntax
    # error, so neither may the CLI.
    assert "Traceback (most recent call last)" not in result.stderr
    assert "cli/run.py" not in result.stderr


def test_stdin_code_executes_with_namespace(runner, patched_context):
    """Code piped on stdin via '-' executes with both variables bound."""
    result = runner.invoke(cli, ["run", "-"], input="print(type(pathling).__name__)\n")

    assert result.exit_code == 0, result.stderr
    assert "PathlingContext" in result.stdout


def test_stdin_traceback_names_stdin(runner, patched_context):
    """An exception in stdin code names <stdin> in the traceback."""
    result = runner.invoke(cli, ["run", "-"], input="raise ValueError('piped')\n")

    assert result.exit_code == 1
    assert '"<stdin>"' in result.stderr
    assert "ValueError: piped" in result.stderr


def test_inline_and_stdin_sys_path_zero_is_empty(runner, patched_context):
    """sys.path[0] is '' (the working directory) for -c and stdin code."""
    inline = runner.invoke(cli, ["run", "-c", "import sys; print(repr(sys.path[0]))"])
    piped = runner.invoke(
        cli, ["run", "-"], input="import sys\nprint(repr(sys.path[0]))\n"
    )

    assert inline.exit_code == 0, inline.stderr
    assert inline.stdout.splitlines()[0] == "''"
    assert piped.exit_code == 0, piped.stderr
    assert piped.stdout.splitlines()[0] == "''"


# ========== Usage errors before session start ==========


def test_script_and_inline_code_is_usage_error(runner, forbidden_context, tmp_path):
    """Supplying both a script and -c is a usage error before session start."""
    script = _write_script(tmp_path, "print('never runs')\n")

    result = runner.invoke(cli, ["run", script, "-c", "print(1)"])

    assert result.exit_code == 2
    assert "-c" in result.stderr


def test_no_source_is_usage_error(runner, forbidden_context):
    """Supplying no code source is a usage error before session start."""
    result = runner.invoke(cli, ["run"])

    assert result.exit_code == 2


# ========== Argument passing (interpreter parity) ==========

ARGV_PROGRAM = "import sys\nprint(sys.argv)\n"


def test_script_argv_follows_interpreter_convention(runner, patched_context, tmp_path):
    """For `run script.py a b`, user code sees ['script.py', 'a', 'b']."""
    script = _write_script(tmp_path, ARGV_PROGRAM)

    result = runner.invoke(cli, ["run", script, "a", "b"])

    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[0] == str([script, "a", "b"])


def test_inline_code_argv_follows_interpreter_convention(runner, patched_context):
    """For `run -c CODE a b`, user code sees ['-c', 'a', 'b']."""
    result = runner.invoke(cli, ["run", "-c", ARGV_PROGRAM, "a", "b"])

    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[0] == str(["-c", "a", "b"])


def test_stdin_argv_follows_interpreter_convention(runner, patched_context):
    """For `run - a b`, user code sees ['-', 'a', 'b']."""
    result = runner.invoke(cli, ["run", "-", "a", "b"], input=ARGV_PROGRAM)

    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[0] == str(["-", "a", "b"])


def test_dash_prefixed_arguments_pass_through(runner, patched_context, tmp_path):
    """Dash-prefixed trailing arguments reach the script unparsed."""
    script = _write_script(tmp_path, ARGV_PROGRAM)

    result = runner.invoke(cli, ["run", script, "--input", "a.ndjson", "out/"])

    assert result.exit_code == 0, result.stderr
    assert result.stdout.splitlines()[0] == str([script, "--input", "a.ndjson", "out/"])


def test_sys_argv_is_restored_after_command(runner, patched_context, tmp_path):
    """The process's original sys.argv is restored after the command returns."""
    script = _write_script(tmp_path, "print('ok')\n")
    original_argv = list(sys.argv)

    result = runner.invoke(cli, ["run", script, "a", "b"])

    assert result.exit_code == 0, result.stderr
    assert sys.argv == original_argv


def test_missing_script_is_usage_error_without_session(
    runner, forbidden_context, tmp_path
):
    """A missing script path is a usage error that never starts the session."""
    result = runner.invoke(cli, ["run", str(tmp_path / "missing.py")])

    assert result.exit_code == 2
    assert "missing.py" in result.stderr
