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

"""The ``pathling run`` command.

Executes user-supplied Python code - from a script file, standard input, or an
inline ``-c`` option - with ``spark`` (the Spark session) and ``pathling`` (the
configured Pathling context) bound in the code's global scope, reproducing
Python interpreter script semantics (``sys.argv``, ``__main__``, ``__file__``,
``sys.path``, traceback fidelity, and ``SystemExit`` propagation).

Author: John Grimes.
"""

import os
import sys
import traceback
from dataclasses import dataclass
from typing import Optional, Tuple

import click

from pathling.cli import session
from pathling.cli.errors import EXIT_RUNTIME

# The context key under which the raw command arguments are recorded.
_RAW_ARGS_KEY = "pathling.run.raw_args"


@dataclass
class CodeSource:
    """A resolved source of program text for execution.

    :param text: the program source code.
    :param filename: the filename to compile under, which appears in
           tracebacks and syntax errors (the script path, ``<stdin>``, or
           ``<string>``).
    :param argv0: the value for ``sys.argv[0]``, following Python interpreter
           conventions (the script path, ``-``, or ``-c``).
    :param path_entry: the entry to prepend to ``sys.path`` (the script's
           directory for files, ``""`` for stdin and inline code).
    :param file_attr: the value for ``__file__`` in the program's globals, or
           None to leave it unset (stdin and inline code).
    """

    text: str
    filename: str
    argv0: str
    path_entry: str
    file_attr: Optional[str]


class RunCommand(click.Command):
    """A command that records its raw argument list before parsing.

    The raw arguments are needed to distinguish ``run script.py -c CODE``
    (a usage error: two code sources) from ``run -c CODE a b`` (inline code
    with trailing arguments), which parse to the same option values.
    """

    def parse_args(self, ctx, args):
        """Stores the raw arguments on the context, then parses as normal.

        :param ctx: the Click context.
        :param args: the raw argument list for this command.
        :return: the remaining arguments after parsing.
        """
        ctx.meta[_RAW_ARGS_KEY] = list(args)
        return super().parse_args(ctx, args)


def _positional_precedes_code_flag(raw_args) -> bool:
    """Determines whether a positional argument appears before ``-c``.

    A positional (a script path or ``-``) before the inline-code flag means
    the user supplied two code sources, which is a usage error. A positional
    after the flag is simply an argument to the inline program.

    :param raw_args: the raw argument list as typed on the command line.
    :return: True when a script positional precedes the ``-c`` flag.
    """
    for token in raw_args:
        if token == "-c" or token == "--code" or token.startswith("--code="):
            return False
        if token == "--":
            return False
        if token == "-" or not token.startswith("-"):
            return True
    return False


def _resolve_source(ctx, script, code, args) -> Tuple[CodeSource, list]:
    """Validates the code-source rules and reads the program text.

    Exactly one source is required: a script path, ``-`` (stdin), or
    ``-c CODE``. All validation happens here, before the Spark session is
    started.

    :param ctx: the Click context.
    :param script: the script positional, or None.
    :param code: the ``-c`` option value, or None.
    :param args: the trailing arguments tuple.
    :return: the resolved :class:`CodeSource` and the program's argument list
             (``sys.argv[1:]``).
    :raises click.UsageError: when the code-source rules are violated or the
            script file cannot be read.
    """
    if code is not None:
        if _positional_precedes_code_flag(ctx.meta.get(_RAW_ARGS_KEY, [])):
            raise click.UsageError(
                "Cannot use both a script and -c; supply exactly one code source."
            )
        # Any positional that Click captured belongs to the program's argv.
        trailing = ([script] if script is not None else []) + list(args)
        return (
            CodeSource(
                text=code,
                filename="<string>",
                argv0="-c",
                path_entry="",
                file_attr=None,
            ),
            trailing,
        )

    if script is None:
        raise click.UsageError(
            "Supply a code source: a script path, '-' for stdin, or -c CODE."
        )

    if script == "-":
        return (
            CodeSource(
                text=sys.stdin.read(),
                filename="<stdin>",
                argv0="-",
                path_entry="",
                file_attr=None,
            ),
            list(args),
        )

    try:
        with open(script, encoding="utf-8") as handle:
            text = handle.read()
    except OSError as exc:
        raise click.UsageError(f"Cannot read script '{script}': {exc}") from exc
    return (
        CodeSource(
            text=text,
            filename=script,
            argv0=script,
            path_entry=os.path.dirname(os.path.abspath(script)),
            file_attr=script,
        ),
        list(args),
    )


def _execute(source: CodeSource, program_args, namespace) -> None:
    """Compiles and executes the program with interpreter semantics.

    ``sys.argv`` and ``sys.path`` are set for the duration of execution and
    restored afterwards. Uncaught exceptions print a standard traceback with
    the CLI's own frames removed and exit 1; ``SystemExit`` propagates
    untouched so its status becomes the process exit code.

    :param source: the resolved code source.
    :param program_args: the program's arguments (``sys.argv[1:]``).
    :param namespace: the extra globals to bind (``spark`` and ``pathling``).
    """
    try:
        code_object = compile(source.text, source.filename, "exec")
    except SyntaxError as exc:
        # Pass no traceback: the interpreter prints only the error excerpt and
        # the SyntaxError line for a syntax error, with no traceback header or
        # frames.
        traceback.print_exception(type(exc), exc, None, file=sys.stderr)
        # Flush explicitly: the stream may be block-buffered (for example the
        # capture stream used by Click's test runner) and the process exits
        # immediately after.
        sys.stderr.flush()
        sys.exit(EXIT_RUNTIME)

    program_globals = {"__name__": "__main__", **namespace}
    if source.file_attr is not None:
        program_globals["__file__"] = source.file_attr

    saved_argv = sys.argv
    saved_path = list(sys.path)
    sys.argv = [source.argv0] + list(program_args)
    sys.path.insert(0, source.path_entry)
    try:
        exec(code_object, program_globals)
    except Exception as exc:
        # Drop the CLI's own frame (this function's exec call) so the
        # traceback starts at the user's code, exactly as the interpreter
        # would print it.
        tb = exc.__traceback__.tb_next if exc.__traceback__ else None
        traceback.print_exception(type(exc), exc, tb, file=sys.stderr)
        # See the flush comment in the syntax-error path above.
        sys.stderr.flush()
        sys.exit(EXIT_RUNTIME)
    finally:
        sys.argv = saved_argv
        sys.path[:] = saved_path


@click.command(
    name="run",
    cls=RunCommand,
    context_settings={"ignore_unknown_options": True},
)
@click.argument("script", required=False)
@click.option("-c", "--code", "code", help="Inline Python code to execute.")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def run(ctx, script, code, args):
    """Run Python code with the Pathling environment ready.

    Executes a script file (or '-' for standard input, or inline code via
    -c) with two variables already in scope: spark (the Spark session) and
    pathling (the configured Pathling context). Trailing arguments are
    passed to the code as sys.argv, following Python interpreter
    conventions.

    \b
    See the Pathling Python API reference:
    https://pathling.csiro.au/docs/python/pathling.html

    Example - project a tabular view of patients, then summarise it with
    SQL. Save this as summary.py and run "pathling run summary.py":

    \b
        patients = pathling.read.ndjson("data").view(
            "Patient",
            select=[{"column": [{"path": "gender", "name": "gender"}]}],
        )
        patients.createOrReplaceTempView("patient")
        spark.sql("SELECT gender, count(*) AS count "
                  "FROM patient GROUP BY gender").show()
    """
    source, program_args = _resolve_source(ctx, script, code, args)

    obj = ctx.obj
    pc = session.create_context(obj.config, obj.console)
    namespace = {"spark": pc.spark, "pathling": pc}

    _execute(source, program_args, namespace)
