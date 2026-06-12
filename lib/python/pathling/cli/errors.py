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

"""Error handling for the Pathling command line interface.

JVM exceptions raised through Py4J carry verbose Java stack traces that are
unhelpful at the command line. This module unwraps them to their root message
and maps recognised categories onto concise, actionable guidance. Stack traces
are shown only when the user passes ``--verbose``.

Author: John Grimes.
"""

import re
import traceback
from typing import Optional

# Exit code for a successful invocation.
EXIT_SUCCESS = 0

# Exit code for a runtime failure (a command failed while executing).
EXIT_RUNTIME = 1

# Exit code for a usage error (the command was invoked incorrectly).
EXIT_USAGE = 2


class CliError(Exception):
    """An error with a message that is safe to show the user directly.

    :param message: the human-readable error message.
    :param exit_code: the process exit code to use; defaults to a runtime
           failure.
    """

    def __init__(self, message: str, exit_code: int = EXIT_RUNTIME) -> None:
        super().__init__(message)
        self.message = message
        self.exit_code = exit_code


def unwrap_java_exception(exc: BaseException) -> str:
    """Extracts the most useful single-line message from an exception.

    Py4J wraps Java exceptions, whose ``str`` representation includes the full
    stack trace. This returns the leading message line, stripping the Java
    exception class prefix where present.

    :param exc: the exception to unwrap.
    :return: a concise message describing the underlying problem.
    """
    java_exception = getattr(exc, "java_exception", None)
    if java_exception is not None:
        get_message = getattr(java_exception, "getMessage", None)
        message = get_message() if callable(get_message) else None
        if not message:
            message = str(java_exception)
    else:
        message = str(exc)

    # Take only the first non-empty line; Java traces span many lines.
    first_line = next((line for line in message.splitlines() if line.strip()), message)
    # Strip a leading fully-qualified Java exception class name, e.g.
    # "java.lang.IllegalArgumentException: actual message".
    match = re.match(r"^(?:[\w.$]+Exception|[\w.$]+Error):\s*(.*)$", first_line)
    if match and match.group(1):
        return match.group(1).strip()
    return first_line.strip()


def _categorise(root_message: str) -> Optional[str]:
    """Classifies a root message into a known error category.

    :param root_message: the unwrapped root message.
    :return: a category key, or None when the message is not recognised.
    """
    lowered = root_message.lower()
    if "connection refused" in lowered or "failed to connect" in lowered:
        return "connection"
    if "unknownhost" in lowered or "unknown host" in lowered:
        return "connection"
    if "fhirpath" in lowered or "parse error" in lowered:
        return "fhirpath"
    return None


def is_connection_error(exc: BaseException) -> bool:
    """Determines whether an exception represents a server connection failure.

    :param exc: the exception to classify.
    :return: True when the unwrapped message looks like a connection failure.
    """
    return _categorise(unwrap_java_exception(exc)) == "connection"


def friendly_message(
    exc: BaseException,
    verbose: bool = False,
    server_url: Optional[str] = None,
) -> str:
    """Builds a friendly, actionable message for an unexpected exception.

    :param exc: the exception to describe.
    :param verbose: when True, append the full traceback.
    :param server_url: a server URL to name in connection errors, or None.
    :return: the message to present to the user.
    """
    root = unwrap_java_exception(exc)
    category = _categorise(root)

    if category == "connection" and server_url:
        message = (
            f"Could not reach the server at {server_url}: {root}. "
            "Check the URL and your network connection."
        )
    elif category == "fhirpath":
        message = (
            f"The FHIRPath expression could not be evaluated: {root}. "
            "Check the expression syntax."
        )
    else:
        message = root or f"{type(exc).__name__}"
        if not verbose:
            message = f"{message} Re-run with --verbose for the full stack trace."

    if verbose:
        # Use the three-argument form for Python 3.9 compatibility.
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        message = message + "\n\n" + trace
    return message
