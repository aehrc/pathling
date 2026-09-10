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

"""Unit tests for exception unwrapping, message mapping, and exit codes.

Author: John Grimes.
"""

from pathling.cli.errors import (
    EXIT_RUNTIME,
    EXIT_SUCCESS,
    EXIT_USAGE,
    CliError,
    _categorise,
    friendly_message,
    is_auth_error,
    unwrap_java_exception,
)


class _FakeJavaException:
    """A stand-in for a Py4J Java exception exposing getMessage()."""

    def __init__(self, message):
        self._message = message

    def getMessage(self):  # noqa: N802 - mirrors the Java method name.
        return self._message


class _FakeTracelessJavaException:
    """A Java exception with no message of its own, whose str() is a stack trace."""

    def __init__(self, stack_trace):
        self._stack_trace = stack_trace

    def getMessage(self):  # noqa: N802 - mirrors the Java method name.
        return None

    def __str__(self):
        return self._stack_trace


class _FakePy4JError(Exception):
    """A stand-in for py4j.protocol.Py4JJavaError carrying a java_exception."""

    def __init__(self, str_value, java_exception):
        super().__init__(str_value)
        self._str_value = str_value
        self.java_exception = java_exception

    def __str__(self):
        return self._str_value


# ========== Exit codes ==========


def test_exit_codes():
    """The exit code constants follow the contract (0/1/2)."""
    assert EXIT_SUCCESS == 0
    assert EXIT_RUNTIME == 1
    assert EXIT_USAGE == 2


def test_cli_error_default_exit_code():
    """A CliError defaults to the runtime failure exit code."""
    assert CliError("boom").exit_code == EXIT_RUNTIME
    assert CliError("bad usage", exit_code=EXIT_USAGE).exit_code == EXIT_USAGE


# ========== Java exception unwrapping ==========


def test_unwrap_uses_java_get_message():
    """A wrapped Java exception is unwrapped to its getMessage() value."""
    stack_trace = (
        "py4j.protocol.Py4JJavaError: An error occurred\n"
        "\tat au.csiro.pathling.Foo.bar(Foo.java:42)\n"
        "\tat ...\n"
    )
    exc = _FakePy4JError(stack_trace, _FakeJavaException("The real cause"))

    assert unwrap_java_exception(exc) == "The real cause"


def test_unwrap_strips_java_class_prefix():
    """A leading Java exception class name is stripped from the message."""
    exc = _FakeJavaException("java.lang.IllegalArgumentException: bad input here")
    # Wrap so the function sees it via java_exception.
    wrapper = _FakePy4JError("ignored", exc)

    assert unwrap_java_exception(wrapper) == "bad input here"


def test_unwrap_plain_exception():
    """A plain Python exception is unwrapped to its message."""
    assert unwrap_java_exception(ValueError("plain message")) == "plain message"


def test_unwrap_keeps_a_multi_line_java_message_whole():
    """A library failure that deliberately spans several lines is kept intact.

    The import failure that refuses an ambiguous release lists the language
    reference sets the operator has to choose between, one per line. Reducing it
    to its first line would leave the message telling them to choose without
    saying what from.
    """
    message = (
        "The release holds 2 language reference sets and none of them is a "
        "clear default. Name one with the defaultDialect import option:\n"
        "  900000000000508004  GB English\n"
        "  900000000000509007  US English"
    )
    wrapper = _FakePy4JError("ignored", _FakeJavaException(message))

    assert unwrap_java_exception(wrapper) == message


def test_unwrap_still_drops_stack_frames_when_there_is_no_message():
    """Without a getMessage() value, only the leading line of the trace is kept."""
    stack_trace = (
        "java.lang.IllegalStateException: something broke\n"
        "\tat au.csiro.pathling.Foo.bar(Foo.java:42)\n"
        "\tat au.csiro.pathling.Foo.baz(Foo.java:43)\n"
    )
    wrapper = _FakePy4JError("ignored", _FakeTracelessJavaException(stack_trace))

    assert unwrap_java_exception(wrapper) == "something broke"


def test_verbose_hint_does_not_run_onto_a_multi_line_message():
    """The verbose hint goes on its own line after a multi-line message."""
    message = "Something went wrong:\n  a detail the reader needs"
    wrapper = _FakePy4JError("ignored", _FakeJavaException(message))

    formatted = friendly_message(wrapper, verbose=False)

    assert formatted.startswith(message)
    assert formatted.endswith("\nRe-run with --verbose for the full stack trace.")


# ========== Friendly message mapping ==========


def test_connection_error_names_server():
    """A connection error names the server URL and suggests a check."""
    exc = RuntimeError("Connection refused")

    message = friendly_message(exc, server_url="https://tx.example/fhir")

    assert "https://tx.example/fhir" in message
    assert "Connection refused" in message


def test_fhirpath_error_is_categorised():
    """A FHIRPath parse error is mapped to expression guidance."""
    exc = RuntimeError("FHIRPath parse error at position 3")

    message = friendly_message(exc)

    assert "expression" in message.lower()


def test_unknown_error_hints_verbose():
    """An unrecognised error hints at --verbose when not verbose."""
    message = friendly_message(RuntimeError("something odd"), verbose=False)

    assert "--verbose" in message


def test_verbose_includes_stack_trace():
    """Verbose mode appends the full traceback, non-verbose does not."""
    try:
        raise ValueError("kaboom")
    except ValueError as exc:
        verbose = friendly_message(exc, verbose=True)
        quiet = friendly_message(exc, verbose=False)

    assert "Traceback" in verbose
    assert "Traceback" not in quiet


# ========== Authentication error classification (US1) ==========


def test_is_auth_error_true_for_auth_messages():
    """Genuine authentication failures are classified as auth errors."""
    auth_messages = [
        "HTTP 401 Unauthorized",
        "invalid_client",
        # The real SMART backend-services setup failure surfaces this message.
        "Failed to retrieve SMART configuration",
        "Authentication failed: bad credential",
    ]
    for message in auth_messages:
        assert is_auth_error(RuntimeError(message)), message


def test_is_auth_error_false_for_non_auth_messages():
    """Connection, timeout, and server-error failures are not auth errors."""
    non_auth_messages = [
        "Connection refused",
        "Read timed out",
        "HTTP 500 Internal Server Error",
        "Server returned status 503 Service Unavailable",
    ]
    for message in non_auth_messages:
        assert not is_auth_error(RuntimeError(message)), message


# ========== Connection message and FHIRPath categorisation (US4) ==========


def test_connection_error_with_server_url_uses_reach_phrasing():
    """A connection error with a server URL produces the 'could not reach'
    message naming the configured server (FR-011)."""
    exc = RuntimeError("Connection refused")

    message = friendly_message(exc, server_url="https://tx.example/fhir")

    assert "Could not reach the server at https://tx.example/fhir" in message


def test_bare_parse_error_not_categorised_as_fhirpath():
    """A generic 'parse error' that is not a FHIRPath failure is not labelled as
    a FHIRPath error (FR-012)."""
    assert _categorise("JSON parse error at line 2") != "fhirpath"


def test_genuine_fhirpath_error_still_categorised():
    """A message naming FHIRPath is still categorised as a FHIRPath error."""
    assert _categorise("FHIRPath parse error at position 3") == "fhirpath"


# ========== Local-mode friendly messages (US1, T005) ==========


def test_local_mode_message_names_store_and_suggests_import():
    """A local-mode failure names the store path and suggests the import
    commands (FR-011)."""
    exc = RuntimeError("no such table")

    message = friendly_message(exc, store_path="/data/tx-store")

    assert "/data/tx-store" in message
    assert "import-snomed" in message
    assert "import-fhir-terminology" in message


def test_local_mode_message_never_names_a_server():
    """A connection-style failure in local mode never names a terminology server
    URL, even when one is also supplied (FR-011)."""
    exc = RuntimeError("Connection refused")

    message = friendly_message(
        exc, server_url="https://tx.example/fhir", store_path="/data/tx-store"
    )

    assert "https://tx.example/fhir" not in message
    assert "/data/tx-store" in message


def test_local_mode_message_includes_root_cause():
    """The local-mode message still surfaces the underlying failure cause."""
    message = friendly_message(RuntimeError("store is empty"), store_path="/s")

    assert "store is empty" in message


def test_local_mode_message_does_not_repeat_store_path():
    """When the root cause already names the store path, the friendly message
    does not state it a second time."""
    # The library failure already names the store path in quotes.
    exc = RuntimeError(
        "No readable terminology store at '/data/tx-store'. The path may not "
        "exist or may not contain an imported store."
    )

    message = friendly_message(exc, store_path="/data/tx-store")

    assert message.count("/data/tx-store") == 1
    assert "import-snomed" in message
