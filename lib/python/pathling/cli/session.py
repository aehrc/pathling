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

"""Lazy Spark session and Pathling context creation for the CLI.

The Spark session is created only when a command actually needs it, behind a
status spinner on stderr. Spark and JVM logging is suppressed by default by
pointing the driver JVM at a generated log4j2 configuration before launch, and
lowering the log level once the context exists; ``--verbose`` leaves logging at
its defaults.

Author: John Grimes.
"""

import os
import sys
import tempfile
from typing import Optional

from rich.console import Console

from pathling.cli.config import CliConfig
from pathling.cli.render import progress_status, stderr_console

# A log4j2 configuration that silences Spark and JVM logging. It is written to a
# temporary file and supplied to the driver JVM before launch so that startup
# noise, and task-failure stack traces, are suppressed (the CLI surfaces its own
# concise error message instead). The level is raised again by ``--verbose``.
_QUIET_LOG4J2 = """\
rootLogger.level = off
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %p %c{1}: %m%n
rootLogger.appenderRef.console.ref = console
"""


def _write_quiet_log4j2() -> str:
    """Writes the quiet log4j2 configuration to a temporary file.

    :return: the path to the written configuration file.
    """
    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".properties", prefix="pathling-cli-log4j2-", delete=False
    )
    handle.write(_QUIET_LOG4J2)
    handle.close()
    return handle.name


def _build_quiet_spark(config: CliConfig):
    """Builds a Spark session with logging suppressed before JVM launch.

    The Pathling and Delta package configuration is shared with
    :func:`pathling.context._build_spark_session`; this function only adds the
    CLI-specific quiet-logging options on top.

    :param config: the resolved CLI configuration.
    :return: a configured :class:`SparkSession`.
    """
    from pathling.context import _build_spark_session

    # Pin Spark's Python workers to the interpreter running the CLI. Without
    # this, Spark launches workers using whatever ``python3`` is first on the
    # PATH, which may be a different minor version from the driver and causes a
    # PYTHON_VERSION_MISMATCH error on any operation that uses Python workers.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    extra_configs = {
        # Enable Arrow-based columnar transfer so that any ``toPandas`` call in
        # the interactive console collects via Arrow record batches rather than
        # pickling rows one at a time. File output no longer collects to the
        # driver - Spark writes it directly - so this setting now serves only
        # the interactive console. Set before the user overlay below so an
        # explicit --spark-conf value still wins.
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    if not config.verbose:
        log4j2_path = _write_quiet_log4j2()
        extra_configs["spark.driver.extraJavaOptions"] = (
            f"-Dlog4j2.configurationFile=file:{log4j2_path}"
        )
        extra_configs["spark.ui.showConsoleProgress"] = "false"
    # Overlay the user-supplied Spark configuration last so that a user value
    # wins over the CLI's quiet-logging options for the same key (FR-012). A
    # user-set spark.driver.extraJavaOptions therefore replaces the quiet
    # log4j2 option, leaving Spark logging at its defaults.
    extra_configs.update(config.spark_conf)
    return _build_spark_session(extra_configs)


def _create_pathling_context(config: CliConfig):
    """Builds the Spark session and Pathling context from the configuration.

    :param config: the resolved CLI configuration.
    :return: a configured :class:`PathlingContext`.
    """
    from pathling import PathlingContext

    spark = _build_quiet_spark(config)
    if not config.verbose:
        # In local mode the driver JVM is launched before builder-level Java
        # options apply, so log suppression relies on lowering the level here.
        # OFF (rather than ERROR) also hides task-failure stack traces, which
        # the CLI surfaces as its own concise message.
        spark.sparkContext.setLogLevel("OFF")

    auth = config.tx_auth
    return PathlingContext.create(
        spark,
        terminology_server_url=config.tx_server,
        enable_auth=bool(auth and auth.enabled),
        token_endpoint=auth.token_endpoint if auth else None,
        client_id=auth.client_id if auth else None,
        client_secret=auth.client_secret if auth else None,
        scope=auth.scope if auth else None,
    )


def create_context(config: CliConfig, console: Optional[Console] = None):
    """Creates a :class:`PathlingContext` configured from the CLI settings.

    In the default (non-verbose) mode the JVM launcher's startup banner and Ivy
    dependency-resolution report - which Spark prints to file descriptor 2
    before any log configuration can take effect in local mode - are swallowed
    by redirecting that descriptor for the duration of session creation, while
    the status spinner is routed to a preserved copy of the real stderr so that
    progress remains visible.

    :param config: the resolved CLI configuration.
    :param console: the stderr console for the status spinner; created when
           None.
    :return: a configured :class:`PathlingContext`.
    """
    console = console or stderr_console()

    if config.verbose:
        with progress_status(console, "Starting Spark session...", True):
            return _create_pathling_context(config)

    # Preserve the real stderr for the spinner, then point fd 2 at /dev/null so
    # the launcher's banner is discarded during session creation.
    saved_fd = os.dup(2)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    spinner_stream = os.fdopen(os.dup(saved_fd), "w")
    spinner_console = Console(file=spinner_stream, markup=False, highlight=False)
    try:
        os.dup2(devnull_fd, 2)
        with progress_status(spinner_console, "Starting Spark session...", False):
            return _create_pathling_context(config)
    finally:
        os.dup2(saved_fd, 2)
        os.close(saved_fd)
        os.close(devnull_fd)
        spinner_stream.close()
