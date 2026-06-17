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

"""The ``pathling console`` command.

Opens an interactive IPython session with ``spark`` (the Spark session) and
``pathling`` (the configured Pathling context) bound in the user namespace,
after a banner identifying the version and the variables in scope. IPython is
imported inside the command body so that ``--help`` stays fast.

Author: John Grimes.
"""

import platform

import click

from pathling._version import __version__
from pathling.cli import session


def build_banner() -> str:
    """Builds the banner shown before the console's first prompt.

    The banner identifies the Pathling and Python versions, lists the
    variables in scope, and explains how to exit.

    :return: the banner text.
    """
    return (
        f"Pathling console (version {__version__}, "
        f"Python {platform.python_version()})\n"
        "Variables in scope: spark (SparkSession), pathling (PathlingContext)\n"
        "Type exit or press Ctrl-D to leave.\n"
    )


@click.command(name="console")
@click.pass_obj
def console(obj):
    """Open an interactive console with the Pathling environment ready.

    Starts an IPython session with two variables in scope: spark (the Spark
    session) and pathling (the configured Pathling context). Exit with
    'exit' or Ctrl-D.

    \b
    See the Pathling Python API reference:
    https://pathling.csiro.au/docs/python/pathling.html

    Examples:

        pathling console

        pathling --tx-server https://tx.example.org/fhir console
    """
    pc = session.create_context(obj.config, obj.console)

    import IPython
    from traitlets.config import Config

    config = Config()
    config.TerminalInteractiveShell.banner1 = build_banner()
    IPython.start_ipython(
        argv=[],
        user_ns={"spark": pc.spark, "pathling": pc},
        config=config,
    )
