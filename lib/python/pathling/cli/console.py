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

Opens an interactive IPython session with ``spark`` (the Spark session), ``pc``
(the configured Pathling context), and the Pathling package's public API
pre-imported into the user namespace, after a banner identifying the version and
the variables in scope. The ``pathling`` name is bound to the package module
itself, so typing ``import pathling`` at the prompt cannot clobber the context.
The terminology display function is bound as ``tx_display`` rather than
``display`` so that IPython's built-in ``display`` remains reachable at the
prompt. IPython is imported inside the command body so that ``--help`` stays
fast.

Author: John Grimes.
"""

from __future__ import annotations

import platform
from typing import TYPE_CHECKING

import click

from pathling._version import __version__
from pathling.cli import session

if TYPE_CHECKING:
    from pathling.cli.main import CliContext


def build_banner() -> str:
    """Builds the banner shown before the console's first prompt.

    The banner identifies the Pathling and Python versions, lists the
    variables in scope - naming the context ``pc``, the name a user copies into
    their next command - notes that the Pathling public functions are
    pre-imported (with the terminology display available as ``tx_display``),
    and explains how to exit.

    :return: the banner text.
    """
    return (
        f"Pathling console (version {__version__}, "
        f"Python {platform.python_version()})\n"
        "Variables in scope: spark (SparkSession), pc (PathlingContext)\n"
        "Pathling public functions are pre-imported (member_of, translate, "
        "to_coding, ...); see https://pathling.csiro.au/docs/python/pathling.html\n"
        "The terminology display function is available as tx_display "
        "(display is IPython's built-in).\n"
        "Type exit or press Ctrl-D to leave.\n"
    )


@click.command(name="console")
@click.pass_obj
def console(obj: CliContext) -> None:
    """Open an interactive console with the Pathling environment ready.

    Starts an IPython session with spark (the Spark session) and pc
    (the configured Pathling context) in scope. The Pathling public functions
    (member_of, translate, to_coding, and so on) are pre-imported, so no
    "from pathling import ..." is needed; the terminology display is available
    as tx_display, leaving IPython's built-in display unchanged. Exit with
    'exit' or Ctrl-D.

    \b
    See the Pathling Python API reference:
    https://pathling.csiro.au/docs/python/pathling.html

    Examples:

        pathling console

        pathling --tx-server https://tx.example.org/fhir console
    """
    pc = session.create_context(obj.config, obj.console)

    # Pre-import the public API surface, but expose the terminology display as
    # tx_display only: IPython installs its own `display` into the interpreter's
    # built-ins, so binding Pathling's display here would silently shadow it.
    user_ns = session.public_namespace()
    user_ns["tx_display"] = user_ns.pop("display")
    user_ns["spark"] = pc.spark
    user_ns["pc"] = pc

    import IPython
    from traitlets.config import Config

    config = Config()
    config.TerminalInteractiveShell.banner1 = build_banner()
    IPython.start_ipython(
        argv=[],
        user_ns=user_ns,
        config=config,
    )
