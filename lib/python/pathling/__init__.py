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

"""Python API for Pathling.

Public names are exposed lazily (PEP 562) so that importing the ``pathling``
package itself does not pull in PySpark and the JVM-backed submodules. The
heavy imports happen only when a public name is first accessed, which keeps the
command line interface's ``--help`` and ``--version`` paths fast.

Author: John Grimes.
"""

# The TYPE_CHECKING imports below exist only so that type checkers and IDEs can
# resolve the lazily-exported public names; they are intentionally unused at
# runtime, so unused-import checks are disabled for this re-export shim.
# ruff: noqa: F401

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .coding import Coding
    from .context import PathlingContext, StorageType
    from .core import Expression, VariableExpression
    from .datasource import DataSource, DataSources
    from .fhir import MimeType, Version
    from .functions import to_coding, to_ecl_value_set, to_snomed_coding
    from .udfs import (
        Equivalence,
        PropertyType,
        designation,
        display,
        member_of,
        property_of,
        subsumed_by,
        subsumes,
        translate,
    )

# Maps each lazily-exported public name to the submodule that defines it.
_LAZY_EXPORTS = {
    "Coding": "pathling.coding",
    "PathlingContext": "pathling.context",
    "StorageType": "pathling.context",
    "Expression": "pathling.core",
    "VariableExpression": "pathling.core",
    "DataSource": "pathling.datasource",
    "DataSources": "pathling.datasource",
    "MimeType": "pathling.fhir",
    "Version": "pathling.fhir",
    "to_coding": "pathling.functions",
    "to_snomed_coding": "pathling.functions",
    "to_ecl_value_set": "pathling.functions",
    "member_of": "pathling.udfs",
    "translate": "pathling.udfs",
    "subsumes": "pathling.udfs",
    "subsumed_by": "pathling.udfs",
    "property_of": "pathling.udfs",
    "display": "pathling.udfs",
    "designation": "pathling.udfs",
    "PropertyType": "pathling.udfs",
    "Equivalence": "pathling.udfs",
}

__all__ = list(_LAZY_EXPORTS)

# The package submodules that may be accessed lazily after a bare
# ``import pathling`` (e.g. ``pathling.udfs.member_of``). This curated allow-list
# keeps the fallback safe - unknown names still raise ``AttributeError`` - and
# restores the pre-shim behaviour where these submodules were importable as
# package attributes, without importing PySpark on a bare ``import pathling``.
_LAZY_SUBMODULES = (
    "coding",
    "context",
    "core",
    "datasource",
    "fhir",
    "functions",
    "udfs",
)


def __getattr__(name: str) -> Any:
    """Resolves a public name or submodule by importing it on first access.

    :param name: the attribute being accessed on the ``pathling`` package.
    :return: the resolved attribute value.
    :raises AttributeError: if the name is neither a known public export nor a
            known package submodule.
    """
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is not None:
        module = importlib.import_module(module_name)
        value = getattr(module, name)
        # Cache on the package so subsequent lookups bypass this hook.
        globals()[name] = value
        return value
    if name in _LAZY_SUBMODULES:
        submodule = importlib.import_module(f"{__name__}.{name}")
        # Cache the submodule so subsequent lookups bypass this hook.
        globals()[name] = submodule
        return submodule
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list:
    """Returns the public names and submodules of the package for introspection.

    :return: the sorted list of public export names and lazily-available
             submodule names.
    """
    return sorted(set(__all__) | set(_LAZY_SUBMODULES))
