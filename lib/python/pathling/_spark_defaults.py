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

"""The single, PySpark-free source of truth for Pathling's managed Spark defaults.

The managed coordinates, the Delta SQL extension, and the Delta catalog that
every Pathling Spark session requires are defined here, built from the versions
in :mod:`pathling._version`. Both the session builder
(:func:`pathling.context._build_spark_session`) and the CLI merge logic
(:mod:`pathling.cli.sparkconf`) import these values so the two cannot drift
apart. This module deliberately imports no PySpark, so the CLI configuration
path can reference the defaults without paying Spark's import cost.

Author: John Grimes.
"""

from pathling._version import (
    __delta_version__,
    __java_version__,
    __scala_version__,
)

# The Spark configuration key holding the comma-separated Maven coordinates.
PACKAGES_KEY = "spark.jars.packages"

# The Spark configuration key holding the comma-separated SQL extension classes.
EXTENSIONS_KEY = "spark.sql.extensions"

# The Spark configuration key for the session catalog implementation.
CATALOG_KEY = "spark.sql.catalog.spark_catalog"

# The managed Maven coordinate (group:artifact) for the Pathling library runtime.
LIBRARY_RUNTIME_COORDINATE = "au.csiro.pathling:library-runtime"

# The managed Maven coordinate (group:artifact) for Delta Lake, which carries the
# Scala binary version in its artifact identifier.
DELTA_COORDINATE = f"io.delta:delta-spark_{__scala_version__}"

# The group:artifact identities of the coordinates Pathling manages, used to
# detect a user-supplied override at a different version.
MANAGED_COORDINATES = frozenset({LIBRARY_RUNTIME_COORDINATE, DELTA_COORDINATE})

# The Delta SQL extension class that Pathling always requires.
DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"

# The Delta catalog implementation that Pathling always requires.
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def managed_spark_defaults() -> dict:
    """Returns the Spark configuration that Pathling always requires.

    The packages string lists both managed coordinates at the versions declared
    in :mod:`pathling._version` and retains a trailing comma, matching the
    historical inline literal. The extension and catalog are fixed Delta class
    names.

    :return: a mapping of managed Spark configuration key to value.
    """
    return {
        PACKAGES_KEY: (
            f"{LIBRARY_RUNTIME_COORDINATE}:{__java_version__},"
            f"{DELTA_COORDINATE}:{__delta_version__},"
        ),
        EXTENSIONS_KEY: DELTA_EXTENSION,
        CATALOG_KEY: DELTA_CATALOG,
    }
