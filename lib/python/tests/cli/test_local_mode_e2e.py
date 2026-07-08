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

"""End-to-end test for CLI local terminology mode.

A real SNOMED CT store is built from the checked-in ``rf2-mini`` fixture through
the library API, then the ``member-of`` command is driven through Click's
``CliRunner`` with ``--tx-store`` and a config file, proving the whole chain
(flag/config -> resolved config -> local session -> local evaluation) works with
no terminology server. The store is populated once for the module.

Author: John Grimes.
"""

import csv
import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pytest import fixture

from pathling import PathlingContext
from pathling._version import __delta_version__, __java_version__, __scala_version__
from pathling.cli import session as session_module
from pathling.cli.main import cli
from tests.cli.conftest import make_cli_runner

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, os.pardir)
)
RF2_MINI = os.path.join(
    PROJECT_ROOT,
    "terminology",
    "src",
    "test",
    "resources",
    "rf2-mini",
    "international-20230601",
)
SNOMED = "http://snomed.info/sct"
# The implicit value set of DIABETES (1002007) and its descendants; TYPE1_DIABETES
# (1003002) is a descendant, HYPERTENSION (1007001) is not.
DIABETES_VALUE_SET = "http://snomed.info/sct?fhir_vs=ecl/<<1002007"
DIABETES = "1002007"
TYPE1_DIABETES = "1003002"
HYPERTENSION = "1007001"


@fixture(scope="module")
def spark_session(request):
    """Creates a Spark session backed by the built library-runtime JAR."""
    logging.getLogger("java_gateway").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.appName("pathling-cli-local-mode-e2e")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            f"au.csiro.pathling:library-runtime:{__java_version__},"
            f"io.delta:delta-spark_{__scala_version__}:{__delta_version__}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", mkdtemp())
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())
    return spark


@fixture(scope="module")
def local_store(spark_session):
    """Imports the rf2-mini SNOMED fixture into a temporary store once."""
    store = os.path.join(mkdtemp(), "store")
    PathlingContext.create(spark_session).import_snomed(RF2_MINI, store)
    return store


def _write_codes(directory, name="codes.csv"):
    """Writes a small CSV of SNOMED codes and returns its path."""
    path = os.path.join(directory, name)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["code"])
        for code in (DIABETES, TYPE1_DIABETES, HYPERTENSION):
            writer.writerow([code])
    return path


def _local_context_spy(monkeypatch, spark_session, store):
    """Routes the CLI's context creation to a real local-mode context.

    Rather than let the CLI build a second Spark session in-process, this reuses
    the module's session while still constructing the context from the resolved
    CLI configuration, so the local-mode parameter threading is exercised for
    real. The resolved configuration is captured so the test can assert that no
    terminology server was configured.

    :return: a dict populated with the resolved configuration on invocation.
    """
    captured = {}

    def factory(config, console=None):
        captured["tx_store"] = config.tx_store
        captured["tx_server_explicit"] = config.tx_server_explicit
        # Build the context exactly as the CLI would from this configuration,
        # but over the shared session so the JVM is not started twice.
        monkeypatch.setattr(
            session_module, "_build_quiet_spark", lambda cfg: spark_session
        )
        return session_module._create_pathling_context(config)

    monkeypatch.setattr(session_module, "create_context", factory)
    return captured


def _membership(csv_output):
    """Parses the member-of CSV output into a code -> membership map."""
    reader = csv.DictReader(csv_output.splitlines())
    return {row["code"]: row["member_of"] for row in reader}


def test_member_of_via_flag_offline(monkeypatch, spark_session, local_store, tmp_path):
    """member-of with --tx-store evaluates against the local store, offline."""
    captured = _local_context_spy(monkeypatch, spark_session, local_store)
    codes = _write_codes(str(tmp_path))
    runner = make_cli_runner()

    result = runner.invoke(
        cli,
        [
            "--tx-store",
            local_store,
            "member-of",
            codes,
            "--code-column",
            "code",
            "--system",
            SNOMED,
            "--value-set",
            DIABETES_VALUE_SET,
            "--format",
            "csv",
        ],
    )

    assert result.exit_code == 0, result.output
    membership = _membership(result.stdout)
    assert membership[DIABETES] == "True"
    assert membership[TYPE1_DIABETES] == "True"
    assert membership[HYPERTENSION] == "False"
    # The local store was selected and no server was explicitly configured.
    assert captured["tx_store"] is not None
    assert captured["tx_store"].path == local_store
    assert captured["tx_server_explicit"] is False
