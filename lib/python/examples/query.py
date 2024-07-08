#  Copyright 2023 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
from pyspark.sql import DataFrame, SparkSession
from tempfile import mkdtemp

from pathling import PathlingContext, DataSource, Expression as exp
from pathling._version import __java_version__

HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(HERE, "data")
BUNDLES_DIR = os.path.join(DATA_DIR, "bundles")
NDJSON_DIR = os.path.join(DATA_DIR, "resources")

TEMP_DIR = mkdtemp()
PARQUET_DIR = os.path.join(TEMP_DIR, "parquet")
DELTA_DIR = os.path.join(TEMP_DIR, "delta")
WAREHOUSE_DIR = os.path.join(TEMP_DIR, "warehouse")
NDJSON_DIR_2 = os.path.join(TEMP_DIR, "ndjson")

# Configure Hive, so that we can test out the tables functionality.
spark = (
    SparkSession.builder.config(
        "spark.jars.packages",
        f"au.csiro.pathling:library-runtime:{__java_version__},io.delta:delta-spark_2.12:3.2.0",
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .getOrCreate()
)

# Enable Delta, so that we can test out the Delta functionality.
pc = PathlingContext.create(spark)

# Use a schema called "datasources" when interacting with the table catalog.
pc.spark.sql("CREATE SCHEMA IF NOT EXISTS datasources")
pc.spark.catalog.setCurrentDatabase("datasources")


def aggregate(data_source: DataSource) -> DataFrame:
    """
    A function for testing out our data sources with an aggregate query.
    """

    result = data_source.aggregate(
        "Patient",
        # Number of patients.
        aggregations=[exp("count()", "Number of patients")],
        # Group by gender.
        groupings=[exp("gender", "Gender")],
    )
    result.show()
    return result


def extract(data_source: DataSource) -> DataFrame:
    """
    A function for testing out our data sources with an extract query.
    """

    result = data_source.extract(
        "Patient",
        columns=[
            exp("name.first().given.first()", "Given name"),
            exp("name.first().family", "Family name"),
            exp("telecom.where(system = 'phone').value", "Phone number"),
            exp(
                "reverseResolve(Condition.subject).exists("
                "code.subsumedBy(http://snomed.info/sct|404684003))",
                "Clinical finding",
            ),
        ],
    )
    result.show()
    return result


# Read from NDJSON files.
print(f"Reading from NDJSON files: {NDJSON_DIR}")
ndjson = pc.read.ndjson(NDJSON_DIR)
aggregate(ndjson)
extract(ndjson)

# Read from FHIR JSON Bundles.
print(f"Reading from FHIR JSON Bundles: {BUNDLES_DIR}")
bundles = pc.read.bundles(BUNDLES_DIR, ["Patient", "Condition"])
aggregate(bundles)
extract(bundles)

# Save the NDJSON data as Parquet.
print(f"Writing to Parquet files: {PARQUET_DIR}")
ndjson.write.parquet(PARQUET_DIR)

# Read from Parquet files.
print(f"Reading from Parquet files: {PARQUET_DIR}")
parquet = pc.read.parquet(PARQUET_DIR)
aggregate(parquet)
extract(parquet)

# Save the bundles data as Delta.
print(f"Writing to Delta files: {DELTA_DIR}")
bundles.write.delta(DELTA_DIR)

# Read from Delta files.
print(f"Reading from Delta files: {DELTA_DIR}")
delta = pc.read.delta(DELTA_DIR)
aggregate(delta)
extract(delta)

# Save the NDJSON data to tables.
print(f"Writing to tables: {WAREHOUSE_DIR}")
ndjson.write.tables()

# Read from tables.
print(f"Reading from tables: {WAREHOUSE_DIR}")
tables = pc.read.tables()
aggregate(tables)
extract(tables)

# Save the bundles data as NDJSON.
print(f"Writing to NDJSON files: {NDJSON_DIR_2}")
ndjson.write.ndjson(NDJSON_DIR_2)

# Read from the written NDJSON files.
print(f"Reading from NDJSON files: {NDJSON_DIR_2}")
ndjson2 = pc.read.ndjson(NDJSON_DIR_2)
aggregate(ndjson2)
extract(ndjson2)
