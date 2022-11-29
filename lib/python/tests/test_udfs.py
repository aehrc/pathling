#  Copyright 2022 Commonwealth Scientific and Industrial Research
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

import logging
import os
from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, BooleanType, Row, ArrayType, \
    IntegerType
from pytest import fixture

from pathling import PathlingContext
from pathling.coding import Coding
from pathling.etc import SNOMED_URI
from pathling.etc import find_jar as find_pathling_jar
from pathling.udfs import member_of, subsumes, subsumed_by, translate, display

PROJECT_DIR = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))


@fixture(scope="module")
def spark(request):
    """
    Fixture for creating a Spark Session available for all tests in this
    testing session.
    """

    gateway_log = logging.getLogger('java_gateway')
    gateway_log.setLevel(logging.ERROR)

    # Get the shaded JAR for testing purposes.
    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[1]') \
        .config('spark.jars', find_pathling_jar()) \
        .config('spark.sql.warehouse.dir', mkdtemp()) \
        .config('spark.driver.memory', '4g') \
        .getOrCreate()

    request.addfinalizer(lambda: spark.stop())

    return spark


@fixture(scope="module")
def ptl(spark):
    return PathlingContext.create(spark, mock_terminology=True)


CODING_TYPE = StructType([
    StructField("id", StringType(), True),
    StructField("system", StringType(), True),
    StructField("version", StringType(), True),
    StructField("code", StringType(), True),
    StructField("display", StringType(), True),
    StructField("userSelected", BooleanType(), True),
    StructField("_fid", IntegerType(), True),
])

LOINC_URI = "http://loinc.org"

CodingRow = Row("id", "system", "version", "code", "display", "userSelected", "_fid")


def snomed_coding_row(code: str):
    return CodingRow(None, SNOMED_URI, None, code, None, None, None)


def loinc_coding_row(code: str):
    return CodingRow(None, LOINC_URI, None, code, None, None, None)


Result = Row("id", "result")


def test_member_of(spark: SparkSession, ptl: PathlingContext):
    df = spark.createDataFrame(
            [
                ("code-1", snomed_coding_row("368529001")),
                ("code-2", loinc_coding_row("55915-3")),
                ("code-3", None),
            ],
            schema=StructType([StructField("id", StringType()),
                               StructField("code", CODING_TYPE)])
    )

    result_df_col = df.select(
            df["id"],
            member_of("code",
                      "http://snomed.info/sct?fhir_vs=refset/723264001").alias(
                    "is_member"))

    assert result_df_col.collect() == [Result("code-1", True), Result("code-2", False),
                                       Result("code-3", None)]

    result_df_str = df.select(
            "id",
            member_of("code",
                      "http://loinc.org/vs/LP14885-5").alias(
                    "is_member"))

    assert result_df_str.collect() == [Result("code-1", False), Result("code-2", True),
                                       Result("code-3", None)]

    result_df_coding = df.limit(1).select(
            "id",
            member_of(Coding(SNOMED_URI, "368529001"),
                      "http://snomed.info/sct?fhir_vs=refset/723264001").alias(
                    "result"))

    assert result_df_coding.collect() == [Result("code-1", True)]


def test_member_of_array(spark: SparkSession, ptl: PathlingContext):
    PathlingContext.create(spark, mock_terminology=True)
    df = spark.createDataFrame(
            [
                ("code-1", [snomed_coding_row("368529001"), snomed_coding_row("368529002")]),
                ("code-2", [loinc_coding_row("55915-3"), loinc_coding_row("55915-4")]),
                ("code-3", None),
            ],
            schema=StructType([StructField("id", StringType()),
                               StructField("code", ArrayType(CODING_TYPE))])
    )

    result_df_col = df.select(
            df["id"],
            member_of("code",
                      "http://snomed.info/sct?fhir_vs=refset/723264001").alias(
                    "is_member"))

    assert result_df_col.collect() == [Result("code-1", True), Result("code-2", False),
                                       Result("code-3", None)]

    result_df_str = df.select(
            "id",
            member_of("code",
                      "http://loinc.org/vs/LP14885-5").alias(
                    "is_member"))

    assert result_df_str.collect() == [Result("code-1", False), Result("code-2", True),
                                       Result("code-3", None)]

    result_df_coding = df.limit(1).select(
            "id",
            member_of(Coding(SNOMED_URI, "368529001"),
                      "http://snomed.info/sct?fhir_vs=refset/723264001").alias(
                    "result"))

    assert result_df_coding.collect() == [Result("code-1", True)]


@fixture(scope="module")
def subsumption_df(spark: SparkSession):
    return spark.createDataFrame(
            [
                ("id-1", snomed_coding_row("107963000"), [snomed_coding_row("63816008")]),
                ("id-2", loinc_coding_row("55915-3"),
                 [snomed_coding_row("63816008"), loinc_coding_row("55914-3")]),
                ("id-3", None, [snomed_coding_row("107963000")]),
            ],
            schema=StructType([StructField("id", StringType()),
                               StructField("codeA", CODING_TYPE),
                               StructField("codeB", ArrayType(CODING_TYPE))
                               ])
    )


def test_subsumes(subsumption_df, ptl: PathlingContext):
    result_df = subsumption_df.select("id",
                                      subsumes(subsumption_df["codeA"], "codeB").alias("result"))
    assert result_df.collect() == [
        Result("id-1", True),
        Result("id-2", False),
        Result("id-3", None)
    ]

    result_df = subsumption_df.select("id", subsumes("codeA", Coding(SNOMED_URI, "63816008")).alias(
            "result"))
    assert result_df.collect() == [
        Result("id-1", True),
        Result("id-2", False),
        Result("id-3", None)
    ]

    result_df = subsumption_df.select("id",
                                      subsumes(Coding(LOINC_URI, "55914-3"),
                                               subsumption_df["codeB"]).alias(
                                              "result"))
    assert result_df.collect() == [
        Result("id-1", False),
        Result("id-2", True),
        Result("id-3", False)
    ]


def test_subsumed_by(subsumption_df, ptl: PathlingContext):
    result_df = subsumption_df.select("id",
                                      subsumed_by(subsumption_df["codeB"], "codeA").alias("result"))
    assert result_df.collect() == [
        Result("id-1", True),
        Result("id-2", False),
        Result("id-3", None)
    ]

    result_df = subsumption_df.select("id",
                                      subsumed_by(Coding(SNOMED_URI, "63816008"), "codeA").alias(
                                              "result"))
    assert result_df.collect() == [
        Result("id-1", True),
        Result("id-2", False),
        Result("id-3", None)
    ]

    result_df = subsumption_df.select("id",
                                      subsumed_by(subsumption_df["codeB"],
                                                  Coding(LOINC_URI, "55914-3")).alias("result"))
    assert result_df.collect() == [
        Result("id-1", False),
        Result("id-2", True),
        Result("id-3", False)
    ]


def test_translate(spark: SparkSession, ptl: PathlingContext):
    df = spark.createDataFrame(
            [
                ("id-1", snomed_coding_row("368529001")),
                ("id-2", loinc_coding_row("55915-3")),
                ("id-3", None),
            ],
            schema=StructType([StructField("id", StringType()),
                               StructField("code", CODING_TYPE)])
    )

    result_df = df.select("id", translate(
            df["code"], "http://snomed.info/sct?fhir_cm=100").alias("result"))

    assert result_df.collect() == [
        Result("id-1", [snomed_coding_row("368529002")]),
        Result("id-2", []),
        Result("id-3", None),
    ];

    result_df = df.select("id", translate(
            "code", "http://snomed.info/sct?fhir_cm=100",
            equivalences="equivalent,relatedto").alias("result"))
    assert result_df.collect() == [
        Result("id-1", [snomed_coding_row("368529002"), loinc_coding_row("55916-3")]),
        Result("id-2", []),
        Result("id-3", None),
    ];

    result_df = df.select("id", translate(
            "code", "http://snomed.info/sct?fhir_cm=100",
            equivalences="equivalent,relatedto", target=LOINC_URI).alias("result"))
    assert result_df.collect() == [
        Result("id-1", [loinc_coding_row("55916-3")]),
        Result("id-2", []),
        Result("id-3", None),
    ];

    result_df = df.select("id", translate(
            "code", "http://snomed.info/sct?fhir_cm=200",
            equivalences="equivalent,relatedto").alias("result"))
    assert result_df.collect() == [
        Result("id-1", []),
        Result("id-2", []),
        Result("id-3", None),
    ];

    result_df = df.limit(1).select("id", translate(
            Coding(LOINC_URI, "55915-3"), "http://snomed.info/sct?fhir_cm=200",
            reverse=True,
            equivalences="relatedto").alias("result"))

    assert result_df.collect() == [
        Result("id-1", [snomed_coding_row("368529002")]),
    ];


def test_display(spark: SparkSession, ptl: PathlingContext):
    df = spark.createDataFrame(
            [
                ("id-1", snomed_coding_row("439319006")),
                ("id-2", loinc_coding_row("55915-3")),
                ("id-3", None),
            ],
            schema=StructType([StructField("id", StringType()),
                               StructField("code", CODING_TYPE)])
    )

    expected_result = [
        Result("id-1", "Screening for phenothiazine in serum"),
        Result("id-2", None),
        Result("id-3", None),
    ]

    result_df = df.select("id", display("code").alias("result"))
    assert result_df.collect() == expected_result

    result_df = df.select("id", display(df["code"]).alias("result"))
    assert result_df.collect() == expected_result

    result_df = df.limit(1).select("id", display(Coding(SNOMED_URI, "439319006")).alias("result"))
    assert result_df.collect() == [
        Result("id-1", "Screening for phenothiazine in serum")
    ];
