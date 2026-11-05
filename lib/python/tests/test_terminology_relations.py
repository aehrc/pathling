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

"""Tests for the value set and concept map relations of the Python API.

The shared ``pathling_ctx`` fixture uses the mock terminology service, which holds the value set
``http://snomed.info/sct?fhir_vs=refset/723264001`` and the concept map
``http://snomed.info/sct?fhir_cm=100``. The supplied resources are the worked examples of the SQL
on FHIR "Terminology in SQL" page.

Author: John Grimes.
"""

import json

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import Row

SNOMED = "http://snomed.info/sct"
ICD10 = "http://hl7.org/fhir/sid/icd-10"
LOINC = "http://loinc.org"
MOCK_VALUE_SET = "http://snomed.info/sct?fhir_vs=refset/723264001"
MOCK_CONCEPT_MAP = "http://snomed.info/sct?fhir_cm=100"

VALUE_SET_COLUMNS = ["system", "version", "code", "display", "inactive"]
CONCEPT_MAP_COLUMNS = [
    "source_system",
    "source_version",
    "source_code",
    "source_display",
    "target_system",
    "target_version",
    "target_code",
    "target_display",
    "relationship",
]

CVD_VALUE_SET = json.dumps(
    {
        "resourceType": "ValueSet",
        "url": "http://example.org/ValueSet/cardiovascular-disease",
        "version": "2026",
        "status": "active",
        "expansion": {
            "timestamp": "2026-09-16T00:00:00Z",
            "total": 3,
            "contains": [
                {
                    "abstract": True,
                    "display": "Ischaemic heart disease",
                    "contains": [
                        {"system": SNOMED, "code": "22298006", "display": "MI"},
                        {"system": ICD10, "version": "2019", "code": "I21"},
                    ],
                }
            ],
        },
    }
)

SCT_TO_ICD10 = json.dumps(
    {
        "resourceType": "ConceptMap",
        "url": "http://example.org/ConceptMap/sct-to-icd10",
        "version": "2026",
        "status": "active",
        "group": [
            {
                "source": SNOMED,
                "target": ICD10 + "|2019",
                "element": [
                    {
                        "code": "22298006",
                        "target": [{"code": "I21", "equivalence": "equivalent"}],
                    },
                    {
                        "code": "73211009",
                        "target": [{"code": "E14", "equivalence": "narrower"}],
                    },
                    {
                        "code": "102499006",
                        "target": [{"equivalence": "unmatched"}],
                    },
                ],
            }
        ],
    }
)


# ---------------------------------------------------------------------------------------------
# Value set relations.
# ---------------------------------------------------------------------------------------------


def test_value_set_resolves_a_canonical_to_one_row_per_member(pathling_ctx):
    # The mock value set has one SNOMED CT member, under the five specified columns.
    df = pathling_ctx.value_set(MOCK_VALUE_SET)

    assert df.columns == VALUE_SET_COLUMNS
    assert df.collect() == [Row(SNOMED, None, "368529001", None, None)]


def test_value_set_accepts_a_member_cap(pathling_ctx):
    # The single member fits a cap of one.
    assert pathling_ctx.value_set(MOCK_VALUE_SET, max_members=1).count() == 1


def test_value_set_unknown_canonical_fails(pathling_ctx):
    # A value set that cannot be resolved is an error, not an empty relation.
    with pytest.raises(Py4JJavaError, match="could not be resolved"):
        pathling_ctx.value_set("http://example.org/ValueSet/unknown|1")


def test_value_set_from_json_uses_the_supplied_expansion(pathling_ctx):
    # The abstract grouping entry contributes no row; its two nested members do.
    rows = pathling_ctx.value_set_from_json(CVD_VALUE_SET).collect()

    assert rows == [
        Row(SNOMED, None, "22298006", "MI", None),
        Row(ICD10, "2019", "I21", None, None),
    ]


def test_value_set_from_json_over_the_cap_fails(pathling_ctx):
    with pytest.raises(Py4JJavaError, match="more than the maximum of 1 members"):
        pathling_ctx.value_set_from_json(CVD_VALUE_SET, max_members=1)


# ---------------------------------------------------------------------------------------------
# Concept map relations.
# ---------------------------------------------------------------------------------------------


def test_concept_map_resolves_a_canonical_to_one_row_per_mapping(pathling_ctx):
    # The mock concept map maps one SNOMED CT code to a SNOMED CT and a LOINC code, with the R4
    # equivalences converted to R5 relationships.
    df = pathling_ctx.concept_map(MOCK_CONCEPT_MAP)

    assert df.columns == CONCEPT_MAP_COLUMNS
    rows = df.orderBy("target_system").collect()
    assert rows == [
        Row(
            SNOMED, None, "368529001", None, LOINC, None, "55916-3", None, "related-to"
        ),
        Row(
            SNOMED,
            None,
            "368529001",
            None,
            SNOMED,
            None,
            "368529002",
            None,
            "equivalent",
        ),
    ]


def test_concept_map_over_the_cap_fails(pathling_ctx):
    with pytest.raises(Py4JJavaError, match="more than the maximum of 1 mappings"):
        pathling_ctx.concept_map(MOCK_CONCEPT_MAP, max_mappings=1)


def test_concept_map_unknown_canonical_fails(pathling_ctx):
    with pytest.raises(Py4JJavaError, match="could not be resolved"):
        pathling_ctx.concept_map("http://example.org/ConceptMap/unknown")


def test_concept_map_from_json_translates_in_sql(pathling_ctx):
    # The worked example: a LEFT JOIN tells a translated code, a code the map says has no mapping,
    # and a code the map is silent about apart.
    spark = pathling_ctx.spark
    spark.createDataFrame(
        [("p1", SNOMED, "22298006"), ("p2", ICD10, "I21"), ("p4", SNOMED, "102499006")],
        ["patient_id", "system", "code"],
    ).createOrReplaceTempView("conditions")
    pathling_ctx.concept_map_from_json(SCT_TO_ICD10).createOrReplaceTempView(
        "sct_to_icd10"
    )

    rows = spark.sql(
        """
        SELECT conditions.patient_id, sct_to_icd10.source_code,
               sct_to_icd10.target_version, sct_to_icd10.target_code
        FROM conditions
        LEFT JOIN sct_to_icd10
          ON sct_to_icd10.source_system = conditions.system
         AND sct_to_icd10.source_code = conditions.code
        ORDER BY conditions.patient_id
        """
    ).collect()

    assert rows == [
        Row("p1", "22298006", "2019", "I21"),
        Row("p2", None, None, None),
        Row("p4", "102499006", "2019", None),
    ]


def test_concept_map_from_json_with_max_mappings(pathling_ctx):
    # The three rows of the worked example fit a cap of three but not of two.
    assert pathling_ctx.concept_map_from_json(SCT_TO_ICD10, max_mappings=3).count() == 3
    with pytest.raises(Py4JJavaError, match="more than the maximum of 2 mappings"):
        pathling_ctx.concept_map_from_json(SCT_TO_ICD10, max_mappings=2)
