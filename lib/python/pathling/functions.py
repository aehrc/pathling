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

import urllib.parse
from typing import Optional

from pyspark.sql import Column
from pyspark.sql.functions import lit, struct

SNOMED_URI = "http://snomed.info/sct"
LOINC_URI = "http://loinc.org"


def to_coding(
    coding_column: Column, system: str, version: Optional[str] = None
) -> Column:
    """
    Converts a Column containing codes into a Column that contains a Coding struct. The Coding
    struct Column can be used as an input to terminology functions such as `member_of` and
    `translate`.

    :param coding_column: the Column containing the codes
    :param system: the URI of the system the codes belong to
    :param version: the version of the code system
    :return: a Column containing a Coding struct
    """
    id_column = lit(None).alias("id")
    system_column = lit(system).alias("system")
    version_column = lit(version).alias("version")
    display_column = lit(None).alias("display")
    user_selected_column = lit(None).alias("userSelected")
    return struct(
        id_column,
        system_column,
        version_column,
        coding_column.alias("code"),
        display_column,
        user_selected_column,
    )


def to_snomed_coding(coding_column: Column, version: Optional[str] = None) -> Column:
    """
    Converts a Column containing codes into a Column that contains a SNOMED Coding struct. The
    Coding struct Column can be used as an input to terminology functions such as `member_of` and
    `translate`.

    :param coding_column: the Column containing the codes
    :param version: the version of the code system
    :return: a Column containing a Coding struct
    """
    return to_coding(coding_column, SNOMED_URI, version)


def to_loinc_coding(coding_column: Column, version: Optional[str] = None) -> Column:
    """
    Converts a Column containing codes into a Column that contains a LOINC Coding struct. The
    Coding struct Column can be used as an input to terminology functions such as `member_of` and
    `translate`.

    :param coding_column: the Column containing the codes
    :param version: the version of the code system
    :return: a Column containing a Coding struct
    """
    return to_coding(coding_column, LOINC_URI, version)


def to_ecl_value_set(ecl: str) -> str:
    """
    Converts a SNOMED CT ECL expression into a FHIR ValueSet URI. Can be used with the `member_of`
    function.

    :param ecl: the ECL expression
    :return: the ValueSet URI
    """
    return SNOMED_URI + "?fhir_vs=ecl/" + urllib.parse.quote(ecl, safe="()*!'")
