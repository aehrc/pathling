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

from typing import Optional

from pyspark.sql.functions import lit, struct

from pathling.functions import SNOMED_URI


class Coding:
    """
    A Coding represents a code in a code system.
    See: https://hl7.org/fhir/R4/datatypes.html#Coding
    """

    def __init__(
        self,
        system: str,
        code: str,
        version: Optional[str] = None,
        display: Optional[str] = None,
        user_selected: Optional[bool] = None,
    ):
        """
        :param system: a URI that identifies the code system
        :param code: the code
        :param version: a URI that identifies the version of the code system
        :param display: the display text for the Coding
        :param user_selected: an indicator of whether the Coding was chosen directly by the user
        """
        self.system = system
        self.code = code
        self.version = version
        self.display = display
        self.user_selected = user_selected

    def to_literal(self):
        """
        Converts a Coding into a Column that contains a Coding struct. The Coding
        struct Column can be used as an input to terminology functions such as `member_of` and
        `translate`.

        :return: a Column containing a Coding struct
        """
        id_column = lit(None).alias("id")
        system_column = lit(self.system).alias("system")
        version_column = lit(self.version).alias("version")
        code_column = lit(self.code).alias("code")
        display_column = lit(self.display).alias("display")
        user_selected_column = lit(self.user_selected).alias("userSelected")
        return struct(
            id_column,
            system_column,
            version_column,
            code_column,
            display_column,
            user_selected_column,
        )

    @classmethod
    def of_snomed(
        cls,
        code: str,
        version: Optional[str] = None,
        display: Optional[str] = None,
        user_selected: Optional[bool] = None,
    ) -> "Coding":
        """
        Creates a SNOMED Coding.

        :param code: the code
        :param version: a URI that identifies the version of the code system
        :param display: the display text for the Coding
        :param user_selected: an indicator of whether the Coding was chosen directly by the user
        :return: a SNOMED coding with given arguments.
        """
        return Coding(SNOMED_URI, code, version, display, user_selected)
