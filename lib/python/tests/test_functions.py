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

from pathling.functions import to_ecl_value_set


def test_to_ecl_value_set():
    ecl = (
        "("
        " (^929360071000036103|Medicinal product unit of use refset| : "
        "  {"
        "   700000111000036105|Strength reference set| >= #10000,"
        "   177631000036102|has unit| = 700000881000036108|microgram/each|,"
        "   700000081000036101|has intended active ingredient| = 1978011000036103|codeine|"
        "  },"
        "  {"
        "   700000111000036105|Strength reference set| >= #250,"
        "   177631000036102|has unit| = 700000801000036102|mg/each|,"
        "   700000081000036101|has intended active ingredient| = 2442011000036104|paracetamol|"
        "  },"
        "  30523011000036108|has manufactured dose form| = 154011000036109|tablet|"
        " )"
        ")"
    )
    expected = (
        "http://snomed.info/sct?fhir_vs=ecl/(%20("
        "%5E929360071000036103%7CMedicinal%20product%20unit%20of%20use%20refset%7C%20%3A"
        "%20%20%20%7B%20%20%20700000111000036105%7CStrength%20reference%20set%7C%20%3E%3D"
        "%20%2310000%2C%20%20%20177631000036102%7Chas%20unit%7C%20%3D%20700000881000036108"
        "%7Cmicrogram%2Feach%7C%2C%20%20%20700000081000036101%7Chas%20intended%20active"
        "%20ingredient%7C%20%3D%201978011000036103%7Ccodeine%7C%20%20%7D%2C%20%20%7B%20%20"
        "%20700000111000036105%7CStrength%20reference%20set%7C%20%3E%3D%20%23250%2C%20%20"
        "%20177631000036102%7Chas%20unit%7C%20%3D%20700000801000036102%7Cmg%2Feach%7C%2C"
        "%20%20%20700000081000036101%7Chas%20intended%20active%20ingredient%7C%20%3D"
        "%202442011000036104%7Cparacetamol%7C%20%20%7D%2C%20%2030523011000036108%7Chas"
        "%20manufactured%20dose%20form%7C%20%3D%20154011000036109%7Ctablet%7C%20))"
    )
    print(expected)
    assert to_ecl_value_set(ecl) == expected
