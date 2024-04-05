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
from datetime import datetime
from typing import Optional, NamedTuple
import json

from py4j.java_gateway import JavaObject

from pathling.jvm import jvm_pathling


class MimeType:
    """
    Constants for FHIR encoding mime types.
    """

    FHIR_JSON: str = "application/fhir+json"
    FHIR_XML: str = "application/fhir+xml"


class Version:
    """
    Constants for FHIR versions.
    """

    R4: str = "R4"


class Reference(NamedTuple):
    reference: Optional[str] = None
    type: Optional[str] = None
    display: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(self._asdict())

    def to_java(self) -> JavaObject:
        return (
            jvm_pathling()
            .export.fhir.Reference.builder()
            .reference(self.reference)
            .type(self.type)
            .display(self.display)
            .build()
        )


def as_fhir_instant(dt: datetime) -> str:
    """
    Convert a datetime to a FHIR instant string.
    :param dt: datetime to convert
    :return: string representation of the datetime as a FHIR instant
    """
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def from_fhir_instant(s: str) -> datetime:
    """
    Convert a FHIR instant string to a datetime.
    :param s: a string representation of a FHIR instant
    :return: datetime representation of the FHIR instant
    """
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
