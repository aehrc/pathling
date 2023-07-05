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

from typing import Any, Optional, Union, Collection

from py4j.java_gateway import JavaObject
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import lit

from pathling.coding import Coding

CodingArg = Union[Column, str, Coding]
EquivalenceArg = Union[str, Collection[str]]


def _coding_to_java_column(coding: Optional[CodingArg]) -> JavaObject:
    if coding is None:
        return _to_java_column(lit(None))
    else:
        return _to_java_column(
            coding.to_literal() if isinstance(coding, Coding) else coding
        )


def _ensure_collection(
    collection_or_value: Optional[Union[Any, Collection[Any]]]
) -> Optional[Collection[Any]]:
    return (
        collection_or_value
        if isinstance(collection_or_value, Collection)
        and not isinstance(collection_or_value, str)
        else [collection_or_value]
        if collection_or_value is not None
        else None
    )


def _get_jvm() -> JavaObject:
    """
    Gets the py4j JVM associated with this process.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None
    assert sc._jvm is not None
    return sc._jvm


def _invoke_udf(name: str, *args: Any) -> Column:
    """
    Invokes a Terminology UDF function identified by name with args
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    jf = getattr(_get_jvm().au.csiro.pathling.sql.Terminology, name)
    return Column(jf(*args))


def _invoke_support_function(name: str, *args: Any) -> Any:
    """
    Invokes a Terminology supporting function identified by name with args
    """
    jf = getattr(_get_jvm().au.csiro.pathling.sql.TerminologySupport, name)
    return jf(*args)


class PropertyType:
    """
    Allowed property types.
    """

    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DECIMAL = "decimal"
    DATETIME = "dateTime"
    CODE = "code"
    CODING = "Coding"


class Equivalence:
    """
    Concept map equivalences, see https://www.hl7.org/fhir/R4/valueset-concept-map-equivalence.html.
    """

    RELATEDTO = "relatedto"
    EQUIVALENT = "equivalent"
    EQUAL = "equal"
    WIDER = "wider"
    SUBSUMES = "subsumes"
    NARROWER = "narrower"
    SPECIALIZES = "specializes"
    INEXACT = "inexact"
    UNMATCHED = "unmatched"
    DISJOINT = "disjoint"


def member_of(coding: CodingArg, value_set_uri: str) -> Column:
    """
    Takes a Coding or array of Codings column as its input. Returns the column which contains a
    Boolean value, indicating whether any of the input Codings is the member of the specified FHIR
    ValueSet.

    :param coding: a Column containing a struct representation of a Coding or an array of such
           structs.
    :param value_set_uri: an identifier for a FHIR ValueSet
    :return: a Column containing the result of the operation.
    """
    return _invoke_udf("member_of", _coding_to_java_column(coding), value_set_uri)


def translate(
    coding: CodingArg,
    concept_map_uri: str,
    reverse: bool = False,
    equivalences: Optional[EquivalenceArg] = None,
    target: Optional[str] = None,
) -> Column:
    """
    Takes a Coding column as input.  Returns the Column which contains an array of
    Coding value with translation targets from the specified FHIR ConceptMap. There
    may be more than one target concept for each input concept. Only the translation with
    the specified equivalences are returned.
    See also :class:`Equivalence`.
    :param coding: a Column containing a struct representation of a Coding
    :param concept_map_uri: an identifier for a FHIR ConceptMap
    :param reverse: the direction to traverse the map - false results in "source to target"
           mappings, while true results in "target to source"
    :param equivalences: a value of a collection of values from the ConceptMapEquivalence ValueSet
    :param target: identifies the value set in which a translation is sought.  If there's no
           target specified, the server should return all known translations.
    :return: a Column containing the result of the operation (an array of Coding structs).
    """
    return _invoke_udf(
        "translate",
        _coding_to_java_column(coding),
        concept_map_uri,
        reverse,
        _invoke_support_function(
            "equivalenceCodesToEnum", _ensure_collection(equivalences)
        ),
        target,
    )


def subsumes(left_coding: CodingArg, right_coding: CodingArg) -> Column:
    """
    Takes two Coding columns as input. Returns the Column, which contains a
        Boolean value, indicating whether the left Coding subsumes the right Coding.

    :param left_coding: a Column containing a struct representation of a Coding or an array of
           Codings.
    :param right_coding: a Column containing a struct representation of a Coding or an array of
           Codings.
    :return: a Column containing the result of the operation (boolean).
    """
    return _invoke_udf(
        "subsumes",
        _coding_to_java_column(left_coding),
        _coding_to_java_column(right_coding),
    )


def subsumed_by(left_coding: CodingArg, right_coding: CodingArg) -> Column:
    """
    Takes two Coding columns as input. Returns the Column, which contains a
    Boolean value, indicating whether the left Coding is subsumed by the right Coding.

    :param left_coding: a Column containing a struct representation of a Coding or an array of
           Codings.
    :param right_coding: a Column containing a struct representation of a Coding or an array of
           Codings.
    :return: a Column containing the result of the operation (boolean).
    """
    return _invoke_udf(
        "subsumed_by",
        _coding_to_java_column(left_coding),
        _coding_to_java_column(right_coding),
    )


def display(coding: CodingArg, accept_language: Optional[str] = None) -> Column:
    """
    Takes a Coding column as its input. Returns the Column, which contains the canonical display
    name associated with the given code.

    :param coding: a Column containing a struct representation of a Coding.
    :param accept_language: the optional language preferences for the returned display name.
            Overrides the parameter `accept_language` in
            :func:`PathlingContext.create <pathling.PathlingContext.create>`.
    :return: a Column containing the result of the operation (String).
    """
    return _invoke_udf("display", _coding_to_java_column(coding), accept_language)


def property_of(
    coding: CodingArg,
    property_code: str,
    property_type: str = PropertyType.STRING,
    accept_language: Optional[str] = None,
) -> Column:
    """
    Takes a Coding column as its input. Returns the Column, which contains the values of properties
    for this coding with specified names and types. The type of the result column depends on the
    types of the properties. Primitive FHIR types are mapped to their corresponding SQL primitives.
    Complex types are mapped to their corresponding structs. The allowed property types are: code |
    Coding | string | integer | boolean | dateTime | decimal.
    See also :class:`PropertyType`.

    :param coding: a Column containing a struct representation of a Coding
    :param property_code: the code of the property to retrieve.
    :param property_type: the type of the property to retrieve.
    :param accept_language: the optional language preferences for the returned property values.
            Overrides the parameter `accept_language` in
            :func:`PathlingContext.create <pathling.PathlingContext.create>`.
    :return: the Column containing the result of the operation (array of property values)
    """
    FHIRDefinedType_property_type = (
        _get_jvm().org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.fromCode(
            property_type
        )
    )
    return _invoke_udf(
        "property_of",
        _coding_to_java_column(coding),
        property_code,
        FHIRDefinedType_property_type,
        accept_language,
    )


def designation(
    coding: CodingArg, use: Optional[CodingArg] = None, language: Optional[str] = None
) -> Column:
    """
    Takes a Coding column as its input. Returns the Column, which contains the values of
    designations (strings) for this coding for the specified use and language. If the language is
    not provided (is null) then all designations with the specified type are returned regardless of
    their language.

    :param coding: a Column containing a struct representation of a Coding
    :param use: the code with the use of the designations
    :param language: the language of the designations
    :return: the Column containing the result of the operation (array of strings with designation
             values)
    """
    return _invoke_udf(
        "designation",
        _coding_to_java_column(coding),
        _coding_to_java_column(use),
        language,
    )
