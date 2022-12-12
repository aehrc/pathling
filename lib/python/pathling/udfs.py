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

from typing import (
    Any,
    Callable,
    Optional,
    Union
)

from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column
from pathling.coding import Coding

CodingArg = Union[Column, str, Coding]


def _coding_to_java_column(coding: CodingArg) -> Any:
    return _to_java_column(coding.to_literal() if Coding == type(coding) else coding)


def _get_jvm_function(name: str, sc: SparkContext) -> Callable:
    """
    Retrieves JVM function identified by name from
    Java gateway associated with sc.
    """
    assert sc._jvm is not None
    return getattr(sc._jvm.au.csiro.pathling.sql.Terminology, name)


def _invoke_function(name: str, *args: Any) -> Column:
    """
    Invokes JVM function identified by name with args
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    assert SparkContext._active_spark_context is not None
    jf = _get_jvm_function(name, SparkContext._active_spark_context)
    return Column(jf(*args))


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
    return _invoke_function("member_of", _coding_to_java_column(coding), value_set_uri)


def translate(coding: CodingArg, concept_map_uri: str,
              reverse: bool = False, equivalences: Optional[str] = None,
              target: Optional[str] = None) -> Column:
    """
    Takes a Coding column as input.  Returns the Column which contains an array of 
    Coding value with translation targets from the specified FHIR ConceptMap. There 
    may be more than one target concept for each input concept.
    
    :param coding: a Column containing a struct representation of a Coding
    :param concept_map_uri: an identifier for a FHIR ConceptMap
    :param reverse: the direction to traverse the map - false results in "source to target" 
        mappings, while true results in "target to source"
    :param equivalences: a comma-delimited set of values from the ConceptMapEquivalence ValueSet
    :param target: identifies the value set in which a translation is sought.  If there's no 
        target specified, the server should return all known translations.
    :return: a Column containing the result of the operation (an array of Coding structs).
    """
    return _invoke_function("translate", _coding_to_java_column(coding), concept_map_uri, reverse,
                            equivalences, target)


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
    return _invoke_function("subsumes", _coding_to_java_column(left_coding),
                            _coding_to_java_column(right_coding))


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
    return _invoke_function("subsumed_by", _coding_to_java_column(left_coding),
                            _coding_to_java_column(right_coding))


def display(coding: CodingArg) -> Column:
    """
    Takes a Coding column as its input. Returns the Column, which contains the canonical display 
    name associated with the given code.    
    :param coding: a Column containing a struct representation of a Coding.
    :return: a Column containing the result of the operation (String).
    """
    return _invoke_function("display", _coding_to_java_column(coding))


def property_of(coding: CodingArg, property_code: str,
                property_type: str = PropertyType.STRING) -> Column:
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
    :return: the Column containing the result of the operation (array of property values)
    """
    return _invoke_function("property_of", _coding_to_java_column(coding), property_code,
                            property_type)


def designation(coding: CodingArg, use: CodingArg,
                language: Optional[str] = None) -> Column:
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
    return _invoke_function("designation", _coding_to_java_column(coding),
                            _coding_to_java_column(use),
                            language)
