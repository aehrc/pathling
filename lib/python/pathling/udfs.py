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
              reverse: bool = False, equivalences: Optional[str] = None) -> Column:
    """
    Takes a Coding column as input.  Returns the Column which contains an array of 
    Coding value with translation targets from the specified FHIR ConceptMap. There 
    may be more than one target concept for each input concept.
    
    :param coding: a Column containing a struct representation of a Coding
    :param concept_map_uri: an identifier for a FHIR ConceptMap
    :param reverse: the direction to traverse the map - false results in "source to target" 
    mappings, while true results in "target to source"
    :param equivalences: a comma-delimited set of values from the ConceptMapEquivalence ValueSet
    :return: a Column containing the result of the operation (an array of Coding structs).
    """
    return _invoke_function("translate", _coding_to_java_column(coding), concept_map_uri, reverse,
                            equivalences)


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