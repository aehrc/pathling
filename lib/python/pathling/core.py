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

from typing import Any, Callable, Sequence, Tuple, Optional, Union

from py4j.java_collections import SetConverter
from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession


class Function:
    """
    Wraps a Python lambda function so that it can be passed to Java functions
    that expect a `java.util.function.Function` object.

    :param lambda_function: A Python lambda function that takes one argument.
    :param spark: A `pyspark.sql.SparkSession` object.
    """

    def __init__(self, lambda_function, spark):
        self._lambda_function = lambda_function
        self._jvm = spark._jvm

    def _wrap_result(self, result: Any) -> Any:
        # we need to manually convert the python object result to the java object
        # result, so that it can be returned to the java side and py4j does not seem to be doing
        # it automatically. So we just call the identity() to force the conversion.
        return self._jvm.java.util.function.Function.identity().apply(result)

    def apply(self, arg):
        """
        Invokes the wrapped lambda function with the given argument.

        :param arg: The argument to pass to the lambda function.
        :return: The result of the lambda function, converted to a Java object.
        """
        return self._wrap_result(self._lambda_function(arg))

    class Java:
        implements = ["java.util.function.Function"]


class SparkConversionsMixin:
    """
    A mixin that provides access to the Spark session and a number for utility methods for
    converting between Python and Java objects.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def _wrap_df(self, jdf: JavaObject) -> DataFrame:
        #
        # Before Spark v3.3 Dataframes were constructs with SQLContext, which was available
        # in `_wrapped` attribute of SparkSession.
        # Since v3.3 Dataframes are constructed with SparkSession instance directly.
        #
        return DataFrame(
            jdf,
            self._spark._wrapped if hasattr(self._spark, "_wrapped") else self._spark,
        )

    def _lambda_to_function(self, lambda_function: Callable) -> Function:
        return Function(lambda_function, self._spark)


ExpOrStr = Union["Expression", str]


class Expression:
    """
    Represents an FHIRPath expression that may have an optional name/alias.
    To make it easier to work with expressions, uses can alias this class with their own name,
    for example: 'exp' or 'fp' using import and then use the alias method to create labeled
    expressions. For example:

    ```
    from pathling import Expression as fp
    fp('some FHIRPath expression').alias('some name')
    ```
    """

    def __init__(self, expression: str, label: Optional[str] = None):
        """
        Initializes a new instance of the Expression class.

        :param expression: The FHIRPath expression.
        :param label: The optional label/alias for the expression.
        """
        self._expression = expression
        self._label = label

    @property
    def expression(self) -> str:
        """
        Gets the FHIRPath expression.

        :return: The FHIRPath expression.
        """
        return self._expression

    @property
    def label(self) -> Optional[str]:
        """
        Gets the optional label/alias for the expression.

        :return: The optional label/alias for the expression.
        """
        return self._label

    def alias(self, label: str) -> "Expression":
        """
        Creates a new Expression object with the specified label/alias.

        :param label: The label/alias to use for the new Expression object.
        :return: A new Expression object with the specified label/alias.
        """
        return Expression(self.expression, label)

    def as_tuple(self) -> Tuple:
        """
        Gets a tuple representing the expression and its optional label/alias.

        :return: A tuple representing the expression and its optional label/alias.
        """
        return (self.expression, self.label) if self.label else (self.expression,)

    @classmethod
    def as_expression(cls, exp_or_str: ExpOrStr) -> "Expression":
        """
        Casts the specified expression or string into an Expression object.

        :param exp_or_str: The expression or string to cast.
        :return: An Expression object.
        """
        if isinstance(exp_or_str, Expression):
            return exp_or_str
        return Expression(exp_or_str)

    @classmethod
    def as_expression_sequence(
        cls, sequence_of_exp_or_str: Sequence[ExpOrStr]
    ) -> "Sequence[Expression]":
        """
        Cast a sequence of expressions or strings into a sequence of Expression objects.

        :param sequence_of_exp_or_str: The sequence of expressions or strings to cast.
        :return: A sequence of Expression objects.
        """
        return tuple(
            cls.as_expression(exp_or_str) for exp_or_str in sequence_of_exp_or_str
        )


class StringMapper:
    """
    A wrapper for a Python lambda that can be passed as a Java lambda for mapping a string value to
    another string value.
    """

    def __init__(self, gateway, fn):
        self._gateway = gateway
        self._fn = fn

    def apply(self, arg):
        return self._fn(arg)

    class Java:
        implements = ["java.util.function.UnaryOperator"]


class StringToStringSetMapper:
    """
    A wrapper for a Python lambda that can be passed as a Java lambda for mapping a string value
    to a list of string values.
    """

    def __init__(self, gateway, fn):
        self._gateway = gateway
        self._fn = fn

    def apply(self, arg):
        return SetConverter().convert(self._fn(arg), self._gateway)

    class Java:
        implements = ["java.util.function.Function"]
