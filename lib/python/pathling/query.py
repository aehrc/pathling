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
from typing import Optional, Sequence

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession




class Expression:
  def __init__(self, expression):
    self.expression = expression

  def alias(self, label:str):
    return (self.expression, label)


def exp(expression: str) -> Expression:
  return Expression(expression)


class SparkAware:
    """
    A mixin that provides access to the Spark session
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

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


class ExtractQuery(SparkAware):
    """
    A query that extracts tabular data from a fhir resources.
    """

    def __init__(self, jquery: JavaObject, spark: SparkSession):
        SparkAware.__init__(self, spark)
        self._jquery = jquery

    def with_column(
        self, column_expression: str, column_name: Optional[str] = None
    ) -> "ExtractQuery":
        """Adds a column to the query.
        :param column_expression:
        :param column_name:
        :return:
        """
        if column_name is not None:
            self._jquery.withColumn(column_expression, column_name)
        else:
            self._jquery.withColumn(column_expression)
        return self

    def with_filter(self, filter_expression: str) -> "ExtractQuery":
        """Adds a filter to the query.
        :param filter_expression:
        :return:
        """
        self._jquery.withFilter(filter_expression)
        return self

    def execute(self) -> DataFrame:
        """Executes the query.
        :return:
        """
        return self._wrap_df(self._jquery.execute())


class AggregateQuery(SparkAware):
    """
    A query that aggregates data from fhir resources
    """

    def __init__(self, jquery: JavaObject, spark: SparkSession):
        SparkAware.__init__(self, spark)
        self._jquery = jquery

    def with_grouping(
        self, grouping_expression: str, grouping_label: Optional[str] = None
    ) -> "AggregateQuery":
        """Adds a column to the query.
        :param grouping_expression:
        :param grouping_label:
        :return:
        """
        if grouping_label is not None:
            self._jquery.withGrouping(grouping_expression, grouping_label)
        else:
            self._jquery.withGrouping(grouping_expression)
        return self

    def with_aggregation(
        self, aggregation_expression: str, aggregation_label: Optional[str] = None
    ) -> "AggregateQuery":
        """Adds a column to the query.
        :param aggregation_expression:
        :param aggregation_label:
        :return:
        """
        if aggregation_label is not None:
            self._jquery.withAggregation(aggregation_expression, aggregation_label)
        else:
            self._jquery.withAggregation(aggregation_expression)
        return self

    def with_filter(self, filter_expression: str) -> "AggregateQuery":
        """Adds a filter to the query.
        :param filter_expression:
        :return:
        """
        self._jquery.withFilter(filter_expression)
        return self

    def execute(self) -> DataFrame:
        """Executes the query.
        :return:
        """
        return self._wrap_df(self._jquery.execute())


class PathlingClient:
    class Builder:
        def __init__(self, jbuilder: JavaObject, spark: SparkSession):
            self._jbuilder = jbuilder
            self._spark = spark

        def with_resource(
            self, resource_code: str, resource_data: DataFrame
        ) -> "PathlingClient.Builder":
            """Adds a resource to the client.
            :param resource_code:
            :param resource_data:
            """
            self._jbuilder.withResource(resource_code, resource_data._jdf)
            return self

        def build(self) -> "PathlingClient":
            return PathlingClient(self._jbuilder.build(), self._spark)

    def __init__(self, jclient: JavaObject, spark: SparkSession):
        self._jclient = jclient
        self._spark = spark

    def extract_query(self, resource_type: str) -> ExtractQuery:
        """Creates a new extract query.
        :param resource_type:
        :return:
        """
        return ExtractQuery(self._jclient.newExtractQuery(resource_type), self._spark)

    def aggregate_query(self, resource_type: str) -> AggregateQuery:
        """Creates a new aggregate query.
        :param resource_type:
        :return:
        """
        return AggregateQuery(
            self._jclient.newAggregateQuery(resource_type), self._spark
        )

    def extract(
        self,
        resource_type: str,
        columns: Sequence[str],
        filters: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """Runs an extract query for the given resource type.
        :param resource_type:
        :param columns:
        :param filters:
        :return:
        """
        query = self.extract_query(resource_type)
        for column in columns:
            query.with_column(*(column,) if isinstance(column, str) else column)
        if filters:
            for filter in filters:
                query.with_filter(filter)
        return query.execute()

    def aggregate(
        self,
        resource_type: str,
        aggregations: Sequence[str],
        groupings: Optional[Sequence[str]] = None,
        filters: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """Runs an aggregate query for the given resource type.
        :param resource_type:
        :param aggregations:
        :param groupings:
        :param filters:
        :return:
        """
        query = self.aggregate_query(resource_type)
        for aggregation in aggregations:
            query.with_aggregation(
                *(aggregation,) if isinstance(aggregation, str) else aggregation
            )
        if groupings:
            for grouping in groupings:
                query.with_grouping(
                    *(grouping,) if isinstance(grouping, str) else grouping
                )
        if filters:
            for filter in filters:
                query.with_filter(filter)
        return query.execute()
