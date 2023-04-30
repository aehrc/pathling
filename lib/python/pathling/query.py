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

from typing import Optional, Sequence, Tuple

from py4j.java_gateway import JavaObject

from pathling.core import ExpOrStr, Expression
from pathling.datasource import DataSource


class QueryWithFilters:
    """
    Represents a query that can be executed against a data source, with the capability to filter
    the resources included in the result and define the subject resource type for all FHIRPath
    expressions in the query.

    .. note::
        This class is meant to be subclassed with an implementation of the `_create_jquery` method.
    """

    _subject_resource: str
    _filters: Optional[Tuple[str]]
    _data_source: Optional[DataSource]

    def __init__(
        self,
        subject_resource: str,
        filters: Optional[Sequence[str]],
        data_source: DataSource = None,
    ):
        """
        Initializes a new instance of the QueryWithFilters class.

        :param subject_resource: The FHIR resource type to use as the subject for the query.
        :param filters: An optional sequence of FHIRPath expressions that determine whether
               a resource is included in the result. The expressions must evaluate to a Boolean
               value. Multiple filters are combined using AND logic.
        :param data_source: The data source to execute the query against. If not provided, the query
               can only be executed by calling the `execute` method with a data source argument.
        """
        self._subject_resource = subject_resource
        self._filters = tuple(filters) if filters else None
        self._data_source = data_source

    @property
    def subject_resource(self) -> str:
        """
        The FHIR resource type to use as the subject for the query.
        """
        return self._subject_resource

    @property
    def filters(self) -> Optional[Sequence[str]]:
        """
        An optional sequence of FHIRPath expressions that determine whether a resource is included
        in the result.
        """
        return self._filters

    def execute(self, data_source: DataSource = None):
        """
        Execute the query against a data source.

        :param data_source: The data source to execute the query against. If not provided, the query
               will use the data source provided to the constructor.
        :type data_source: Optional[DataSource]

        :returns: A Spark DataFrame containing the query results.
        """
        resolved_data_source = data_source or self._data_source
        if not resolved_data_source:
            raise ValueError("A data source is required to execute the query.")
        jquery = self._create_jquery(resolved_data_source)
        return resolved_data_source._wrap_df(jquery.execute())

    def _create_jquery(self, data_source: DataSource) -> JavaObject:
        """
        This method should be implemented by a subclass to create a Java query object that can be
        executed against the data source.

        :param data_source: The data source to execute the query against.

        :returns: A java query object.
        """
        raise NotImplementedError("Must be implemented by subclass")


class ExtractQuery(QueryWithFilters):
    """
    Represents an extract query that extracts specified columns from FHIR resources and applies
    optional filters.

    :param subject_resource: A string representing the type of FHIR resource to extract data from.
    :param columns: A sequence of FHIRPath expressions that define the columns to include in the
           extract.
    :param filters: An optional sequence of FHIRPath expressions that can be evaluated against
           each resource in the data set to determine whether it is included within the result.
           The expression must evaluate to a Boolean value. Multiple filters are combined using AND
           logic.
    :param data_source: An optional DataSource instance to use for executing the query.
    """

    def __init__(
        self,
        subject_resource: str,
        columns: Sequence[ExpOrStr],
        filters: Optional[Sequence[str]],
        data_source: DataSource = None,
    ):
        """
        Initializes a new instance of the ExtractQuery class.

        :param subject_resource: A string representing the type of FHIR resource to extract data
               from.
        :param columns: A sequence of FHIRPath expressions that define the columns to include in the
               extract.
        :param filters: An optional sequence of FHIRPath expressions that can be evaluated against
               each resource in the data set to determine whether it is included within the result.
               The expression must evaluate to a Boolean value. Multiple filters are combined using
               AND logic.
        :param data_source: An optional DataSource instance to use for executing the query.
        """
        super().__init__(subject_resource, filters, data_source)
        self._columns = Expression.as_expression_sequence(columns)

    @property
    def columns(self) -> Sequence[Expression]:
        """
        Gets the columns to extract.

        :return: A sequence of Expression objects representing the columns to extract.
        """
        return self._columns

    def _create_jquery(self, data_source: DataSource) -> JavaObject:
        """
        Creates a new instance of a Java-based extract query object.

        :param data_source: The DataSource instance to use for executing the query.
        :return: A new instance of a Java-based extract query object.
        """
        jquery = data_source._jds.extract(self._subject_resource)
        for column in self._columns:
            jquery.column(*column.as_tuple())
        if self._filters:
            for f in self._filters:
                jquery.filter(f)
        return jquery


class AggregateQuery(QueryWithFilters):
    """
    Represents an aggregate query for FHIR data. The query calculates summary values based
    on aggregations and groupings of FHIR resources.

    :param subject_resource: A string representing the type of FHIR resource to aggregate data from.
    :param aggregations: A sequence of FHIRPath expressions that calculate a summary value from
           each grouping. The expressions must be singular.
    :param groupings: An optional sequence of FHIRPath expressions that determine which groupings
           the resources should be counted within.
    :param filters: An optional sequence of FHIRPath expressions that determine whether
           a resource is included in the result. The expressions must evaluate to a Boolean value.
           Multiple filters are combined using AND logic.
    :param data_source: The DataSource object containing the data to be queried.
    """

    def __init__(
        self,
        subject_resource: str,
        aggregations: Sequence[ExpOrStr],
        groupings: Optional[Sequence[ExpOrStr]] = None,
        filters: Optional[Sequence[str]] = None,
        data_source: DataSource = None,
    ):
        """
        Creates a new AggregateQuery object.

        :param subject_resource: A string representing the type of FHIR resource to aggregate data
               from.
        :param aggregations: A sequence of FHIRPath expressions that calculate a summary value from
               each grouping. The expressions must be singular.
        :param groupings: An optional sequence of FHIRPath expressions that determine which
               groupings the resources should be counted within.
        :param filters: An optional sequence of FHIRPath expressions that determine whether
               a resource is included in the result. The expressions must evaluate to a Boolean
               value. Multiple filters are combined using AND logic.
        :param data_source: The DataSource object containing the data to be queried.
        """
        super().__init__(subject_resource, filters, data_source)
        self._aggregations = Expression.as_expression_sequence(aggregations)
        self._groupings = (
            Expression.as_expression_sequence(groupings) if groupings else None
        )

    @property
    def aggregations(self) -> Sequence[Expression]:
        """
        The sequence of FHIRPath expressions that calculate a summary value from each grouping.

        :return: A sequence of Expression objects representing the aggregations for the query.
        """
        return self._aggregations

    @property
    def groupings(self) -> Optional[Sequence[Expression]]:
        """
        The optional sequence of FHIRPath expressions that determine which groupings the resources
        should be counted within.

        :return: A sequence of Expression objects representing the groupings for the query.
        """
        return self._groupings

    def _create_jquery(self, data_source: DataSource) -> JavaObject:
        """
        Internal method that creates a new Java AggregateQuery object with the appropriate
        aggregations, groupings, and filters.

        :param data_source: The DataSource object containing the data to be queried.
        :return: A new Java AggregateQuery object with the appropriate parameters.
        """
        jquery = data_source._jds.aggregate(self._subject_resource)
        for aggregation in self._aggregations:
            jquery.aggregation(*aggregation.as_tuple())

        if self._groupings:
            for grouping in self._groupings:
                jquery.grouping(*grouping.as_tuple())
        if self._filters:
            for f in self._filters:
                jquery.filter(f)
        return jquery
