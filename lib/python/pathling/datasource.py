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


from typing import Dict, Sequence, Optional, Callable

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame, SparkSession

from pathling import PathlingContext
from pathling.core import ExpOrStr
from pathling.core import SparkConversionsMixin
from pathling.fhir import MimeType


class DataSource(SparkConversionsMixin):
    """
    A data source that can be used to run queries against FHIR data.
    """

    def __init__(self, jds: JavaObject, spark: SparkSession):
        SparkConversionsMixin.__init__(self, spark)
        self._jds = jds
        self._spark = spark

    def extract(
        self,
        resource_type: str,
        columns: Sequence[ExpOrStr],
        filters: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """
        Runs an extract query for the given resource type, using the specified columns and filters
        to create a tabular extract from FHIR data.

        For more information see: :class:`ExtractQuery`

        :param resource_type: A string representing the type of FHIR resource to extract data from.
        :param columns: A sequence of FHIRPath expressions that define the columns to include in the
               extract.
        :param filters: An optional sequence of FHIRPath expressions that can be evaluated against
               each resource in the data set to determine whether it is included within the result.
               The expression must evaluate to a Boolean value. Multiple filters are combined using
               AND logic.

        :return: A Spark DataFrame containing the results of the extract query.
        """
        from pathling.query import ExtractQuery

        return ExtractQuery(resource_type, columns, filters).execute(self)

    def aggregate(
        self,
        resource_type: str,
        aggregations: Sequence[ExpOrStr],
        groupings: Optional[Sequence[ExpOrStr]] = None,
        filters: Optional[Sequence[str]] = None,
    ) -> DataFrame:
        """
        Runs an aggregate query for the given resource type, using the specified aggregation,
        grouping, and filter expressions. The context for each of the expressions is a collection
        of resources of the subject resource type.

        For more information see: :class:`AggregateQuery`

        :param resource_type: A string representing the type of FHIR resource to aggregate data
               from.
        :param aggregations: A sequence of FHIRPath expressions that calculate a summary value from
               each grouping. The expressions must be singular.
        :param groupings: An optional sequence of FHIRPath expressions that determine which
               groupings the resources should be counted within.
        :param filters: An optional sequence of FHIRPath expressions that determine whether
               a resource is included in the result. The expressions must evaluate to a Boolean
               value. Multiple filters are combined using AND logic.
        :return: A Spark DataFrame object containing the results of the aggregate query.
        """
        from pathling.query import AggregateQuery

        return AggregateQuery(resource_type, aggregations, groupings, filters).execute(
            self
        )


class DataSources(SparkConversionsMixin):
    """
    A factory for creating data sources.
    """

    def __init__(self, pc: PathlingContext):
        SparkConversionsMixin.__init__(self, pc.spark)
        self._pc = pc
        self._jdataSources = pc._jpc.datasources()

    def _wrap_ds(self, jds: JavaObject) -> DataSource:
        return DataSource(jds, self.spark)

    def with_resources(self, resources: Dict[str, DataFrame]) -> DataSource:
        """
        Creates an immutable, ad-hoc data source from a dictionary of Spark DataFrames indexed with
        resource type codes.

        :param resources: A dictionary of Spark DataFrames, where the keys are resource type codes
               and the values are the data frames containing the resource data.
        :return: A DataSource object that can be used to run queries against the data.
        """
        jbuilder = self._jdataSources.directBuilder()
        for resource_code, resource_data in resources.items():
            jbuilder.withResource(resource_code, resource_data._jdf)
        return self._wrap_ds(jbuilder.build())

    def from_text_files(
        self,
        files_glob: str,
        filename_mapper: Callable[[str], Sequence[str]],
        mime_type: str = None,
    ) -> DataSource:
        """
        Creates a data source from a set of text files matching the glob expression, that contain
        FHIR resources encoded as text (e.g. JSON or XML), one resource per line.

        To optimize the encoding process, filename mapping function helps to determine which files
        contain which resource types.

        :param files_glob: A glob expression that defines the files to be included in the
               datasource.
        :param filename_mapper: A function that maps a filename to a list of resource type codes
              it contains.
        :param mime_type:  The MIME type of the text file encoding. Defaults to
               `application/fhir+json`.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(
            self._jdataSources.fromTextFiles(
                files_glob,
                self._lambda_to_function(filename_mapper),
                mime_type or MimeType.FHIR_JSON,
            )
        )

    def from_ndjson_dir(self, ndjson_dir_uri) -> DataSource:
        """
        Creates a data source from a directory containing NDJSON files. The files must be named with
        the resource type code and must have the ".ndjson" extension, e.g. "Patient.ndjson"
        or "Observation.ndjson".

        :param ndjson_dir_uri: The URI of the directory containing the NDJSON files.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(self._jdataSources.fromNdjsonDir(ndjson_dir_uri))

    def from_warehouse(
        self, warehouse_dir_uri: str, database_name="default"
    ) -> DataSource:
        """
        Creates a data source from a warehouse directory.

        :param warehouse_dir_uri: The URI of the warehouse directory.
        :param database_name: The name of the database to use. Defaults to "default".
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(
            self._jdataSources.fromWarehouse(warehouse_dir_uri, database_name)
        )
