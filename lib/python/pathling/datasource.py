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


from typing import Dict, Sequence, Optional, Callable, TYPE_CHECKING

from py4j.java_collections import SetConverter
from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame

from pathling import PathlingContext
from pathling.core import ExpOrStr, StringMapper, StringToStringListMapper
from pathling.core import SparkConversionsMixin
from pathling.fhir import MimeType

if TYPE_CHECKING:
    from .datasink import DataSinks


class DataSource(SparkConversionsMixin):
    """
    A data source that can be used to run queries against FHIR data.
    """

    def __init__(self, jds: JavaObject, pc: PathlingContext):
        SparkConversionsMixin.__init__(self, pc.spark)
        self._jds = jds
        self.pc = pc

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

    @property
    def write(self) -> "DataSinks":
        """
        Provides access to a :class:`DataSinks` object that can be used to persist data.
        """
        from pathling.datasink import DataSinks

        return DataSinks(self)


class DataSources(SparkConversionsMixin):
    """
    A factory for creating data sources.
    """

    def __init__(self, pc: PathlingContext):
        SparkConversionsMixin.__init__(self, pc.spark)
        self._pc = pc
        self._jdataSources = pc._jpc.datasources()

    def _wrap_ds(self, jds: JavaObject) -> DataSource:
        return DataSource(jds, self._pc)

    def from_datasets(self, resources: Dict[str, DataFrame]) -> DataSource:
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

    def from_ndjson_dir(
        self, ndjson_dir_uri, filename_mapper: Callable[[str], Sequence[str]] = None
    ) -> DataSource:
        """
        Creates a data source from a directory containing NDJSON files. The files must be named with
        the resource type code and must have the ".ndjson" extension, e.g. "Patient.ndjson"
        or "Observation.ndjson".

        :param ndjson_dir_uri: The URI of the directory containing the NDJSON files.
        :param filename_mapper: An optional function that maps a filename to the list of resource
               types that it contains.
        :return: A DataSource object that can be used to run queries against the data.
        """
        if filename_mapper:
            files_glob = (
                self.spark._jvm.au.csiro.pathling.utilities.Strings.safelyJoinPaths(
                    ndjson_dir_uri, "*.ndjson"
                )
            )
            wrapped_mapper = StringToStringListMapper(
                self.spark._jvm._gateway_client, filename_mapper
            )
            return self._wrap_ds(
                self._jdataSources.fromTextFiles(
                    files_glob, wrapped_mapper, MimeType.FHIR_JSON
                )
            )
        else:
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

    def from_bundles(
        self,
        bundles_dir_uri: str,
        resource_types: Sequence[str],
        mime_type: str = MimeType.FHIR_JSON,
    ) -> DataSource:
        """
        Creates a data source from a directory containing FHIR bundles.

        :param bundles_dir_uri: The URI of the directory containing the bundles.
        :param resource_types: A sequence of resource type codes that should be extracted from the
               bundles.
        :param mime_type: The MIME type of the bundles. Defaults to `application/fhir+json`.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(
            self._jdataSources.fromBundlesDir(
                bundles_dir_uri,
                SetConverter().convert(resource_types, self.spark._jvm._gateway_client),
                mime_type,
            )
        )

    def from_tables(
        self,
        resource_types: Sequence[str],
        table_name_mapper: Callable[[str], str] = None,
    ) -> DataSource:
        """
        Creates a data source from a set of Spark tables, where the table names are the resource
        type codes.

        :param resource_types: A sequence of resource type codes that should be extracted from the
               tables.
        :return: A DataSource object that can be used to run queries against the data.
        """
        if table_name_mapper:
            resource_types_enum = SetConverter().convert(
                map(
                    lambda resource_type: self.spark._jvm.org.hl7.fhir.r4.model.Enumerations.ResourceType.fromCode(
                        resource_type
                    ),
                    resource_types,
                ),
                self.spark._jvm._gateway_client,
            )
            wrapped_mapper = StringMapper(
                self.spark._jvm._gateway_client, table_name_mapper
            )
            return (
                self._jdataSources.catalogSourceBuilder(resource_types_enum)
                .withTableNameMapper(wrapped_mapper)
                .build()
            )
        else:
            return self._wrap_ds(
                self._jdataSources.fromTables(
                    SetConverter().convert(
                        resource_types, self.spark._jvm._gateway_client
                    )
                )
            )
