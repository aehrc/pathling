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


from json import dumps, loads
from py4j.java_collections import SetConverter
from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame
from typing import Dict, Sequence, Optional, Callable

from pathling import PathlingContext
from pathling.core import (
    StringToStringSetMapper,
    SparkConversionsMixin,
)
from pathling.fhir import MimeType


class DataSource(SparkConversionsMixin):
    """
    A data source that can be used to run queries against FHIR data.
    """

    def __init__(self, jds: JavaObject, pc: PathlingContext):
        SparkConversionsMixin.__init__(self, pc.spark)
        self._jds = jds
        self.pc = pc

    def read(self, resource_code: str) -> DataFrame:
        """
        Reads the data for the given resource type from the data source.

        :param resource_code: A string representing the type of FHIR resource to read data from.

        :return: A Spark DataFrame containing the data for the given resource type.
        """
        return self._wrap_df(self._jds.read(resource_code))

    def resource_types(self):
        """
        Returns a list of the resource types that are available in the data source.
        
        :return: A list of strings representing the resource types.
        """
        return [r.toCode() for r in self._jds.getResourceTypes()]

    @property
    def write(self) -> "DataSinks":
        """
        Provides access to a :class:`DataSinks` object that can be used to persist data.
        """
        from pathling.datasink import DataSinks

        return DataSinks(self)

    def view(
        self,
        resource: Optional[str] = None,
        select: Optional[Sequence[Dict]] = None,
        constants: Optional[Sequence[Dict]] = None,
        where: Optional[Sequence[Dict]] = None,
        json: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes a SQL on FHIR view definition and returns the result as a Spark DataFrame.

        :param resource: The FHIR resource that the view is based upon, e.g. 'Patient' or
               'Observation'.
        :param select: A list of columns and nested selects to include in the view.
        :param constants: A list of constants that can be used in FHIRPath expressions.
        :param where: A list of FHIRPath expressions that can be used to filter the view.
        :param json: A JSON string representing the view definition, as an alternative to providing
               the parameters as Python objects.
        :return: A Spark DataFrame containing the results of the view.
        """
        if json:
            query_json = json
            parsed = loads(json)
            resource = parsed.get("resource")
        else:
            args = locals()
            query = {
                key: args[key]
                for key in ["resource", "select", "constants", "where"]
                if args[key] is not None
            }
            query_json = dumps(query)
        jquery = self._jds.view(resource)
        jquery.json(query_json)
        return self._wrap_df(jquery.execute())


class DataSources(SparkConversionsMixin):
    """
    A factory for creating data sources.
    """

    def __init__(self, pathling: PathlingContext):
        SparkConversionsMixin.__init__(self, pathling.spark)
        self._pc = pathling
        self._jdataSources = pathling._jpc.read()

    def _wrap_ds(self, jds: JavaObject) -> DataSource:
        return DataSource(jds, self._pc)

    def ndjson(
        self,
        path,
        extension: Optional[str] = "ndjson",
        file_name_mapper: Callable[[str], Sequence[str]] = None,
    ) -> DataSource:
        """
        Creates a data source from a directory containing NDJSON files. The files must be named with
        the resource type code and must have the ".ndjson" extension, e.g. "Patient.ndjson"
        or "Observation.ndjson".

        :param path: The URI of the directory containing the NDJSON files.
        :param extension: The file extension to use when searching for files. Defaults to "ndjson".
        :param file_name_mapper: An optional function that maps a filename to the set of resource
               types that it contains.
        :return: A DataSource object that can be used to run queries against the data.
        """
        if file_name_mapper:
            wrapped_mapper = StringToStringSetMapper(
                self.spark._jvm._gateway_client, file_name_mapper
            )
            return self._wrap_ds(
                self._jdataSources.ndjson(path, extension, wrapped_mapper)
            )
        else:
            return self._wrap_ds(self._jdataSources.ndjson(path, extension))

    def bundles(
        self,
        path: str,
        resource_types: Sequence[str],
        mime_type: str = MimeType.FHIR_JSON,
    ) -> DataSource:
        """
        Creates a data source from a directory containing FHIR bundles.

        :param path: The URI of the directory containing the bundles.
        :param resource_types: A sequence of resource type codes that should be extracted from the
               bundles.
        :param mime_type: The MIME type of the bundles. Defaults to `application/fhir+json`.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(
            self._jdataSources.bundles(
                path,
                SetConverter().convert(resource_types, self.spark._jvm._gateway_client),
                mime_type,
            )
        )

    def datasets(self, resources: Dict[str, DataFrame]) -> DataSource:
        """
        Creates an immutable, ad-hoc data source from a dictionary of Spark DataFrames indexed with
        resource type codes.

        :param resources: A dictionary of Spark DataFrames, where the keys are resource type codes
               and the values are the data frames containing the resource data.
        :return: A DataSource object that can be used to run queries against the data.
        """
        jbuilder = self._jdataSources.datasets()
        for resource_code, resource_data in resources.items():
            jbuilder.dataset(resource_code, resource_data._jdf)
        return self._wrap_ds(jbuilder)

    def parquet(self, path: str) -> DataSource:
        """
        Creates a data source from a directory containing Parquet tables. Each table must be named
        according to the name of the resource type that it stores.

        :param path: The URI of the directory containing the Parquet tables.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(self._jdataSources.parquet(path))

    def delta(self, path: str) -> DataSource:
        """
        Creates a data source from a directory containing Delta tables, as used by Pathling Server
        for persistence. Each table must be named according to the name of the resource type that
        it stores.

        :param path: The URI of the directory containing the Delta tables.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(self._jdataSources.delta(path))

    def tables(
        self,
        schema: Optional[str] = None,
    ) -> DataSource:
        """
        Creates a data source from a set of Spark tables, where the table names are the resource
        type codes.

        :param schema: An optional schema name that should be used to qualify the table names.
        :return: A DataSource object that can be used to run queries against the data.
        """
        if schema:
            return self._wrap_ds(self._jdataSources.tables(schema))
        else:
            return self._wrap_ds(self._jdataSources.tables())
