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

from typing import Callable, Optional

from pathling.core import SparkConversionsMixin, StringMapper
from pathling.datasource import DataSource


class ImportMode:
    """
    Constants that represent the different import modes.
    """

    OVERWRITE: str = "overwrite"
    MERGE: str = "merge"


class DataSinks(SparkConversionsMixin):
    """
    A class for writing FHIR data to a variety of different targets.
    """

    def __init__(self, datasource: DataSource):
        SparkConversionsMixin.__init__(self, datasource.spark)
        self._datasinks = (
            self.spark._jvm.au.csiro.pathling.library.io.sink.DataSinkBuilder(
                datasource.pc._jpc, datasource._jds
            )
        )

    def ndjson(self, path: str, file_name_mapper: Callable[[str], str] = None) -> None:
        """
        Writes the data to a directory of NDJSON files. The files will be named using the resource
        type and the ".ndjson" extension.

        :param path: The URI of the directory to write the files to.
        :param file_name_mapper: An optional function that can be used to customise the mapping of
        the resource type to the file name.
        """
        if file_name_mapper:
            wrapped_mapper = StringMapper(
                self.spark._jvm._gateway_client, file_name_mapper
            )
            self._datasinks.ndjson(path, wrapped_mapper)
        else:
            self._datasinks.ndjson(path)

    def parquet(self, path: str) -> None:
        """
        Writes the data to a directory of Parquet files.

        :param path: The URI of the directory to write the files to.
        """
        self._datasinks.parquet(path)

    def delta(
        self, path: str, import_mode: Optional[str] = ImportMode.OVERWRITE
    ) -> None:
        """
        Writes the data to a directory of Delta files.

        :param path: The URI of the directory to write the files to.
        :param import_mode: The import mode to use when writing the data - "overwrite" will
        overwrite any existing data, "merge" will merge the new data with the existing data based
        on resource ID.
        """
        self._datasinks.delta(path, import_mode)

    def tables(
        self,
        schema: Optional[str] = None,
        import_mode: Optional[str] = ImportMode.OVERWRITE,
    ) -> None:
        """
        Writes the data to a set of tables in the Spark catalog.

        :param schema: The name of the schema to write the tables to.
        :param import_mode: The import mode to use when writing the data - "overwrite" will
        overwrite any existing data, "merge" will merge the new data with the existing data based
        on resource ID.
        """
        if schema:
            self._datasinks.tables(import_mode, schema)
        else:
            self._datasinks.tables(import_mode)
