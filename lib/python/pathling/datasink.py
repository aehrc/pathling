#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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


from typing import TYPE_CHECKING, Callable, Optional

from pathling.core import SparkConversionsMixin, StringMapper

if TYPE_CHECKING:
    from pathling.datasource import DataSource


class SaveMode:
    """
    Constants that represent the different save modes.

    OVERWRITE: Overwrite any existing data.
    APPEND: Append the new data to the existing data.
    IGNORE: Only save the data if the file does not already exist.
    ERROR: Raise an error if the file already exists.
    MERGE: Merge the new data with the existing data based on resource ID.
    """

    OVERWRITE: str = "overwrite"
    APPEND: str = "append"
    IGNORE: str = "ignore"
    ERROR: str = "error"
    MERGE: str = "merge"


class DataSinks(SparkConversionsMixin):
    """
    A class for writing FHIR data to a variety of different targets.
    """

    def __init__(self, datasource: "DataSource"):
        SparkConversionsMixin.__init__(self, datasource.spark)
        self._datasinks = (
            self.spark._jvm.au.csiro.pathling.library.io.sink.DataSinkBuilder(
                datasource.pc._jpc, datasource._jds
            )
        )

    def ndjson(
        self,
        path: str,
        save_mode: Optional[str] = SaveMode.ERROR,
        file_name_mapper: Callable[[str], str] = None,
    ) -> None:
        """
        Writes the data to a directory of NDJSON files. The files will be named using the resource
        type and the ".ndjson" extension.

        :param path: The URI of the directory to write the files to.
        :param save_mode: The save mode to use when writing the data:
            - "overwrite" will overwrite any existing data.
            - "append" will append the new data to the existing data.
            - "ignore" will only save the data if the file does not already exist.
            - "error" will raise an error if the file already exists.
        :param file_name_mapper: An optional function that can be used to customise the mapping of
        the resource type to the file name.
        """
        if file_name_mapper:
            wrapped_mapper = StringMapper(
                self.spark._jvm._gateway_client, file_name_mapper
            )
            self._datasinks.saveMode(save_mode).ndjson(path, wrapped_mapper)
        else:
            self._datasinks.saveMode(save_mode).ndjson(path)

    def parquet(self, path: str, save_mode: Optional[str] = SaveMode.ERROR) -> None:
        """
        Writes the data to a directory of Parquet files.

        :param path: The URI of the directory to write the files to.
        :param save_mode: The save mode to use when writing the data:
            - "overwrite" will overwrite any existing data.
            - "append" will append the new data to the existing data.
            - "ignore" will only save the data if the file does not already exist.
            - "error" will raise an error if the file already exists.
        """
        self._datasinks.saveMode(save_mode).parquet(path)

    def delta(
        self,
        path: str,
        save_mode: Optional[str] = SaveMode.OVERWRITE,
        delete_on_merge: bool = False,
    ) -> None:
        """
        Writes the data to a directory of Delta files.

        :param path: The URI of the directory to write the files to.
        :param save_mode: The save mode to use when writing the data - "overwrite" will
        overwrite any existing data, "merge" will merge the new data with the existing data based
        on resource ID.
        :param delete_on_merge: If merging, whether to delete any resources not found in the source, but found in the destination.
        """
        self._datasinks.saveMode(save_mode).delta(path, delete_on_merge)

    def tables(
        self,
        schema: Optional[str] = None,
        save_mode: Optional[str] = SaveMode.OVERWRITE,
        table_format: Optional[str] = None,
        delete_on_merge: bool = False,
    ) -> None:
        """
        Writes the data to a set of tables in the Spark catalog.

        :param schema: The name of the schema to write the tables to. Required when table_format is specified.
        :param save_mode: The save mode to use when writing the data - "overwrite" will
        overwrite any existing data, "merge" will merge the new data with the existing data based
        on resource ID.
        :param table_format: The table format to use (e.g., "delta", "parquet"). When specified, schema must also be provided.
        If not specified, the default catalog table format will be used.
        :param delete_on_merge: If merging, whether to delete any resources not found in the source, but found in the destination.
        Only applies when save_mode is "merge" and table_format is "delta".
        """
        if table_format:
            if not schema:
                raise ValueError("schema must be provided when table_format is specified")
            self._datasinks.saveMode(save_mode).tables(schema, table_format, delete_on_merge)
        elif schema:
            self._datasinks.saveMode(save_mode).tables(schema)
        else:
            self._datasinks.saveMode(save_mode).tables()
