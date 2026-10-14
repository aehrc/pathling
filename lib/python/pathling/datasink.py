#
# Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Optional

from pathling.core import SparkConversionsMixin, StringMapper

if TYPE_CHECKING:
    from pathling.datasource import DataSource


@dataclass
class FileInformation:
    """
    Information about a file created by a write operation.

    :param fhir_resource_type: The FHIR resource type code for this file.
    :param absolute_url: The absolute URL or path to the file.
    """

    fhir_resource_type: str
    absolute_url: str


@dataclass
class WriteDetails:
    """
    Details about files created or modified by a write operation.

    :param file_infos: A list of file information objects describing each file written.
    """

    file_infos: List[FileInformation]


def _convert_write_details(java_result) -> WriteDetails:
    """
    Convert a Java WriteDetails object to a Python WriteDetails dataclass.

    :param java_result: The Java WriteDetails object from the library API.
    :returns: A Python WriteDetails dataclass with the converted data.
    """
    java_file_infos = java_result.fileInfos()
    file_infos = [
        FileInformation(
            fhir_resource_type=fi.fhirResourceType(),
            absolute_url=fi.absoluteUrl(),
        )
        for fi in java_file_infos
    ]
    return WriteDetails(file_infos=file_infos)


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
    ) -> WriteDetails:
        """
        Writes the data to a directory of NDJSON files. Each resource type is written to one or
        more files named with the resource type, a partition number and an identifier of the
        write, for example "Patient.00000-<write id>.ndjson".

        :param path: The URI of the directory to write the files to.
        :param save_mode: The save mode to use when writing the data, applied to the files
            each resource type already has in the directory:
            - "overwrite" will replace any existing files of the resource type.
            - "append" will add files beside the existing files of the resource type.
            - "ignore" will only save the resource type if it has no files already.
            - "error" will raise an error if any resource type already has files.
        :param file_name_mapper: An optional function that can be used to customise the mapping of
        the resource type to the file name.
        :returns: Details about the files that were written.
        """
        if file_name_mapper:
            wrapped_mapper = StringMapper(
                self.spark._jvm._gateway_client, file_name_mapper
            )
            result = self._datasinks.saveMode(save_mode).ndjson(path, wrapped_mapper)
        else:
            result = self._datasinks.saveMode(save_mode).ndjson(path)
        return _convert_write_details(result)

    def parquet(
        self, path: str, save_mode: Optional[str] = SaveMode.ERROR
    ) -> WriteDetails:
        """
        Writes the data to a directory of Parquet files. Each resource type is written to one or
        more files named with the resource type, a partition number and an identifier of the
        write, for example "Patient.00000-<write id>.parquet".

        :param path: The URI of the directory to write the files to.
        :param save_mode: The save mode to use when writing the data, applied to the files
            each resource type already has in the directory:
            - "overwrite" will replace any existing files of the resource type.
            - "append" will add files beside the existing files of the resource type.
            - "ignore" will only save the resource type if it has no files already.
            - "error" will raise an error if any resource type already has files.
        :returns: Details about the files that were written.
        """
        result = self._datasinks.saveMode(save_mode).parquet(path)
        return _convert_write_details(result)

    def delta(
        self, path: str, save_mode: Optional[str] = SaveMode.OVERWRITE
    ) -> WriteDetails:
        """
        Writes the data to a directory of Delta files.

        :param path: The URI of the directory to write the files to.
        :param save_mode: The save mode to use when writing the data - "overwrite" will
        overwrite any existing data, "merge" will merge the new data with the existing data based
        on resource ID.
        :returns: Details about the files that were written.
        """
        result = self._datasinks.saveMode(save_mode).delta(path)
        return _convert_write_details(result)

    def tables(
        self,
        schema: Optional[str] = None,
        save_mode: Optional[str] = SaveMode.OVERWRITE,
        format: Optional[str] = None,
    ) -> WriteDetails:
        """
        Writes the data to a set of tables in the Spark catalog.

        :param schema: The name of the schema to write the tables to. If omitted, the current
        database of the Spark session is used.
        :param save_mode: The save mode to use when writing the data - "overwrite" will
        overwrite any existing data, "merge" will merge the new data with the existing data based
        on resource ID.
        :param format: The table format to create tables in, such as "delta" or "parquet". If
        omitted, Spark's default (the `spark.sql.sources.default` setting) is used. Use "delta"
        for tables that will later be appended to or merged into: only a Delta table can be
        widened to admit data whose schema differs from its own, and only a Delta table can be
        merged into.
        :returns: Details about the files that were written.
        """
        sink_builder = self._datasinks.saveMode(save_mode)
        if format:
            # The Java method that takes a format also requires a schema, so the current
            # database stands in for an omitted one, which is where the tables would go anyway.
            result = sink_builder.tables(
                schema or self.spark.catalog.currentDatabase(), format
            )
        elif schema:
            result = sink_builder.tables(schema)
        else:
            result = sink_builder.tables()
        return _convert_write_details(result)
