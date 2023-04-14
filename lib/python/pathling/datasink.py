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

from pathling.core import SparkConversionsMixin
from pathling.datasource import DataSource


class DataSink(SparkConversionsMixin):
    """
    A class for writing FHIR data to a variety of different targets.
    """

    def __init__(self, datasource: DataSource):
        SparkConversionsMixin.__init__(self, datasource.spark)
        self._datasinks = self.spark._jvm.au.csiro.pathling.library.data.DataSinks(
            datasource.pc._jpc, datasource._jds.getDataSource()
        )

    def to_ndjson_dir(self, ndjson_dir_uri: str):
        """
        Writes the data to a directory of NDJSON files. The files will be named using the resource
        type and the ".ndjson" extension.

        :param ndjson_dir_uri: The URI of the directory to write the files to.
        """
        self._datasinks.toNdjsonDir(ndjson_dir_uri)
