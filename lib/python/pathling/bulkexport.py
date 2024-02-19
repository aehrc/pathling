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

from enum import Enum
from typing import Sequence, Optional

from py4j.java_gateway import JavaObject, JVMView
from pyspark import SparkContext

from datetime import datetime

Method = Enum('Method', ['SYSTEM', 'PATIENT', 'GROUP'])


def bulk_export(
    fhirEndpointUrl: str,
    outputDirUrl: str,
    outputFormat: str = 'ndjson',
    types: Optional[Sequence[str]] = None,
    since: Optional[datetime] = None,
    method: Optional[Method] = Method.SYSTEM,
    groupId: Optional[str] = None,
    patients: Optional[Sequence[str]] = None,
):
    """
    Bulk export FHIR resources from a FHIR server.
    :param fhirEndpointUrl: the FHIR endpoint URL
    :param outputDirUrl: the output directory URL
    :param outputFormat: the value of the `_outputFormat` parameter for Bulk Export kick-off request
    :param types: the value of the `_type` parameter for Bulk Export kick-off request
    :param since: the value of the `_since` parameter for Bulk Data kick-off request 
    """

    if SparkContext._active_spark_context is None:
        raise ValueError("No active SparkContext")

    jsc: JavaObject = SparkContext._active_spark_context._jsc
    jvm: Optional[JVMView] = SparkContext._active_spark_context._jvm
    jvm_bulk_export: JavaObject = jvm.au.csiro.pathling.export

    def datetime_to_instant(dt: datetime) -> JavaObject:
        return jvm.java.time.Instant.ofEpochMilli(int(dt.timestamp() * 1000))

    bulk_export_client = jvm_bulk_export.BulkExportClient.builder() \
        .withFileStoreFactory(jvm_bulk_export.fs.HdfsFileStoreFactory.ofSpark(jsc.sc())) \
        .withFhirEndpointUrl(fhirEndpointUrl) \
        .withOutputDir(outputDirUrl) \
        .withOutputFormat(outputFormat) \
        .withTypes(types or []) \
        .withSince(datetime_to_instant(since) if since else None) \
        .build()

    bulk_export_client.export()
