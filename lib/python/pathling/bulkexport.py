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
from typing import Sequence, Optional, NamedTuple

from py4j.java_gateway import JavaObject, JVMView

from datetime import datetime, timedelta, timezone

from pathling.fhir import Reference

from pathling.jvm import (
    jvm_pathling,
    jvm,
    get_active_java_spark_context,
    datetime_to_instant,
    timedelta_to_duration,
    instant_to_datetime,
)
from pathling.config import auth_config


class ExportLevel(Enum):
    """
    The level of bulk export.
    """

    SYSTEM = "SYSTEM"
    PATIENT = "PATIENT"
    GROUP = "GROUP"


class BulkExportResult(NamedTuple):
    """
    The result of a bulk export operation.
    """

    transaction_time: datetime
    """
    The value of the `transactionTime` field of the Bulk Export final response. 
    Indicates the server's time when the query is run.
    """

    @classmethod
    def from_java(cls, j_result: JavaObject) -> "BulkExportResult":
        """
        Create a BulkExportResult from a Java BulkExportResult.
        :param j_result: the Java BulkExportResult
        :return: the equivalent BulkExportResult
        """
        return cls(transaction_time=instant_to_datetime(j_result.getTransactionTime()))


# noinspection PyProtectedMember
class BulkExportClient:
    """
    Client for bulk export of FHIR resources from a FHIR server.
    See: https://hl7.org/fhir/uv/bulkdata/STU2/export.html

    Example use::

        BulkExportClient('http://example.com/fhir', 'output-dir').export()
    """

    def __init__(
        self,
        fhir_endpoint_url: str,
        output_dir_url: str,
        level: Optional[ExportLevel] = ExportLevel.SYSTEM,
        group_id: Optional[str] = None,
        output_format: str = "application/fhir+ndjson",
        types: Optional[Sequence[str]] = None,
        since: Optional[datetime] = None,
        patients: Optional[Sequence[Reference]] = None,
        timeout: Optional[timedelta] = None,
        max_concurrent_downloads=10,
        auth_config: Optional[JavaObject] = None,
    ):
        """
        :param fhir_endpoint_url: the FHIR endpoint URL
        :param output_dir_url: the output directory URL
        :param level: the level of bulk export. One of :class:`ExportLevel` values.
        :param group_id: the value of the `groupId` parameter the group export. Required for GROUP
                level export.
        :param output_format: the value of the `_outputFormat` parameter for Bulk Export kick-off request
        :param types: the value of the `_type` parameter for Bulk Export kick-off request
        :param since: the value of the `_since` parameter for Bulk Data kick-off request
        :param patients: the value of the `patient` parameter for Bulk Export kick-off request
        :param timeout: the maximum time to wait for the export to complete. If the export does not
                complete within this time, an exception is raised. By default, not time limit is set.
        :param max_concurrent_downloads: the maximum number of concurrent downloads to allow.
                By default, 10.
        :param auth_config: optional authentication configuration created
                with :func:`pathling.config.auth_config`
        """

        sc: JavaObject = get_active_java_spark_context().sc()

        client_builder = None
        if level == ExportLevel.SYSTEM:
            client_builder = jvm_pathling().export.BulkExportClient.systemBuilder()
        elif level == ExportLevel.PATIENT:
            client_builder = jvm_pathling().export.BulkExportClient.patientBuilder()
        elif level == ExportLevel.GROUP:
            if group_id is None:
                raise ValueError("groupId is required for group export")
            client_builder = jvm_pathling().export.BulkExportClient.groupBuilder(
                group_id
            )

        client_builder.withFileStoreFactory(
            jvm_pathling().library.fs.HdfsFileStoreFactory.ofSpark(sc)
        ).withFhirEndpointUrl(fhir_endpoint_url).withOutputDir(
            output_dir_url
        ).withOutputFormat(
            output_format
        ).withTypes(
            types or []
        ).withSince(
            datetime_to_instant(since) if since else None
        ).withMaxConcurrentDownloads(
            max_concurrent_downloads
        )

        timeout and client_builder.withTimeout(timedelta_to_duration(timeout))
        patients and client_builder.withPatients([ref.to_java() for ref in patients])
        auth_config and client_builder.withAuthConfig(auth_config)

        self._jclient = client_builder.build()

    def export(self) -> BulkExportResult:
        """
        Runs the bulk export process.
        :return: the result of the export operation od type :class:`BulkExportResult`
        """
        return BulkExportResult.from_java(self._jclient.export())
