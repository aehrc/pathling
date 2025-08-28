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

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Callable

from py4j.java_gateway import JavaObject, JVMView
from pyspark.sql import SparkSession


@dataclass
class FileResult:
    """
    Represents the result of a single file export operation.
    """
    source: str
    """
    The source URL of the exported file.
    """
    destination: str
    """
    The destination URL where the file was saved.
    """
    size: int
    """
    The size of the exported file in bytes.
    """


@dataclass
class ExportResult:
    """
    Represents the result of a bulk export operation.
    """
    transaction_time: datetime
    """
    The time at which the transaction was processed at the server.
    Corresponds to `transactionTime` in the bulk export response.
    """
    results: List[FileResult]
    """
    A list of FileResult objects representing the exported files.
    """

    @classmethod
    def from_java(cls, java_result: JavaObject) -> 'ExportResult':
        """
        Create an ExportResult from a Java export result object.
        
        :param java_result: The Java export result object
        :return: A Python ExportResult object
        """
        # Convert transaction time from Java Instant to Python datetime
        transaction_time = datetime.fromtimestamp(
            java_result.getTransactionTime().toEpochMilli() / 1000.0, tz=timezone.utc)

        # Convert file results
        file_results = [
            FileResult(
                source=str(java_file_result.getSource()),
                destination=str(java_file_result.getDestination()),
                size=java_file_result.getSize())
            for java_file_result in java_result.getResults()
        ]

        return cls(
            transaction_time=transaction_time,
            results=file_results
        )


class BulkExportClient:
    """
    A client for exporting data from the FHIR Bulk Data Access API.
    """

    def __init__(self, java_client):
        """
        Create a new BulkExportClient that wraps a Java BulkExportClient.
        
        :param java_client: The Java BulkExportClient instance to wrap
        """
        self._java_client = java_client

    def export(self) -> ExportResult:
        """
        Export data from the FHIR server.
        
        :return: The result of the export operation as a Python ExportResult object
        """
        java_result = self._java_client.export()
        return ExportResult.from_java(java_result)

    @classmethod
    def _configure_builder(cls, jvm, builder, fhir_endpoint_url: str, output_dir: str,
                           output_format: str = "application/fhir+ndjson",
                           since: Optional[datetime] = None,
                           types: Optional[List[str]] = None,
                           elements: Optional[List[str]] = None,
                           include_associated_data: Optional[List[str]] = None,
                           type_filters: Optional[List[str]] = None,
                           output_extension: str = "ndjson",
                           timeout: Optional[int] = None,
                           max_concurrent_downloads: int = 10,
                           auth_config: Optional[dict] = None):
        """
        Configure common builder parameters.
        
        :param jvm: The JVM instance
        :param builder: The builder instance to configure
        :param fhir_endpoint_url: The URL of the FHIR server
        :param output_dir: Output directory
        :param output_format: Output format
        :param since: Timestamp filter (must include timezone information)
        :param types: Resource types to include
        :param elements: Elements to include
        :param include_associated_data: Associated data to include
        :param type_filters: Resource filters
        :param output_extension: File extension for output files
        :param timeout: Optional timeout duration in seconds
        :param max_concurrent_downloads: Maximum number of concurrent downloads
        :param auth_config: Optional authentication configuration dictionary with the following possible keys:
            - enabled: Whether authentication is enabled (default: False)
            - client_id: The client ID to use for authentication
            - private_key_jwk: The private key in JWK format
            - client_secret: The client secret to use for authentication
            - token_endpoint: The token endpoint URL
            - use_smart: Whether to use SMART authentication (default: True)
            - use_form_for_basic_auth: Whether to use form-based basic auth (default: False)
            - scope: The scope to request
            - token_expiry_tolerance: The token expiry tolerance in seconds (default: 120)
        """
        builder.withFhirEndpointUrl(fhir_endpoint_url)
        builder.withOutputDir(output_dir)
        builder.withOutputFormat(output_format)
        builder.withOutputExtension(output_extension)
        builder.withMaxConcurrentDownloads(max_concurrent_downloads)

        if timeout is not None:
            java_duration = jvm.java.time.Duration.ofSeconds(timeout)
            builder.withTimeout(java_duration)

        if since is not None:
            if since.tzinfo is None:
                raise ValueError("datetime must include timezone information")
            # Format with microsecond precision and timezone offset
            instant_str = since.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # Truncate to milliseconds
            if since.utcoffset() is None:
                instant_str += 'Z'
            else:
                offset = since.strftime('%z')
                # Insert colon in timezone offset
                instant_str += f"{offset[:3]}:{offset[3:]}"
            java_instant = jvm.java.time.Instant.parse(instant_str)
            builder.withSince(java_instant)
        if types is not None:
            for type_ in types:
                builder.withType(type_)
        if elements is not None:
            for element in elements:
                builder.withElement(element)
        if include_associated_data is not None:
            # Convert Python list to Java List<String>
            java_list = jvm.java.util.ArrayList()
            for data in include_associated_data:
                java_list.add(data)
            builder.withIncludeAssociatedData(java_list)
        if type_filters is not None:
            for filter_ in type_filters:
                builder.withTypeFilter(filter_)

        if auth_config is not None:
            auth_builder = jvm.au.csiro.fhir.auth.AuthConfig.builder()

            # Set defaults to match Java class
            auth_builder.enabled(False)
            auth_builder.useSMART(True)
            auth_builder.useFormForBasicAuth(False)
            auth_builder.tokenExpiryTolerance(120)

            # Map Python config to Java builder methods
            if 'enabled' in auth_config:
                auth_builder.enabled(auth_config['enabled'])
            if 'use_smart' in auth_config:
                auth_builder.useSMART(auth_config['use_smart'])
            if 'token_endpoint' in auth_config:
                auth_builder.tokenEndpoint(auth_config['token_endpoint'])
            if 'client_id' in auth_config:
                auth_builder.clientId(auth_config['client_id'])
            if 'client_secret' in auth_config:
                auth_builder.clientSecret(auth_config['client_secret'])
            if 'private_key_jwk' in auth_config:
                auth_builder.privateKeyJWK(auth_config['private_key_jwk'])
            if 'use_form_for_basic_auth' in auth_config:
                auth_builder.useFormForBasicAuth(auth_config['use_form_for_basic_auth'])
            if 'scope' in auth_config:
                auth_builder.scope(auth_config['scope'])
            if 'token_expiry_tolerance' in auth_config:
                auth_builder.tokenExpiryTolerance(auth_config['token_expiry_tolerance'])

            auth_config_obj = auth_builder.build()
            builder.withAuthConfig(auth_config_obj)

    @classmethod
    def for_system(cls, spark, *args, **kwargs) -> 'BulkExportClient':
        """
        Create a builder for a system-level export.
        
        :param spark: The SparkSession instance
        :param fhir_endpoint_url: The URL of the FHIR server to export from
        :param output_dir: The directory to write the output files to
        :param output_format: The format of the output data
        :param since: Only include resources modified after this timestamp
        :param types: List of FHIR resource types to include
        :param elements: List of FHIR elements to include
        :param include_associated_data: Pre-defined set of FHIR resources to include
        :param type_filters: FHIR search queries to filter resources
        :param output_extension: File extension for output files
        :param timeout: Optional timeout duration in seconds
        :param max_concurrent_downloads: Maximum number of concurrent downloads
        :param auth_config: Optional authentication configuration dictionary
        :return: A BulkExportClient configured for system-level export
        """
        builder, jvm = cls._create_builder(spark, lambda bc: bc.systemBuilder())
        cls._configure_builder(jvm, builder, *args, **kwargs)
        return cls(builder.build())

    @classmethod
    def for_group(cls, spark, fhir_endpoint_url: str, output_dir: str,
                  group_id: str, *args, **kwargs) -> 'BulkExportClient':
        """
        Create a builder for a group-level export.
        
        :param spark: The SparkSession instance
        :param fhir_endpoint_url: The URL of the FHIR server to export from
        :param output_dir: The directory to write the output files to
        :param group_id: The ID of the group to export
        :param output_format: The format of the output data
        :param since: Only include resources modified after this timestamp
        :param types: List of FHIR resource types to include
        :param elements: List of FHIR elements to include
        :param include_associated_data: Pre-defined set of FHIR resources to include
        :param type_filters: FHIR search queries to filter resources
        :param output_extension: File extension for output files
        :param timeout: Optional timeout duration in seconds
        :param max_concurrent_downloads: Maximum number of concurrent downloads
        :param auth_config: Optional authentication configuration dictionary
        :return: A BulkExportClient configured for group-level export
        """
        # Pass group_id directly to groupBuilder
        builder, jvm = cls._create_builder(spark, lambda bc: bc.groupBuilder(group_id))
        cls._configure_builder(jvm, builder, fhir_endpoint_url, output_dir, *args, **kwargs)
        return cls(builder.build())

    @classmethod
    def for_patient(cls, spark, fhir_endpoint_url: str, output_dir: str,
                    patients: Optional[List[str]] = None, *args, **kwargs) -> 'BulkExportClient':
        """
        Create a builder for a patient-level export.
        
        :param spark: The SparkSession instance
        :param fhir_endpoint_url: The URL of the FHIR server to export from
        :param output_dir: The directory to write the output files to
        :param patients: List of patient references to include
        :param output_format: The format of the output data
        :param since: Only include resources modified after this timestamp
        :param types: List of FHIR resource types to include
        :param elements: List of FHIR elements to include
        :param include_associated_data: Pre-defined set of FHIR resources to include
        :param type_filters: FHIR search queries to filter resources
        :param output_extension: File extension for output files
        :param timeout: Optional timeout duration in seconds
        :param max_concurrent_downloads: Maximum number of concurrent downloads
        :param auth_config: Optional authentication configuration dictionary
        :return: A BulkExportClient configured for patient-level export
        """
        builder, jvm = cls._create_builder(spark, lambda bc: bc.patientBuilder())
        if patients is not None:
            for patient in patients:
                ref = jvm.au.csiro.fhir.model.Reference.of(patient)
                builder.withPatient(ref)
        cls._configure_builder(jvm, builder, fhir_endpoint_url, output_dir, *args, **kwargs)
        return cls(builder.build())

    @classmethod
    def _create_builder(cls,
                        spark: SparkSession,
                        factory_f: Callable[[JavaObject], JavaObject]) -> Tuple[
        JavaObject, JVMView]:

        jvm: JVMView = spark._jvm
        client_class = jvm.au.csiro.fhir.export.BulkExportClient
        builder: JavaObject = factory_f(client_class)
        builder = builder.withFileStoreFactory(
            jvm.au.csiro.filestore.hdfs.HdfsFileStoreFactory(spark._jsc.sc().hadoopConfiguration())
        )
        return (builder, jvm)
