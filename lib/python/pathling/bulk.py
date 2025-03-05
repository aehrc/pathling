#  Copyright 2025 Commonwealth Scientific and Industrial Research
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

from datetime import datetime
from typing import List, Optional

from pathling import PathlingContext


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

    def export(self):
        """
        Export data from the FHIR server.
        
        :return: The result of the export operation
        """
        return self._java_client.export()

    @classmethod
    def create(cls, jvm, fhir_endpoint_url: str, output_dir: str,
               group_id: Optional[str] = None,
               patients: Optional[List[str]] = None,
               output_format: str = "application/fhir+ndjson",
               since: Optional[datetime] = None,
               types: Optional[List[str]] = None, 
               elements: Optional[List[str]] = None,
               include_associated_data: Optional[List[str]] = None,
               type_filters: Optional[List[str]] = None,
               output_extension: str = "ndjson",
               timeout: Optional[int] = None,
               max_concurrent_downloads: int = 10,
               auth_config: Optional[dict] = None) -> 'BulkExportClient':
        """
        Create a BulkExportClient for the appropriate export level based on the provided arguments.
        
        :param jvm: The JVM instance
        :param fhir_endpoint_url: The URL of the FHIR server to export from
        :param output_dir: The directory to write the output files to
        :param group_id: Optional group ID for group-level export
        :param patients: Optional list of patient IDs for patient-level export
        :param output_format: The format of the output data
        :param since: Only include resources modified after this timestamp
        :param types: List of FHIR resource types to include
        :param elements: List of FHIR elements to include
        :param include_associated_data: Pre-defined set of FHIR resources to include
        :param type_filters: FHIR search queries to filter resources
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
        :return: A BulkExportClient configured for the appropriate export level
        """
        client_class = jvm.au.csiro.fhir.export.BulkExportClient
        
        # Determine the export level based on the provided arguments
        if group_id is not None:
            # Group-level export
            builder = client_class.groupBuilder(group_id)
        elif patients is not None and len(patients) > 0:
            # Patient-level export
            builder = client_class.patientBuilder()
            for patient in patients:
                ref = jvm.au.csiro.fhir.model.Reference.of(patient)
                builder.withPatient(ref)
        else:
            # System-level export
            builder = client_class.systemBuilder()
        
        # Configure the builder with common settings
        cls._configure_builder(jvm, builder, fhir_endpoint_url, output_dir, output_format, 
                              output_extension, max_concurrent_downloads, since, types, 
                              elements, include_associated_data, type_filters, timeout, auth_config)
        
        # Build and return the client
        return cls(builder.build())

    @staticmethod
    def _configure_builder(jvm, builder, fhir_endpoint_url: str, output_dir: str,
                          output_format: str, output_extension: str, max_concurrent_downloads: int,
                          since: Optional[datetime], types: Optional[List[str]],
                          elements: Optional[List[str]], include_associated_data: Optional[List[str]],
                          type_filters: Optional[List[str]], timeout: Optional[int],
                          auth_config: Optional[dict]):
        """
        Configure the builder with common settings.
        
        :param jvm: The JVM instance
        :param builder: The builder to configure
        :param fhir_endpoint_url: The URL of the FHIR server
        :param output_dir: Output directory
        :param output_format: Output format
        :param output_extension: File extension for output files
        :param max_concurrent_downloads: Maximum number of concurrent downloads
        :param since: Timestamp filter
        :param types: Resource types to include
        :param elements: Elements to include
        :param include_associated_data: Associated data to include
        :param type_filters: Resource filters
        :param timeout: Optional timeout duration in seconds
        :param auth_config: Optional authentication configuration
        """
        # Configure basic settings
        builder.withFhirEndpointUrl(fhir_endpoint_url)
        builder.withOutputDir(output_dir)
        builder.withOutputFormat(output_format)
        builder.withOutputExtension(output_extension)
        builder.withMaxConcurrentDownloads(max_concurrent_downloads)
        
        # Configure timeout if provided
        if timeout is not None:
            java_duration = jvm.java.time.Duration.ofSeconds(timeout)
            builder.withTimeout(java_duration)
        
        # Configure since timestamp if provided
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
        
        # Configure resource types if provided
        if types is not None:
            for type_ in types:
                builder.withType(type_)
        
        # Configure elements if provided
        if elements is not None:
            for element in elements:
                builder.withElement(element)
        
        # Configure associated data if provided
        if include_associated_data is not None:
            for data in include_associated_data:
                j_object = jvm.au.csiro.fhir.export.ws.AssociatedData.fromCode(data)
                builder.withIncludeAssociatedDatum(j_object)
        
        # Configure type filters if provided
        if type_filters is not None:
            for filter_ in type_filters:
                builder.withTypeFilter(filter_)
        
        # Configure authentication if provided
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
