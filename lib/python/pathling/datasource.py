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

from datetime import datetime
from json import dumps, loads
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence

from py4j.java_collections import SetConverter
from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from pathling import PathlingContext
from pathling.core import SparkConversionsMixin, StringToStringSetMapper
from pathling.fhir import MimeType
from pathling.spark import Dfs

if TYPE_CHECKING:
    from pathling.datasink import DataSinks


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
        return list(self._jds.getResourceTypes())

    @property
    def write(self) -> "DataSinks":
        """
        Provides access to a :class:`DataSinks` object that can be used to persist data.
        """
        # Import here to avoid circular dependency
        from pathling.datasink import DataSinks

        return DataSinks(self)

    def view(
        self,
        resource: Optional[str] = None,
        select: Optional[Sequence[Dict]] = None,
        constant: Optional[Sequence[Dict]] = None,
        where: Optional[Sequence[Dict]] = None,
        json: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes a SQL on FHIR view definition and returns the result as a Spark DataFrame.

        :param resource: The FHIR resource that the view is based upon, e.g. 'Patient' or
               'Observation'.
        :param select: A list of columns and nested selects to include in the view.
        :param constant: A list of constants that can be used in FHIRPath expressions.
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
                for key in ["resource", "select", "constant", "where"]
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

    # Default extension and MIME type for NDJSON files
    NDJSON_EXTENSION = "ndjson"
    NDJSON_MIMETYPE = "application/fhir+ndjson"

    def __init__(self, pathling: PathlingContext):
        SparkConversionsMixin.__init__(self, pathling.spark)
        self._pc = pathling
        self._jdataSources = pathling._jpc.read()

    def _wrap_ds(self, jds: JavaObject) -> DataSource:
        return DataSource(jds, self._pc)

    def _stored_sources(self, layout_detection: bool) -> JavaObject:
        """
        Returns the Java builder for a source reading stored data, with layout detection enabled
        or disabled for the sources it creates.

        :param layout_detection: Whether the sources classify the layout of the data they read.
        :return: The Java data source builder.
        """
        return self._jdataSources.withLayoutDetection(layout_detection)

    def ndjson(
        self,
        path,
        extension: Optional[str] = None,
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

        extension = extension or DataSources.NDJSON_EXTENSION

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

        The data frames are expected in the Parquet on FHIR layout. Their layout is not checked.
        Data frames in the layout written by earlier releases of Pathling are not supported, and
        the results of a query over them are undefined.

        :param resources: A dictionary of Spark DataFrames, where the keys are resource type codes
               and the values are the data frames containing the resource data.
        :return: A DataSource object that can be used to run queries against the data.
        """
        jbuilder = self._jdataSources.datasets()
        for resource_code, resource_data in resources.items():
            jbuilder.dataset(resource_code, resource_data._jdf)
        return self._wrap_ds(jbuilder)

    def parquet(
        self,
        path: str,
        schemas: Optional[Dict[str, StructType]] = None,
        layout_detection: bool = True,
    ) -> DataSource:
        """
        Creates a data source from a directory containing Parquet tables. Each table must be named
        according to the name of the resource type that it stores.

        The files of a resource type may carry differing schemas, and are read as one dataset whose
        schema is the union of theirs. Merging the schemas reads the footer of every file, so the
        cost of it can be avoided by supplying the schema to read a resource type with. A supplied
        schema is used as given, so it must cover the union of the type's files: columns it omits
        are not read, and it can lose whole rows, not just columns. A view unnesting a repeating
        element returns none of a file's rows for that element when the schema omits every field the
        file carries inside it. The workable pattern is to merge once and persist the result, or to
        take the writer's schema, and supply that.

        The layout of each resource type's data is classified as it is read, and data in the layout
        written by earlier releases of Pathling is refused with a message naming the remedy, which
        is to re-import it from its source. Where there is no source, the legacy export converts it
        to NDJSON for re-import, losing what that layout did not store. The legacy export is a
        separate tool, described in the Pathling documentation on the storage layout.

        :param path: The URI of the directory containing the Parquet tables.
        :param schemas: An optional dictionary of the schema to read each resource type with, keyed
               by resource type code. Resource types it does not name are merged as usual.
        :param layout_detection: Whether to classify the layout of each resource type's data as
               it is read, refusing data in the layout written by earlier releases of Pathling.
               Defaults to True. Set it to False only for conforming data that the detector
               misclassifies; it applies to this data source alone. Data in the earlier
               layout read with it disabled is not supported, and the results of a query over it
               are undefined.
        :return: A DataSource object that can be used to run queries against the data.
        """
        if schemas:
            jvm = self.spark._jvm
            jschemas = jvm.java.util.HashMap()
            for resource_type, schema in schemas.items():
                jschemas.put(
                    resource_type,
                    jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json()),
                )
            return self._wrap_ds(
                self._stored_sources(layout_detection).parquetWithSchemas(
                    path, jschemas
                )
            )
        return self._wrap_ds(self._stored_sources(layout_detection).parquet(path))

    def delta(self, path: str, layout_detection: bool = True) -> DataSource:
        """
        Creates a data source from a directory containing Delta tables, as used by Pathling Server
        for persistence. Each table must be named according to the name of the resource type that
        it stores.

        The layout of each table is classified as it is read, and a table in the layout written by
        earlier releases of Pathling is refused with a message naming the remedy, which is to
        re-import it from its source. Where there is no source, the legacy export converts it to
        NDJSON for re-import, losing what that layout did not store. The legacy export is a separate
        tool, described in the Pathling documentation on the storage layout.

        :param path: The URI of the directory containing the Delta tables.
        :param layout_detection: Whether to classify the layout of each resource type's data as
               it is read, refusing data in the layout written by earlier releases of Pathling.
               Defaults to True. Set it to False only for conforming data that the detector
               misclassifies; it applies to this data source alone. Data in the earlier
               layout read with it disabled is not supported, and the results of a query over it
               are undefined.
        :return: A DataSource object that can be used to run queries against the data.
        """
        return self._wrap_ds(self._stored_sources(layout_detection).delta(path))

    def tables(
        self,
        schema: Optional[str] = None,
        layout_detection: bool = True,
    ) -> DataSource:
        """
        Creates a data source from a set of Spark tables, where the table names are the resource
        type codes.

        The layout of each table is classified as it is read, and a table in the layout written by
        earlier releases of Pathling is refused with a message naming the remedy, which is to
        re-import it from its source. Where there is no source, the legacy export converts it to
        NDJSON for re-import, losing what that layout did not store. The legacy export is a separate
        tool, described in the Pathling documentation on the storage layout.

        :param schema: An optional schema name that should be used to qualify the table names.
        :param layout_detection: Whether to classify the layout of each resource type's data as
               it is read, refusing data in the layout written by earlier releases of Pathling.
               Defaults to True. Set it to False only for conforming data that the detector
               misclassifies; it applies to this data source alone. Data in the earlier
               layout read with it disabled is not supported, and the results of a query over it
               are undefined.
        :return: A DataSource object that can be used to run queries against the data.
        """
        sources = self._stored_sources(layout_detection)
        if schema:
            return self._wrap_ds(sources.tables(schema))
        else:
            return self._wrap_ds(sources.tables())

    def bulk(
        self,
        fhir_endpoint_url: str,
        output_dir: Optional[str] = None,
        overwrite: bool = True,
        group_id: Optional[str] = None,
        patients: Optional[List[str]] = None,
        since: Optional[datetime] = None,
        types: Optional[List[str]] = None,
        elements: Optional[List[str]] = None,
        include_associated_data: Optional[List[str]] = None,
        type_filters: Optional[List[str]] = None,
        timeout: Optional[int] = None,
        max_concurrent_downloads: int = 10,
        auth_config: Optional[Dict] = None,
    ) -> DataSource:
        """
        Creates a data source from a FHIR Bulk Data Access API endpoint.
        Currently only supports bulk export in the ndjson format.

        :param fhir_endpoint_url: The URL of the FHIR server to export from
        :param output_dir: The directory to write the output files to.
                This should be a valid path in the Spark's filesystem.
                If set to `None`, a temporary directory will be used instead.
        :param overwrite: Whether to overwrite the output directory if it already exists. Defaults to True.
        :param group_id: Optional group ID for group-level export
        :param patients: Optional list of patient references for patient-level export
        :param since: Only include resources modified after this timestamp
        :param types: List of FHIR resource types to include
        :param elements: List of FHIR elements to include
        :param include_associated_data: Pre-defined set of FHIR resources to include
        :param type_filters: FHIR search queries to filter resources
        :param timeout: Optional timeout duration in seconds
        :param max_concurrent_downloads: Maximum number of concurrent downloads. Defaults to 10
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
        :return: A DataSource object that can be used to run queries against the data
        """
        from pathling.bulk import BulkExportClient

        dfs = Dfs(self._pc.spark)

        # If `output_dir` is not provided, create a temporary directory
        output_dir = output_dir or dfs.get_temp_dir_path(
            prefix="tmp-bulk-export", qualified=True
        )
        # If `overwrite`, then ensure the output directory does not exist
        if overwrite and dfs.exists(output_dir):
            dfs.delete(output_dir, recursive=True)

        output_format = DataSources.NDJSON_MIMETYPE
        output_extension = DataSources.NDJSON_EXTENSION

        # Create appropriate client based on parameters
        if group_id is not None:
            client = BulkExportClient.for_group(
                self.spark,
                fhir_endpoint_url=fhir_endpoint_url,
                output_dir=output_dir,
                group_id=group_id,
                output_format=output_format,
                since=since,
                types=types,
                elements=elements,
                include_associated_data=include_associated_data,
                type_filters=type_filters,
                output_extension=output_extension,
                timeout=timeout,
                max_concurrent_downloads=max_concurrent_downloads,
                auth_config=auth_config,
            )
        elif patients is not None:
            client = BulkExportClient.for_patient(
                self.spark,
                fhir_endpoint_url=fhir_endpoint_url,
                output_dir=output_dir,
                patients=patients,
                output_format=output_format,
                since=since,
                types=types,
                elements=elements,
                include_associated_data=include_associated_data,
                type_filters=type_filters,
                output_extension=output_extension,
                timeout=timeout,
                max_concurrent_downloads=max_concurrent_downloads,
                auth_config=auth_config,
            )
        else:
            client = BulkExportClient.for_system(
                self.spark,
                fhir_endpoint_url=fhir_endpoint_url,
                output_dir=output_dir,
                output_format=output_format,
                since=since,
                types=types,
                elements=elements,
                include_associated_data=include_associated_data,
                type_filters=type_filters,
                output_extension=output_extension,
                timeout=timeout,
                max_concurrent_downloads=max_concurrent_downloads,
                auth_config=auth_config,
            )

        # Perform the export
        client.export()

        # Return a DataSource that reads from the exported files
        return self.ndjson(output_dir)
