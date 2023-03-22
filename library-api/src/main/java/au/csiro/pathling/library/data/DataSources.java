/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.library.data;

import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;

public class DataSources {

  @Nonnull
  private final PathlingContext pathlingContext;

  public DataSources(@Nonnull final PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
  }

  @Nonnull
  public DirectSourceBuilder directBuilder() {
    return new DirectSourceBuilder(pathlingContext);
  }

  @Nonnull
  public DatabaseSourceBuilder databaseBuilder() {
    return new DatabaseSourceBuilder(pathlingContext);
  }

  @Nonnull
  public FilesystemSourceBuilder filesystemBuilder() {
    return new FilesystemSourceBuilder(pathlingContext);
  }

  @Nonnull
  public ReadableSource fromWarehouse(@Nonnull final String warehouseUrl) {
    return databaseBuilder().withWarehouseUrl(warehouseUrl).build();
  }

  @Nonnull
  public ReadableSource fromWarehouse(@Nonnull final String warehouseUrl,
      @Nonnull final String databaseName) {
    return databaseBuilder().withWarehouseUrl(warehouseUrl).withDatabaseName(databaseName).build();
  }

  @Nonnull
  public ReadableSource fromFiles(@Nonnull final String filesGlob,
      @Nonnull final Function<String, List<String>> filenameMapper,
      @Nonnull final DataFrameReader reader) {
    return this.filesystemBuilder()
        .withFilesGlob(filesGlob)
        .withFilenameMapper(filenameMapper)
        .withReader(reader).build();
  }

  @Nonnull
  public ReadableSource fromFiles(@Nonnull final String filesGlob,
      @Nonnull final Function<String, List<String>> filenameMapper,
      @Nonnull final String format) {
    return this.filesystemBuilder()
        .withFilesGlob(filesGlob)
        .withFilenameMapper(filenameMapper)
        .withFormat(format).build();
  }

  @Nonnull
  public ReadableSource fromTextFiles(@Nonnull final String filesGlob,
      @Nonnull final Function<String, List<String>> filenameMapper,
      @Nonnull final String mimeType) {
    return this.filesystemBuilder()
        .withFilesGlob(filesGlob)
        .withFilenameMapper(filenameMapper)
        .withTextEncoder(mimeType)
        .build();

  }

  @Nonnull
  public ReadableSource fromNdjsonDir(@Nonnull final String ndjsonDir) {
    return fromTextFiles(Path.of(ndjsonDir, "*.ndjson").toString(),
        SupportFunctions::basenameToResource,
        FhirMimeTypes.FHIR_JSON);
  }

  @Nonnull
  public ReadableSource fromParquetDir(@Nonnull final String parquetDir) {
    return fromFiles(Path.of(parquetDir, "*.parquet").toString(),
        SupportFunctions::basenameToResource,
        "parquet");
  }

}
