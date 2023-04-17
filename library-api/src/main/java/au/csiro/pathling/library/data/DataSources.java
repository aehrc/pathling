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

import static au.csiro.pathling.io.PersistenceScheme.safelyJoinPaths;

import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.DataFrameReader;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A factory for creating various different data sources capable of preparing FHIR data for query.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class DataSources {

  @Nonnull
  private final PathlingContext pathlingContext;

  public DataSources(@Nonnull final PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
  }

  /**
   * Creates a new data source from a directory containing NDJSON encoded FHIR resource data, with
   * filenames containing the resource type the file contains, e.g. "Patient.ndjson" should contain
   * only Patient resources.
   * <p>
   * The filename can also optionally contain a qualifier after the resource type, to allow for
   * resources of the same type to be organised into different files, e.g.
   * "Observation.Chart.ndjson" and "Observation.Lab.ndjson".
   *
   * @param ndjsonDir the URI of directory containing NDJSON files
   * @return the new data source
   */
  @Nonnull
  public ReadableSource ndjson(@Nonnull final String ndjsonDir) {
    return text(safelyJoinPaths(ndjsonDir, "*.ndjson"),
        SupportFunctions::baseNameWithQualifierToResource,
        FhirMimeTypes.FHIR_JSON);
  }

  /**
   * Creates a new data source from a directory containing FHIR Bundles. Takes an argument that
   * specifies the resource types that should be extracted from the bundles and added to the data
   * source.
   * <p>
   * If the MIME type is "application/fhir+xml", then the bundles are expected to be in XML format,
   * and the file extensions are expected to be ".xml". If the MIME type is "application/fhir+json",
   * then the bundles are expected to be in JSON format, and the file extensions are expected to be
   * ".json".
   *
   * @param bundlesDir The URI of the directory containing the bundles
   * @param resourceTypes The resource types to extract from the bundles
   * @param mimeType The MIME type of the bundles
   * @return The new data source
   */
  @Nonnull
  public ReadableSource bundles(@Nonnull final String bundlesDir,
      @Nonnull final Set<String> resourceTypes, @Nonnull final String mimeType) {
    final String glob = mimeType.equals(FhirMimeTypes.FHIR_XML)
                        ? "*.xml"
                        : "*.json";
    return this.fileSystemBuilder()
        .withGlob(safelyJoinPaths(bundlesDir, glob))
        .withFilePathMapper(n -> new ArrayList<>(resourceTypes))
        .withBundleEncoder(mimeType)
        .build();
  }

  /**
   * Creates a new data source form a directory containing `parquet` encoded FHIR resource data,
   * with filenames representing the resource type the file/directory contains. E.g.
   * 'Patient.parquet' should contain Patient resources.
   *
   * @param parquetDir the URI of directory containing parquet file (directories).
   * @return the new data source.
   */
  @Nonnull
  public ReadableSource parquet(@Nonnull final String parquetDir) {
    return files(safelyJoinPaths(parquetDir, "*.parquet"),
        SupportFunctions::baseNameWithQualifierToResource,
        "parquet");
  }

  /**
   * Creates a new data source from a Delta warehouse, such as that used for persistence by Pathling
   * Server.
   *
   * @param path the location of the Delta warehouse
   * @return the new data source
   */
  @Nonnull
  public ReadableSource delta(@Nonnull final String path) {
    return databaseBuilder().withPath(path).build();
  }

  /**
   * Creates a new data source from a specified set of tables registered within the catalog. The
   * table names are assumed to be the same as the resource types they contain.
   *
   * @param resourceTypes the resource types to extract from the tables
   * @return the new data source
   */
  @Nonnull
  public ReadableSource tables(@Nonnull final Set<String> resourceTypes) {
    final Set<ResourceType> resourceTypeEnums = resourceTypes.stream()
        .map(ResourceType::fromCode)
        .collect(Collectors.toSet());
    return tableBuilder(resourceTypeEnums)
        .build();
  }

  /**
   * Creates a new data source form a set of files in one of the supported structured data formats.
   *
   * @param filesGlob the URI with glob pattern for the files
   * @param fileNameMapper a function that maps the file URI to a list of resource  types it
   * contains
   * @param reader the data frame reader to use
   * @return the new data source
   * @see FileSystemSourceBuilder
   */
  @Nonnull
  public ReadableSource files(@Nonnull final String filesGlob,
      @Nonnull final Function<String, List<String>> fileNameMapper,
      @Nonnull final DataFrameReader reader) {
    return this.fileSystemBuilder()
        .withGlob(filesGlob)
        .withFilePathMapper(fileNameMapper)
        .withReader(reader).build();
  }


  /**
   * Creates a new data source from a set of files in one of the supported structured data formats.
   *
   * @param filesGlob the URI with glob pattern for the files
   * @param fileNameMapper a function that maps the file URI to a list of resource types it
   * contains
   * @param format the structured format to use (e.g. "parquet")
   * @return the new data source
   * @see FileSystemSourceBuilder
   */
  @Nonnull
  public ReadableSource files(@Nonnull final String filesGlob,
      @Nonnull final Function<String, List<String>> fileNameMapper,
      @Nonnull final String format) {
    return this.fileSystemBuilder()
        .withGlob(filesGlob)
        .withFilePathMapper(fileNameMapper)
        .withFormat(format).build();
  }

  /**
   * Creates a new data source form a set of text files with one of the supported FHIR encodings.
   *
   * @param filesGlob the URI with glob pattern for the files.
   * @param fileNameMapper a function that maps the file URI to a list of resource  types it
   * contains.
   * @param mimeType the MIME type of the encoding to use.
   * @return the new data source.
   * @see FileSystemSourceBuilder
   */
  @Nonnull
  public ReadableSource text(@Nonnull final String filesGlob,
      @Nonnull final Function<String, List<String>> fileNameMapper,
      @Nonnull final String mimeType) {
    return this.fileSystemBuilder()
        .withGlob(filesGlob)
        .withFilePathMapper(fileNameMapper)
        .withTextEncoder(mimeType)
        .build();

  }

  /**
   * Creates a new data source builder for direct data sources.
   *
   * @return the new builder.
   */
  @Nonnull
  public DatasetSourceBuilder datasetBuilder() {
    return new DatasetSourceBuilder(pathlingContext);
  }

  /**
   * Creates a new data source builder for database data sources.
   *
   * @return the new builder
   */
  @Nonnull
  public DatabaseSourceBuilder databaseBuilder() {
    return new DatabaseSourceBuilder(pathlingContext);
  }

  /**
   * Creates a new data source builder for filesystem data sources.
   *
   * @return the new builder
   */
  @Nonnull
  public FileSystemSourceBuilder fileSystemBuilder() {
    return new FileSystemSourceBuilder(pathlingContext);
  }


  /**
   * Creates a new data source builder for catalog data sources.
   *
   * @return the new builder
   */
  @Nonnull
  public CatalogSourceBuilder tableBuilder(final Set<ResourceType> resourceTypeEnums) {
    return new CatalogSourceBuilder(pathlingContext)
        .withResourceTypes(resourceTypeEnums);
  }

}
