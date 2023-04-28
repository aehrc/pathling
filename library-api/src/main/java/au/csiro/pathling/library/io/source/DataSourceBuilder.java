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

package au.csiro.pathling.library.io.source;

import static java.util.stream.Collectors.toSet;

import au.csiro.pathling.library.PathlingContext;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A factory for creating various different data sources capable of preparing FHIR data for query.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class DataSourceBuilder {

  @Nonnull
  private final PathlingContext context;

  public DataSourceBuilder(@Nonnull final PathlingContext context) {
    this.context = context;

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
  public NdjsonSource ndjson(@Nonnull final String ndjsonDir) {
    return new NdjsonSource(context, ndjsonDir);
  }

  /**
   * Creates a new data source from a directory containing NDJSON encoded FHIR resource data, with
   * filenames containing the resource type the file contains, e.g. "Patient.ndjson" should contain
   * only Patient resources.
   * <p>
   * The filename can also optionally contain a qualifier after the resource type, to allow for
   * resources of the same type to be organised into different files, e.g.
   * "Observation.Chart.ndjson" and "Observation.Lab.ndjson".
   * <p>
   * A file extension is also provided, which overrides the default ".ndjson" extension and serves
   * as a filter for the files to be included in the data source.
   *
   * @param path the URI of directory containing NDJSON files
   * @param extension the file extension to expect
   * @return the new data source
   */
  @Nonnull
  public NdjsonSource ndjson(@Nonnull final String path, @Nonnull final String extension) {
    return new NdjsonSource(context, path, extension);
  }

  /**
   * Creates a new data source from a directory containing NDJSON encoded FHIR resource data, with
   * filenames determined by the provided function.
   * <p>
   * A file extension is also provided, which overrides the default ".ndjson" extension and serves
   * as a filter for the files to be included in the data source.
   *
   * @param path the URI of directory containing NDJSON files
   * @param extension the file extension to expect
   * @param fileNameMapper a function that maps a filename to a list of resource types
   * @return the new data source
   */
  @Nonnull
  public NdjsonSource ndjson(@Nonnull final String path,
      @Nonnull final String extension,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    return new NdjsonSource(context, path, extension, fileNameMapper);
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
   * @param path the URI of the directory containing the bundles
   * @param resourceTypes the resource types to extract from the bundles
   * @param mimeType the MIME type of the bundles
   * @return the new data source
   */
  @Nonnull
  public BundlesSource bundles(@Nonnull final String path,
      @Nonnull final Set<String> resourceTypes, @Nonnull final String mimeType) {
    final Set<ResourceType> resourceTypeEnums = resourceTypes.stream()
        .map(ResourceType::fromCode)
        .collect(toSet());
    return new BundlesSource(context, path, mimeType, resourceTypeEnums);
  }

  /**
   * Creates a new data source from Spark datasets.
   *
   * @return a {@link DatasetSource}, which can then be populated with datasets that are mapped to
   * the resource types that they contain, using the {@link DatasetSource#dataset} method.
   */
  @Nonnull
  public DatasetSource datasets() {
    return new DatasetSource(context);
  }

  /**
   * Creates a new data source form a directory containing Parquet-encoded FHIR resource data, with
   * filenames representing the resource type the file/directory contains, e.g. 'Patient.parquet'
   * should contain Patient resources.
   *
   * @param path the URI of the directory containing the Parquet files/directories
   * @return the new data source
   */
  @Nonnull
  public ParquetSource parquet(@Nonnull final String path) {
    return new ParquetSource(context, path);
  }

  /**
   * Creates a new data source from a Delta warehouse.
   *
   * @param path the location of the Delta warehouse
   * @return the new data source
   */
  @Nonnull
  public DeltaSource delta(@Nonnull final String path) {
    return new DeltaSource(context, path);
  }

  /**
   * Creates a new data source from a tables registered within the catalog. The table names are
   * assumed to be the same as the resource types they contain.
   *
   * @return the new data source
   */
  @Nonnull
  public CatalogSource tables() {
    return new CatalogSource(context);
  }

  /**
   * Creates a new data source from a specified set of tables registered within the catalog. The
   * table names are assumed to be the same as the resource types they contain. The schema from
   * which the tables are read is specified.
   *
   * @param schema the schema from which the tables are read
   * @return the new data source
   */
  @Nonnull
  public CatalogSource tables(@Nonnull final String schema) {
    return new CatalogSource(context, schema);
  }

}
