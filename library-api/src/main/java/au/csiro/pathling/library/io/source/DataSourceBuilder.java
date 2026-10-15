/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import static au.csiro.pathling.utilities.Preconditions.checkArgumentNotNull;

import au.csiro.fhir.export.BulkExportClient;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.spark.sql.types.StructType;

/**
 * A factory for creating various different data sources capable of preparing FHIR data for query.
 *
 * <p>The sources that read stored data (Parquet, Delta and catalog tables) classify the layout of
 * each resource type's data as it is read, and refuse data in a layout this release does not read,
 * such as the layout written by earlier releases, with an {@link UnsupportedLayoutException}
 * (FR-037). {@link #withLayoutDetection(boolean)} disables this for the sources a builder creates,
 * as an opt-out for conforming data the detector misclassifies (FR-040).
 *
 * <p>Layout detection at read time is the only check made. Data in the earlier layout that bypasses
 * it, through a source with layout detection disabled or through {@link #datasets()}, is not
 * supported, and the results of a query over it are undefined. Such data is migrated by
 * re-importing it from its source or, where there is none, by converting it to NDJSON with the
 * legacy export, the separate {@code au.csiro.pathling:legacy-export} artifact, and reading that
 * with {@link #ndjson(String)}.
 *
 * @param context the {@link PathlingContext} to use for creating the data sources
 * @param layoutDetection whether the Parquet, Delta and catalog sources this builder creates
 *     classify the layout of the data they read
 * @author Piotr Szul
 * @author John Grimes
 */
public record DataSourceBuilder(@Nonnull PathlingContext context, boolean layoutDetection) {

  /**
   * Creates a builder whose sources classify the layout of the data they read.
   *
   * @param context the {@link PathlingContext} to use for creating the data sources
   */
  public DataSourceBuilder(@Nonnull final PathlingContext context) {
    this(context, true);
  }

  /**
   * Returns a builder whose Parquet, Delta and catalog sources classify the layout of the data they
   * read, or do not. Each source keeps the setting it was created with, so disabling detection for
   * one source leaves every other source, including those created from this builder, unaffected.
   *
   * @param layoutDetection false to skip layout detection in the sources the returned builder
   *     creates
   * @return a builder with the given setting
   */
  @Nonnull
  public DataSourceBuilder withLayoutDetection(final boolean layoutDetection) {
    return new DataSourceBuilder(context, layoutDetection);
  }

  /**
   * Creates a new data source from a directory containing NDJSON encoded FHIR resource data, with
   * filenames containing the resource type the file contains, e.g. "Patient.ndjson" should contain
   * only Patient resources.
   *
   * <p>The filename can also optionally contain a qualifier after the resource type, to allow for
   * resources of the same type to be organised into different files, e.g.
   * "Observation.Chart.ndjson" and "Observation.Lab.ndjson".
   *
   * @param ndjsonDir the URI of directory containing NDJSON files
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource ndjson(@Nullable final String ndjsonDir) {
    return new NdjsonSource(context, checkArgumentNotNull(ndjsonDir));
  }

  /**
   * Creates a new data source from a directory containing NDJSON encoded FHIR resource data, with
   * filenames containing the resource type the file contains, e.g. "Patient.ndjson" should contain
   * only Patient resources.
   *
   * <p>The filename can also optionally contain a qualifier after the resource type, to allow for
   * resources of the same type to be organised into different files, e.g.
   * "Observation.Chart.ndjson" and "Observation.Lab.ndjson".
   *
   * <p>A file extension is also provided, which overrides the default ".ndjson" extension and
   * serves as a filter for the files to be included in the data source.
   *
   * @param path the URI of directory containing NDJSON files
   * @param extension the file extension to expect
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource ndjson(@Nullable final String path, @Nullable final String extension) {
    return new NdjsonSource(context, checkArgumentNotNull(path), checkArgumentNotNull(extension));
  }

  /**
   * Creates a new data source from a directory containing NDJSON encoded FHIR resource data, with
   * filenames determined by the provided function.
   *
   * <p>A file extension is also provided, which overrides the default ".ndjson" extension and
   * serves as a filter for the files to be included in the data source.
   *
   * @param path the URI of directory containing NDJSON files
   * @param extension the file extension to expect
   * @param fileNameMapper a function that maps a filename to a list of resource types
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource ndjson(
      @Nullable final String path,
      @Nullable final String extension,
      @Nullable final Function<String, Set<String>> fileNameMapper) {
    return new NdjsonSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(extension),
        checkArgumentNotNull(fileNameMapper));
  }

  /**
   * Creates a new data source from a directory containing FHIR Bundles. Takes an argument that
   * specifies the resource types that should be extracted from the bundles and added to the data
   * source.
   *
   * <p>If the MIME type is "application/fhir+xml", then the bundles are expected to be in XML
   * format, and the file extensions are expected to be ".xml". If the MIME type is
   * "application/fhir+json", then the bundles are expected to be in JSON format, and the file
   * extensions are expected to be ".json".
   *
   * @param path the URI of the directory containing the bundles
   * @param resourceTypes the resource types to extract from the bundles
   * @param mimeType the MIME type of the bundles
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource bundles(
      @Nullable final String path,
      @Nullable final Set<String> resourceTypes,
      @Nullable final String mimeType) {
    return new BundlesSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(mimeType),
        checkArgumentNotNull(resourceTypes));
  }

  /**
   * Creates a new data source from Spark datasets.
   *
   * @return a {@link DatasetSource}, which can then be populated with datasets that are mapped to
   *     the resource types that they contain, using the {@link DatasetSource#dataset} method.
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
  public QueryableDataSource parquet(@Nullable final String path) {
    return new ParquetSource(
        context,
        checkArgumentNotNull(path),
        FileSource::resourceNameWithQualifierMapper,
        Map.of(),
        layoutDetection);
  }

  /**
   * Creates a new data source from a directory containing Parquet-encoded FHIR resource data, with
   * filenames determined by the provided function.
   *
   * @param path the URI of the directory containing the Parquet files/directories
   * @param fileNameMapper a function that maps a filename to a set of resource types
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource parquet(
      @Nullable final String path, @Nullable final Function<String, Set<String>> fileNameMapper) {
    return new ParquetSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(fileNameMapper),
        Map.of(),
        layoutDetection);
  }

  /**
   * Creates a new data source from a directory containing Parquet-encoded FHIR resource data, with
   * filenames representing the resource type the file/directory contains, reading each resource
   * type named in the given map with the schema it supplies.
   *
   * <p>By default the schemas of a resource type's files are merged into their union, which reads
   * the footer of every file. A supplied schema is used as given instead, so it must cover the
   * union of the type's files: columns it omits are not read, and it can lose whole rows, not just
   * columns. A view unnesting a repeating element returns none of a file's rows for that element
   * when the schema omits every field the file carries inside it. Resource types the map does not
   * name are merged as usual.
   *
   * @param path the URI of the directory containing the Parquet files/directories
   * @param schemas the schema to read each resource type with, keyed by resource type code
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource parquetWithSchemas(
      @Nullable final String path, @Nullable final Map<String, StructType> schemas) {
    return new ParquetSource(
        context,
        checkArgumentNotNull(path),
        FileSource::resourceNameWithQualifierMapper,
        checkArgumentNotNull(schemas),
        layoutDetection);
  }

  /**
   * Creates a new data source from a Delta warehouse.
   *
   * @param path the location of the Delta warehouse
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource delta(@Nullable final String path) {
    return new DeltaSource(context, checkArgumentNotNull(path), layoutDetection);
  }

  /**
   * Creates a new data source from a tables registered within the catalog. The table names are
   * assumed to be the same as the resource types they contain.
   *
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource tables() {
    return new CatalogSource(
        context,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        layoutDetection);
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
  public QueryableDataSource tables(@Nullable final String schema) {
    return new CatalogSource(
        context,
        Optional.of(checkArgumentNotNull(schema)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        layoutDetection);
  }

  /**
   * Creates a new data source from a FHIR Bulk Data endpoint.
   *
   * @param client the configured {@link BulkExportClient} that specifies the endpoint and export
   *     parameters
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource bulk(@Nullable final BulkExportClient client) {
    return new BulkDataSource(context, checkArgumentNotNull(client));
  }
}
