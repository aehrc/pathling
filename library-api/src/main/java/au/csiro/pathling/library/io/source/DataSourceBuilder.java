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
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.SchemaSet;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
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
 * <p>Every argument to the methods of this builder must be non-null. A null argument is rejected
 * with an {@link IllegalArgumentException}.
 *
 * <p>A builder given a schema set with {@link #withSchemas(Map)} reads each resource type the set
 * names with the set's schema, in every source it creates, and infers every other type as usual.
 *
 * @param context the {@link PathlingContext} to use for creating the data sources
 * @param layoutDetection whether the Parquet, Delta and catalog sources this builder creates
 *     classify the layout of the data they read
 * @param schemaSet the schema of each resource type the sources this builder creates read with,
 *     keyed by resource type code; an empty set means no schemas
 * @author Piotr Szul
 * @author John Grimes
 */
public record DataSourceBuilder(
    @Nonnull PathlingContext context,
    boolean layoutDetection,
    @Nonnull Map<String, StructType> schemaSet) {

  /**
   * Creates a builder, refusing a schema set that names something other than a resource type, or
   * that has a null schema.
   *
   * @param context the {@link PathlingContext} to use for creating the data sources
   * @param layoutDetection whether the Parquet, Delta and catalog sources this builder creates
   *     classify the layout of the data they read
   * @param schemaSet the schema of each resource type, keyed by resource type code
   * @throws IllegalArgumentException if a key is not a resource type code, or its schema is null,
   *     naming the key
   */
  public DataSourceBuilder {
    checkArgumentNotNull(context);
    final SchemaSet given = SchemaSet.of(checkArgumentNotNull(schemaSet));
    // The definitions are only consulted when there is a key to check against them.
    if (!given.isEmpty()) {
      given.requireResourceTypes(FhirDefinitionContext.of(context.getFhirContext()));
    }
    schemaSet = given.schemas();
  }

  /**
   * Creates a builder whose sources classify the layout of the data they read, with no schema set.
   *
   * @param context the {@link PathlingContext} to use for creating the data sources
   */
  public DataSourceBuilder(@Nonnull final PathlingContext context) {
    this(context, true, Map.of());
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
    return new DataSourceBuilder(context, layoutDetection, schemaSet);
  }

  /**
   * Returns a builder whose every source projects each resource type the given schema set names to
   * its schema, and infers every other type as usual.
   *
   * <p>A named type has the set's schema, in its order, with the annotations the configuration
   * enables recomputed rather than taken from the given schema. Elements the schema lacks are
   * dropped, and elements the data lacks are null. A stored field whose name differs from the
   * schema's only in case fails the read, naming the resource type and both paths.
   *
   * @param schemaSet the schema of each resource type to project, keyed by resource type code
   * @return a builder with the given schema set, in place of any this builder has
   * @throws IllegalArgumentException if a key is not a resource type code, or its schema is null,
   *     naming the key
   */
  @Nonnull
  public DataSourceBuilder withSchemas(@Nonnull final Map<String, StructType> schemaSet) {
    return new DataSourceBuilder(context, layoutDetection, schemaSet);
  }

  /** Returns the schema set of this builder in the form the sources take it. */
  @Nonnull
  private SchemaSet schemas() {
    return SchemaSet.of(schemaSet);
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
  public QueryableDataSource ndjson(@Nonnull final String ndjsonDir) {
    return new NdjsonSource(
        context,
        checkArgumentNotNull(ndjsonDir),
        "ndjson",
        FileSource::resourceNameWithQualifierMapper,
        schemas());
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
  public QueryableDataSource ndjson(@Nonnull final String path, @Nonnull final String extension) {
    return new NdjsonSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(extension),
        FileSource::resourceNameWithQualifierMapper,
        schemas());
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
      @Nonnull final String path,
      @Nonnull final String extension,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    return new NdjsonSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(extension),
        checkArgumentNotNull(fileNameMapper),
        schemas());
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
      @Nonnull final String path,
      @Nonnull final Set<String> resourceTypes,
      @Nonnull final String mimeType) {
    return new BundlesSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(mimeType),
        checkArgumentNotNull(resourceTypes),
        schemas());
  }

  /**
   * Creates a new data source from Spark datasets.
   *
   * @return a {@link DatasetSource}, which can then be populated with datasets that are mapped to
   *     the resource types that they contain, using the {@link DatasetSource#dataset} method.
   */
  @Nonnull
  public DatasetSource datasets() {
    return new DatasetSource(context, schemas());
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
  public QueryableDataSource parquet(@Nonnull final String path) {
    return new ParquetSource(
        context,
        checkArgumentNotNull(path),
        FileSource::resourceNameWithQualifierMapper,
        schemas(),
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
      @Nonnull final String path, @Nonnull final Function<String, Set<String>> fileNameMapper) {
    return new ParquetSource(
        context,
        checkArgumentNotNull(path),
        checkArgumentNotNull(fileNameMapper),
        schemas(),
        layoutDetection);
  }

  /**
   * Creates a new data source from a Delta warehouse.
   *
   * @param path the location of the Delta warehouse
   * @return the new data source
   */
  @Nonnull
  public QueryableDataSource delta(@Nonnull final String path) {
    return new DeltaSource(context, checkArgumentNotNull(path), schemas(), layoutDetection);
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
        schemas(),
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
  public QueryableDataSource tables(@Nonnull final String schema) {
    return new CatalogSource(
        context,
        Optional.of(checkArgumentNotNull(schema)),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        schemas(),
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
  public QueryableDataSource bulk(@Nonnull final BulkExportClient client) {
    return new BulkDataSource(context, checkArgumentNotNull(client), schemas());
  }
}
