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

package au.csiro.pathling.library.io.sink;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.io.ImportMode;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/**
 * This class knows how to take an @link{EnumerableDataSource} and write it to a variety of
 * different targets.
 *
 * @author John Grimes
 */
public class DataSinkBuilder {

  @Nonnull
  private final PathlingContext context;

  @Nonnull
  private final DataSource source;

  public DataSinkBuilder(@Nonnull final PathlingContext context, @Nonnull final DataSource source) {
    this.context = context;
    this.source = source;
  }

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using the
   * "ndjson" extension.
   *
   * @param path the directory to write the files to
   */
  public void ndjson(@Nullable final String path) {
    new NdjsonSink(context, requireNonNull(path)).write(source);
  }

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using a
   * custom file name mapper.
   *
   * @param path the directory to write the files to
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void ndjson(@Nullable final String path,
      @Nullable final UnaryOperator<String> fileNameMapper) {
    new NdjsonSink(context, requireNonNull(path), requireNonNull(fileNameMapper)).write(source);
  }

  /**
   * Writes the data in the data source to Parquet files, one per resource type and named using the
   * "parquet" extension.
   *
   * @param path the directory to write the files to
   */
  public void parquet(@Nullable final String path) {
    new ParquetSink(requireNonNull(path)).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database. Any existing data in the Delta files
   * will be overwritten.
   *
   * @param path the directory to write the files to
   */
  public void delta(@Nullable final String path) {
    new DeltaSink(context, requireNonNull(path)).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database. Existing data in the Delta files will
   * be dealt with according to the specified {@link ImportMode}.
   *
   * @param path the directory to write the files to
   * @param importMode the import mode to use, "overwrite" will overwrite any existing data, "merge"
   * will merge the new data with the existing data based on resource ID
   */
  public void delta(@Nullable final String path, @Nullable final String importMode) {
    new DeltaSink(context, requireNonNull(path), ImportMode.fromCode(importMode)).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type.
   * <p>
   * Any existing data in the tables will be overwritten.
   */
  public void tables() {
    new CatalogSink(context).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type. Existing data in the tables will be dealt with according to the specified
   * {@link ImportMode}.
   *
   * @param importMode the import mode to use, "overwrite" will overwrite any existing data, "merge"
   * will merge the new data with the existing data based on resource ID
   */
  public void tables(@Nullable final String importMode) {
    new CatalogSink(context, ImportMode.fromCode(importMode)).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type and prefixed with the provided schema name. Existing data in the tables will be
   * dealt with according to the specified {@link ImportMode}.
   *
   * @param importMode the import mode to use, "overwrite" will overwrite any existing data, "merge"
   * will merge the new data with the existing data based on resource ID
   * @param schema the schema name to write the tables to
   */
  public void tables(@Nullable final String importMode, @Nullable final String schema) {
    new CatalogSink(context, ImportMode.fromCode(importMode), requireNonNull(schema)).write(source);
  }

}
