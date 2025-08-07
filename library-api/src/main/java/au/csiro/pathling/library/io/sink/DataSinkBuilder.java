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

import static au.csiro.pathling.utilities.Preconditions.checkArgumentNotNull;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.function.UnaryOperator;

/**
 * This class knows how to take an @link{EnumerableDataSource} and write it to a variety of
 * different targets.
 *
 * @param context the Pathling context to use for writing data
 * @param source the data source containing the data to write
 * @author John Grimes
 */
public record DataSinkBuilder(
    @Nonnull PathlingContext context,
    @Nonnull DataSource source
) {

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using the
   * "ndjson" extension.
   *
   * @param path the directory to write the files to
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   * </ul>
   */
  public void ndjson(@Nullable final String path, @Nullable final String saveMode) {
    checkArgumentNotNull(path);
    new NdjsonSink(context, path, resolveSaveMode(saveMode)).write(source);
  }

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using a
   * custom file name mapper.
   *
   * @param path the directory to write the files to
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   * </ul>
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void ndjson(@Nullable final String path, @Nullable final String saveMode,
      @Nullable final UnaryOperator<String> fileNameMapper) {
    checkArgumentNotNull(path);
    checkArgumentNotNull(fileNameMapper);
    new NdjsonSink(context, path, resolveSaveMode(saveMode), fileNameMapper).write(source);
  }

  /**
   * Writes the data in the data source to Parquet files, one per resource type and named using the
   * "parquet" extension.
   *
   * @param path the directory to write the files to
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   * </ul>
   */
  public void parquet(@Nullable final String path, @Nullable final String saveMode) {
    checkArgumentNotNull(path);
    new ParquetSink(path, resolveSaveMode(saveMode)).write(source);
  }

  /**
   * Writes the data in the data source to Parquet files, one per resource type and named using a
   * custom file name mapper.
   *
   * @param path the directory to write the files to
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   * </ul>
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void parquet(@Nullable final String path, @Nullable final String saveMode,
      @Nullable final UnaryOperator<String> fileNameMapper) {
    checkArgumentNotNull(path);
    checkArgumentNotNull(fileNameMapper);
    new ParquetSink(path, resolveSaveMode(saveMode), fileNameMapper).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database. If any of the Delta files already
   * exist, an error will be raised.
   *
   * @param path the directory to write the files to
   */
  public void delta(@Nullable final String path) {
    checkArgumentNotNull(path);
    new DeltaSink(context, path).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database, named using a custom file name mapper.
   * If any of the Delta files already exist, an error will be raised.
   *
   * @param path the directory to write the files to
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void delta(@Nullable final String path,
      @Nullable final UnaryOperator<String> fileNameMapper) {
    checkArgumentNotNull(path);
    checkArgumentNotNull(fileNameMapper);
    new DeltaSink(context, path, SaveMode.ERROR_IF_EXISTS, fileNameMapper).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database. Existing data in the Delta files will
   * be dealt with according to the specified {@link SaveMode}.
   *
   * @param path the directory to write the files to
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   *   <li>"merge" - merge the new data with the existing data based on resource ID</li>
   * </ul>
   */
  public void delta(@Nullable final String path, @Nullable final String saveMode) {
    checkArgumentNotNull(path);
    checkArgumentNotNull(saveMode);
    new DeltaSink(context, path, SaveMode.fromCode(saveMode)).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database, named using a custom file name mapper.
   * Existing data in the Delta files will be dealt with according to the specified
   * {@link SaveMode}.
   *
   * @param path the directory to write the files to
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   *   <li>"merge" - merge the new data with the existing data based on resource ID</li>
   * </ul>
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void delta(@Nullable final String path, @Nullable final String saveMode,
      @Nullable final UnaryOperator<String> fileNameMapper) {
    checkArgumentNotNull(path);
    checkArgumentNotNull(saveMode);
    checkArgumentNotNull(fileNameMapper);
    new DeltaSink(context, path, SaveMode.fromCode(saveMode), fileNameMapper).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type.
   * <p>
   * If any of the tables already exist, an error will be raised.
   */
  public void tables() {
    new CatalogSink(context).write(source);
  }


  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type. Existing data in the tables will be dealt with according to the specified
   * {@link SaveMode}.
   *
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   *   <li>"merge" - merge the new data with the existing data based on resource ID (this is only
   *   supported where the managed table format is Delta)</li>
   * </ul>
   */
  public void tables(@Nullable final String saveMode) {
    checkArgumentNotNull(saveMode);
    new CatalogSink(context, SaveMode.fromCode(saveMode)).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type and prefixed with the provided schema name. Existing data in the tables will be
   * dealt with according to the specified {@link SaveMode}.
   *
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   *   <li>"merge" - merge the new data with the existing data based on resource ID (this is only
   *   supported where the managed table format is Delta)</li>
   * </ul>
   * @param schema the schema name to write the tables to
   */
  public void tables(@Nullable final String saveMode, @Nullable final String schema) {
    checkArgumentNotNull(saveMode);
    checkArgumentNotNull(schema);
    new CatalogSink(context, SaveMode.fromCode(saveMode), schema).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type, using the specified format. Existing data in the tables will be dealt with
   * according to the specified {@link SaveMode}.
   *
   * @param saveMode the save mode to use:
   * <ul>
   *   <li>"error" - throw an error if the files already exist</li>
   *   <li>"overwrite" - overwrite any existing files</li>
   *   <li>"append" - append to any existing files</li>
   *   <li>"ignore" - do nothing if the files already exist</li>
   *   <li>"merge" - merge the new data with the existing data based on resource ID (this is only
   *   supported where the managed table format is Delta)</li>
   * </ul>
   * @param schema the schema name to write the tables to
   * @param format the table format to use (e.g., "delta", "parquet")
   */
  public void tables(@Nullable final String saveMode, @Nullable final String schema,
      @Nullable final String format) {
    checkArgumentNotNull(saveMode);
    checkArgumentNotNull(schema);
    checkArgumentNotNull(format);
    new CatalogSink(context, SaveMode.fromCode(saveMode), schema, format).write(source);
  }

  @Nonnull
  private static SaveMode resolveSaveMode(final @Nullable String saveMode) {
    return saveMode == null
           ? SaveMode.ERROR_IF_EXISTS
           : SaveMode.fromCode(saveMode);
  }

}
