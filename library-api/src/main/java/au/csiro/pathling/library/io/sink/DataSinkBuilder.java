/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
 * @author John Grimes
 */
public class DataSinkBuilder {

  /**
   * The Pathling context to use for writing data.
   */
  @Nonnull
  private final PathlingContext context;

  /**
   * The data source containing the data to write.
   */
  @Nonnull
  private final DataSource source;

  /**
   * The save mode to use when writing data.
   */
  @Nonnull
  private SaveMode saveMode = SaveMode.ERROR_IF_EXISTS;

  /**
   * @param context the Pathling context to use for writing data
   * @param source the data source containing the data to write
   */
  public DataSinkBuilder(@Nonnull final PathlingContext context, @Nonnull final DataSource source) {
    this.context = context;
    this.source = source;
  }

  /**
   * Sets the save mode to use when writing data.
   *
   * @param saveMode the save mode to use
   * @return this builder for method chaining
   */
  @Nonnull
  public DataSinkBuilder saveMode(@Nonnull final String saveMode) {
    this.saveMode = SaveMode.fromCode(saveMode);
    return this;
  }

  /**
   * Writes the data in the data source to NDJSON files, one per resource type and named using the
   * "ndjson" extension.
   *
   * @param path the directory to write the files to
   */
  public void ndjson(@Nullable final String path) {
    new NdjsonSink(context, checkArgumentNotNull(path), saveMode).write(source);
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
    new NdjsonSink(context, checkArgumentNotNull(path), saveMode,
        checkArgumentNotNull(fileNameMapper)).write(source);
  }

  /**
   * Writes the data in the data source to Parquet files, one per resource type and named using the
   * "parquet" extension.
   *
   * @param path the directory to write the files to
   */
  public void parquet(@Nullable final String path) {
    new ParquetSink(checkArgumentNotNull(path), saveMode).write(source);
  }

  /**
   * Writes the data in the data source to Parquet files, one per resource type and named using a
   * custom file name mapper.
   *
   * @param path the directory to write the files to
   * @param fileNameMapper a function that maps a resource type to a file name
   */
  public void parquet(@Nullable final String path,
      @Nullable final UnaryOperator<String> fileNameMapper) {
    new ParquetSink(checkArgumentNotNull(path), saveMode,
        checkArgumentNotNull(fileNameMapper)).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database.
   *
   * @param path the directory to write the files to
   * @param deleteOnMerge If merging, whether to delete any resources not found in the source, but found in the destination. 
   */
  public void delta(@Nullable final String path, @Nullable final boolean deleteOnMerge) {
    new DeltaSink(context, checkArgumentNotNull(path), saveMode, deleteOnMerge).write(source);
  }

  /**
   * Writes the data in the data source to a Delta database, named using a custom file name mapper.
   *
   * @param path the directory to write the files to
   * @param fileNameMapper a function that maps a resource type to a file name
   * @param deleteOnMerge If merging, whether to delete any resources not found in the source, but found in the destination. 
   * 
   */
  public void delta(@Nullable final String path,
      @Nullable final UnaryOperator<String> fileNameMapper,
      @Nullable final boolean deleteOnMerge) {
    new DeltaSink(context, checkArgumentNotNull(path), saveMode,
        checkArgumentNotNull(fileNameMapper), deleteOnMerge).write(source);
  }


  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type.
   */
  public void tables() {
    new CatalogSink(context, saveMode).write(source);
  }


  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type and prefixed with the provided schema name.
   *
   * @param schema the schema name to write the tables to
   */
  public void tables(@Nullable final String schema) {
    new CatalogSink(context, saveMode, checkArgumentNotNull(schema)).write(source);
  }

  /**
   * Writes the data in the data source to tables within the Spark catalog, named according to the
   * resource type, using the specified format.
   *
   * @param schema the schema name to write the tables to
   * @param format the table format to use (e.g., "delta", "parquet")
   */
  public void tables(@Nullable final String schema, @Nullable final String format) {
    new CatalogSink(context, saveMode, checkArgumentNotNull(schema),
        checkArgumentNotNull(format)).write(source);
  }


}
