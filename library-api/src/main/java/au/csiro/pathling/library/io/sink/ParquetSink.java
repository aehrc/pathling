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

import static au.csiro.pathling.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.source.DataSource;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a data sink that knows how to read data from a {@link DataSource} and write it to a
 * set of Parquet files.
 *
 * @author John Grimes
 */
public class ParquetSink implements DataSink {

  @Nonnull
  private final String path;

  @Nonnull
  private final SaveMode saveMode;

  /**
   * @param path the path to write the Parquet files to
   * @param saveMode the {@link SaveMode} to use
   */
  public ParquetSink(@Nonnull final String path, @Nonnull final SaveMode saveMode) {
    this.path = path;
    this.saveMode = saveMode;
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final ResourceType resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String resultUrl = safelyJoinPaths(path, resourceType.toCode() + ".parquet");
      dataset.write().mode(saveMode).parquet(resultUrl);
    }
  }

}
