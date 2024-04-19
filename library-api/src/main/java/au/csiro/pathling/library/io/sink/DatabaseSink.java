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

import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.ImportMode;
import au.csiro.pathling.io.source.DataSource;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A data sink that writes data to a {@link Database}.
 *
 * @author John Grimes
 */
public abstract class DatabaseSink implements DataSink {

  @Nonnull
  protected final ImportMode importMode;

  @Nonnull
  protected final Database database;

  /**
   * @param database the database to write to
   * @param importMode the {@link ImportMode} to use when writing data
   */
  public DatabaseSink(@Nonnull final Database database, @Nonnull final ImportMode importMode) {
    this.importMode = importMode;
    this.database = database;
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final ResourceType resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      if (importMode.equals(ImportMode.OVERWRITE)) {
        database.overwrite(resourceType, dataset);
      } else if (importMode.equals(ImportMode.MERGE)) {
        database.merge(resourceType, dataset);
      } else {
        throw new IllegalArgumentException("Unsupported import mode: " + importMode);
      }
    }
  }

}
