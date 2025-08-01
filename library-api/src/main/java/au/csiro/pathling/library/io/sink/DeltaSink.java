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

import au.csiro.pathling.io.ImportMode;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A data sink that writes data to a Delta Lake table on a filesystem.
 *
 * @author John Grimes
 */
public class DeltaSink implements DataSink {

  // TODO: Review whether we need to pass in the PathlingContext.
  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final ImportMode importMode;

  @Nonnull
  private final String path;

  /**
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the Delta database to
   */
  public DeltaSink(@Nonnull final PathlingContext context, @Nonnull final String path) {
    this.pathlingContext = context;
    this.importMode = ImportMode.OVERWRITE; // Default import mode
    this.path = path;
  }

  /**
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the Delta database to
   * @param importMode the {@link ImportMode} to use, {@link ImportMode#OVERWRITE} will overwrite
   * any existing data, {@link ImportMode#MERGE} will merge the new data with the existing data
   * based on resource ID
   */
  public DeltaSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final ImportMode importMode) {
    this.pathlingContext = context;
    this.importMode = importMode;
    this.path = path;
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final ResourceType resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      dataset.write()
          .format("delta")
          .mode(importMode.getCode())
          .save(path);
    }
  }

}
