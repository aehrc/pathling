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
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;

/**
 * Represents a data sink that knows how to read data from a {@link DataSource} and write it to a
 * Delta database.
 *
 * @author John Grimes
 */
public class DeltaSink extends DatabaseSink {

  /**
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the Delta database to
   */
  public DeltaSink(@Nonnull final PathlingContext context, @Nonnull final String path) {
    this(context, path, ImportMode.OVERWRITE);
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
    super(Database.forFileSystem(context.getSpark(), context.getFhirEncoders(), path, true),
        importMode);
  }

}
