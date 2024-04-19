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
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;

public class CatalogSink extends DatabaseSink {

  public CatalogSink(@Nonnull final PathlingContext context) {
    this(context, ImportMode.OVERWRITE);
  }

  public CatalogSink(@Nonnull final PathlingContext context, @Nonnull final ImportMode importMode) {
    super(Database.forCatalog(context.getSpark(), context.getFhirEncoders(),
        Optional.empty(), true), importMode);
  }

  public CatalogSink(@Nonnull final PathlingContext context, @Nonnull final ImportMode importMode,
      @Nonnull final String schema) {
    super(Database.forCatalog(context.getSpark(), context.getFhirEncoders(),
        Optional.of(schema), true), importMode);
  }

}
