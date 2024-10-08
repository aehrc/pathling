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

package au.csiro.pathling.library.io.source;

import au.csiro.pathling.io.Database;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * A class for making FHIR data in the Spark catalog available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class CatalogSource extends DatabaseSource {

  public CatalogSource(@Nonnull final PathlingContext context) {
    super(context, buildDatabase(context, Optional.empty()));
  }

  public CatalogSource(@Nonnull final PathlingContext context, @Nonnull final String schema) {
    super(context, buildDatabase(context, Optional.of(schema)));
  }

  @Nonnull
  private static Database buildDatabase(final @Nonnull PathlingContext context,
      final Optional<String> schema) {
    return Database.forCatalog(context.getSpark(), context.getFhirEncoders(),
        schema, true);
  }

}
