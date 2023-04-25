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

package au.csiro.pathling.library.data;

import au.csiro.pathling.io.Database;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A builder for creating a data source from the Spark table catalog.
 *
 * @author John Grimes
 */
public class CatalogSourceBuilder extends AbstractSourceBuilder<CatalogSourceBuilder> {

  @Nullable
  private String schema;

  protected CatalogSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
  }

  /**
   * Sets the schema from which the tables will be loaded.
   *
   * @param schema the schema name
   * @return this builder
   */
  @Nonnull
  public CatalogSourceBuilder withSchema(@Nullable final String schema) {
    this.schema = schema;
    return this;
  }

  /**
   * @return a new data source, built using the Spark table catalog and the supplied options
   */
  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    return Database.forCatalog(pathlingContext.getSpark(), pathlingContext.getFhirEncoders(),
        Optional.ofNullable(schema), true);
  }

}
