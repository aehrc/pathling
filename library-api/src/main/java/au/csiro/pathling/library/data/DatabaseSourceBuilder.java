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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import javax.annotation.Nonnull;

/**
 * The {@link ReadableSource} builder that binds the client to a persistent data storage using
 * {@link Database} as it's data source. The database is represented as directory with delta files
 * with base names matching the resource types names. The database is referenced to by the warehouse
 * URL and the database name, which combined form the database URL. The database name defaults to
 * 'default'
 *
 * @author Piotr Szul
 */
public class DatabaseSourceBuilder extends AbstractSourceBuilder<DatabaseSourceBuilder> {

  @Nonnull
  private StorageConfiguration.StorageConfigurationBuilder storageConfigurationBuilder = StorageConfiguration.builder();

  protected DatabaseSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
  }

  /**
   * Sets the storage configuration for the {@link Database} instance to use with this client.
   *
   * @param storageConfiguration the storage configuration.
   * @return this builder.
   */
  @Nonnull
  public DatabaseSourceBuilder withStorageConfiguration(
      @Nonnull final StorageConfiguration storageConfiguration) {
    this.storageConfigurationBuilder = storageConfiguration.toBuilder();
    return this;
  }

  /**
   * Sets the URL of the warehouse use with this source.
   *
   * @param warehouseUrl the warehouse URL.
   * @return this builder.
   */
  @Nonnull
  public DatabaseSourceBuilder withWarehouseUrl(@Nonnull final String warehouseUrl) {
    this.storageConfigurationBuilder.warehouseUrl(warehouseUrl);
    return this;
  }

  /**
   * Sets the name of the database to use with this source.
   *
   * @param databaseName the database name.
   * @return this builder.
   */
  @Nonnull
  public DatabaseSourceBuilder withDatabaseName(@Nonnull final String databaseName) {
    this.storageConfigurationBuilder.databaseName(databaseName);
    return this;
  }

  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    return new Database(requireNonNull(storageConfigurationBuilder.build()),
        pathlingContext.getSpark(),
        pathlingContext.getFhirEncoders());
  }

}
