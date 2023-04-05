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

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.query.ImmutableDataSource;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * The {@link ReadableSource} builder that binds the client to an {@link
 * ImmutableDataSource}
 */
public class DirectSourceBuilder extends AbstractSourceBuilder<DirectSourceBuilder> {

  @Nonnull
  private final ImmutableDataSource.Builder inMemoryDataSourceBuilder;

  protected DirectSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
    this.inMemoryDataSourceBuilder = ImmutableDataSource.builder();
  }

  /**
   * Registers a dataset as the source of data the specified resource type.
   *
   * @param resourceType the type of the resource.
   * @param dataset the dataset to use as the source of data.
   * @return this builder.
   */
  @Nonnull
  public DirectSourceBuilder withResource(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> dataset) {
    inMemoryDataSourceBuilder.withResource(resourceType, dataset);
    return this;
  }

  /**
   * Registers a dataset as the source of data the specified resource code.
   *
   * @param resourceCode the code of the resource.
   * @param dataset the dataset to use as the source of data.
   * @return this builder.
   */
  @Nonnull
  public DirectSourceBuilder withResource(@Nonnull final String resourceCode,
      @Nonnull final Dataset<Row> dataset) {
    return withResource(ResourceType.fromCode(resourceCode), dataset);
  }

  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    return inMemoryDataSourceBuilder.build();
  }
}