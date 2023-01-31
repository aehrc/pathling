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
package au.csiro.pathling.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * A basic immutable data source that allows for explicit mapping of datasets to resource types.
 */
public class SimpleDataSource implements DataSource {

  @Nonnull
  private final Map<ResourceType, Dataset<Row>> resourceMap;

  public static class Builder {

    private final Map<ResourceType, Dataset<Row>> resourceMap = new HashMap<>();

    @Nonnull
    public Builder withResource(@Nonnull final ResourceType resourceType,
        @Nonnull final Dataset<Row> dataset) {
      resourceMap.put(resourceType, dataset);
      return this;
    }

    @Nonnull
    public SimpleDataSource build() {
      return new SimpleDataSource(resourceMap);
    }
  }

  public SimpleDataSource(
      @Nonnull final Map<ResourceType, Dataset<Row>> resourceMap) {
    this.resourceMap = new HashMap<>(resourceMap);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nonnull final ResourceType resourceType) {
    return resourceMap.computeIfAbsent(resourceType, key -> {
      throw new IllegalStateException(
          String.format("Cannot find data for resource of type: %s", key));
    });
  }
}
