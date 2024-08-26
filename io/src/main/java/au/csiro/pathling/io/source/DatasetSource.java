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

package au.csiro.pathling.io.source;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A class for making FHIR data with Spark datasets available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class DatasetSource implements DataSource {

  @NotNull
  protected Map<ResourceType, Dataset<Row>> resourceMap = new HashMap<>();

  /**
   * Add a dataset to this source.
   *
   * @param resourceType the resource type
   * @param dataset the dataset
   * @return this data source, for chaining
   */
  @NotNull
  public DatasetSource dataset(@NotNull final ResourceType resourceType,
      @NotNull final Dataset<Row> dataset) {
    resourceMap.put(requireNonNull(resourceType), requireNonNull(dataset));
    return this;
  }

  /**
   * Add a dataset to this source.
   *
   * @param resourceCode the resource code
   * @param dataset the dataset
   * @return this data source, for chaining
   */
  @NotNull
  public DatasetSource dataset(@NotNull final String resourceCode,
      @NotNull final Dataset<Row> dataset) {
    resourceMap.put(ResourceType.fromCode(resourceCode), requireNonNull(dataset));
    return this;
  }

  @Override
  @NotNull
  public Dataset<Row> read(@NotNull final ResourceType resourceType) {
    final Dataset<Row> dataset = resourceMap.get(resourceType);
    if (dataset == null) {
      throw new IllegalArgumentException("No data found for resource type: " + resourceType);
    }
    return dataset;
  }

  @Override
  @NotNull
  public Dataset<Row> read(@NotNull final String resourceCode) {
    final ResourceType resourceType = ResourceType.fromCode(resourceCode);
    if (resourceType == null) {
      throw new IllegalArgumentException("Invalid resource type code: " + resourceCode);
    }
    return read(resourceType);
  }

  @Override
  @NotNull
  public Set<ResourceType> getResourceTypes() {
    return resourceMap.keySet();
  }

}
