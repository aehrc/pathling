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

import static au.csiro.pathling.fhir.FhirUtils.getResourceType;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A class for making FHIR data with Spark datasets available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class DatasetSource extends AbstractSource {

  @Nonnull
  protected Map<ResourceType, Dataset<Row>> resourceMap = new HashMap<>();

  public DatasetSource(@Nonnull final PathlingContext context) {
    super(context);
  }

  /**
   * Add a dataset to this source.
   *
   * @param resourceType the resource type
   * @param dataset the dataset
   * @return this data source, for chaining
   */
  public DatasetSource dataset(@Nullable final ResourceType resourceType,
      @Nullable final Dataset<Row> dataset) {
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
  public DatasetSource dataset(@Nullable final String resourceCode,
      @Nullable final Dataset<Row> dataset) {
    resourceMap.put(getResourceType(resourceCode), requireNonNull(dataset));
    return this;
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    final ResourceType resourceType = requireNonNull(ResourceType.fromCode(resourceCode));
    return Optional.ofNullable(resourceMap.get(requireNonNull(resourceType)))
        .orElseThrow(() -> new IllegalArgumentException(
            "No data found for resource type: " + resourceType));
  }

  @Override
  public @Nonnull Set<String> getResourceTypes() {
    return resourceMap.keySet().stream().map(ResourceType::toCode)
        .collect(Collectors.toSet());
  }

}
