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

import au.csiro.pathling.library.PathlingContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
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
  public DatasetSource dataset(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> dataset) {
    resourceMap.put(resourceType, dataset);
    return this;
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nonnull final ResourceType resourceType) {
    return Optional.ofNullable(resourceMap.get(resourceType))
        .orElseThrow(() -> new IllegalArgumentException(
            "No data found for resource type: " + resourceType));
  }

  @Nonnull
  @Override
  public Set<ResourceType> getResourceTypes() {
    return resourceMap.keySet();
  }

}
