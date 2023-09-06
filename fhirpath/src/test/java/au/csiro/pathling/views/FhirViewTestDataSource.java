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

package au.csiro.pathling.views;

import au.csiro.pathling.io.source.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A class for making FHIR data available for the view tests.
 *
 * @author John Grimes
 */
@Slf4j
public class FhirViewTestDataSource implements DataSource {

  private static final Map<ResourceType, Dataset<Row>> resourceTypeToDataset = new HashMap<>();

  public void put(@Nonnull final ResourceType resourceType, @Nonnull final Dataset<Row> dataset) {
    resourceTypeToDataset.put(resourceType, dataset);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final ResourceType resourceType) {
    return resourceTypeToDataset.get(resourceType);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return resourceTypeToDataset.get(ResourceType.fromCode(resourceCode));
  }

  @Nonnull
  @Override
  public Set<ResourceType> getResourceTypes() {
    return resourceTypeToDataset.keySet();
  }

}
