/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.read;

import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.ResourceEncoding;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.stereotype.Component;

/**
 * Executes read operations to fetch a single resource by ID from the data source.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/http.html#read">FHIR read operation</a>
 */
@Component
@Slf4j
public class ReadExecutor {

  @Nonnull private final DataSource dataSource;

  @Nonnull private final PathlingContext pathlingContext;

  /**
   * Constructs a new ReadExecutor.
   *
   * @param dataSource the data source containing the resources to read
   * @param pathlingContext the Pathling context, used to decode rows into FHIR resources
   */
  public ReadExecutor(
      @Nonnull final DataSource dataSource, @Nonnull final PathlingContext pathlingContext) {
    this.dataSource = dataSource;
    this.pathlingContext = pathlingContext;
  }

  /**
   * Reads a single resource by its ID.
   *
   * @param resourceTypeCode the type code of the resource (e.g., "Patient", "ViewDefinition")
   * @param resourceId the logical ID of the resource to read
   * @return the resource with the specified ID
   * @throws ResourceNotFoundError if no resource with the specified ID exists
   * @throws IllegalArgumentException if the resource ID is null, empty, or blank
   */
  @Nonnull
  public IBaseResource read(
      @Nonnull final String resourceTypeCode, @Nonnull final String resourceId) {
    // Validate input.
    if (resourceId == null || resourceId.isBlank()) {
      throw new IllegalArgumentException("Resource ID must not be null, empty, or blank");
    }

    log.info("Reading {} with ID: {}", resourceTypeCode, resourceId);

    // Read the dataset for this resource type.
    final Dataset<Row> dataset = dataSource.read(resourceTypeCode);

    // Filter by ID.
    final Dataset<Row> filtered = dataset.filter(dataset.col("id").equalTo(resourceId));

    // Decode the matching rows into resources.
    final List<IBaseResource> resources =
        ResourceEncoding.decode(pathlingContext, resourceTypeCode, filtered);

    if (resources.isEmpty()) {
      throw new ResourceNotFoundError(resourceTypeCode + " with ID '" + resourceId + "' not found");
    }

    return resources.get(0);
  }
}
