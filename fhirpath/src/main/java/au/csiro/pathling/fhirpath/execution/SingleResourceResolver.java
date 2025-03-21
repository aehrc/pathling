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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A simple implementation of {@link BaseResourceResolver} that provides access to a single FHIR
 * resource type.
 * <p>
 * This resolver is designed for basic FHIRPath evaluation scenarios where only one resource type
 * is needed and no joins between resources are required. It:
 * <ul>
 *   <li>Reads data for a single resource type from a data source</li>
 *   <li>Provides access to that resource as the subject resource</li>
 *   <li>Does not support resolving references to other resources</li>
 * </ul>
 * <p>
 * This class is useful for simple queries or as a building block for more complex resolvers
 * that need to handle multiple resource types and relationships.
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class SingleResourceResolver extends BaseResourceResolver {

  /**
   * The resource type that this resolver provides access to.
   */
  @Nonnull
  ResourceType subjectResource;

  /**
   * The FHIR context used for resource definitions.
   */
  @Nonnull
  FhirContext fhirContext;
  
  /**
   * The data source from which to read the resource data.
   */
  @Nonnull
  DataSource dataSource;

  /**
   * {@inheritDoc}
   * <p>
   * This implementation creates a view containing only the subject resource data
   * from the data source.
   */
  @Override
  @Nonnull
  public Dataset<Row> createView() {
    return getResourceDataset(dataSource, subjectResource);
  }
}
