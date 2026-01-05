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

package au.csiro.pathling.fhirpath.context;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Interface for resolving FHIR resources during FHIRPath evaluation.
 *
 * <p>The ResourceResolver is responsible for:
 *
 * <ul>
 *   <li>Providing access to the subject resource (the primary resource being queried)
 *   <li>Resolving references to other resources
 *   <li>Handling forward and reverse resource joins
 *   <li>Creating the underlying dataset view for query execution
 * </ul>
 *
 * <p>Implementations of this interface manage the association of requested resource collection with
 * the underlying representation in the Spark dataset which is essential for evaluating FHIRPath
 * expressions that traverse resource boundaries.
 */
public interface ResourceResolver {

  /**
   * Resolves the subject resource for this context.
   *
   * <p>The subject resource is the primary resource type being queried, such as Patient in a query
   * starting with Patient.name.given.
   *
   * @return A ResourceCollection representing the subject resource
   */
  @Nonnull
  ResourceCollection resolveSubjectResource();

  /**
   * Resolves a resource by its type code.
   *
   * <p>This method is used to access resources by their type name, such as "Patient" or
   * "Observation".
   *
   * @param resourceCode The FHIR resource type code (e.g., "Patient", "Observation")
   * @return An Optional containing the ResourceCollection if the resource type exists, or empty if
   *     it doesn't
   */
  @Nonnull
  Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode);

  /**
   * Creates the underlying dataset view for query execution.
   *
   * <p>This method returns the Spark Dataset that contains the data for the subject resource and
   * any joined resources. It's used as the starting point for evaluating FHIRPath expressions.
   *
   * @return A Spark Dataset containing the view data
   */
  @Nonnull
  Dataset<Row> createView();
}
