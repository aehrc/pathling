/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library.query;

import au.csiro.pathling.search.FhirSearch;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A single point for executing FHIR search queries. This exists to decouple the execution of
 * search queries from the dispatch, allowing for easier testing and potential extension.
 *
 * @author Piotr Szul
 * @see FhirSearchQuery
 * @see QueryDispatcher
 */
@FunctionalInterface
public interface SearchDispatcher {

  /**
   * Dispatches the given search request to be executed and returns the result.
   *
   * @param resourceType the FHIR resource type to search
   * @param search the search criteria to apply
   * @return the filtered dataset containing matching resources
   */
  @Nonnull
  Dataset<Row> dispatch(@Nonnull ResourceType resourceType, @Nonnull FhirSearch search);

}
