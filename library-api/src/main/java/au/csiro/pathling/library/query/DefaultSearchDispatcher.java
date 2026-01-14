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
import au.csiro.pathling.search.FhirSearchExecutor;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Default implementation of the {@link SearchDispatcher} interface, which uses a
 * {@link FhirSearchExecutor} to execute FHIR search queries.
 *
 * @param searchExecutor the executor used to execute FHIR search queries
 * @author Piotr Szul
 * @see FhirSearchExecutor
 */
public record DefaultSearchDispatcher(
    @Nonnull FhirSearchExecutor searchExecutor
) implements SearchDispatcher {

  @Nonnull
  @Override
  public Dataset<Row> dispatch(@Nonnull final ResourceType resourceType,
      @Nonnull final FhirSearch search) {
    return searchExecutor.execute(resourceType, search);
  }

}
