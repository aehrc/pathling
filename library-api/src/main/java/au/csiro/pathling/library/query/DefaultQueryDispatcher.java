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

import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Default implementation of the {@link QueryDispatcher} interface, which uses a
 * {@link FhirViewExecutor} to execute FHIR views.
 *
 * @param viewExecutor the executor used to build and execute FHIR views
 * @author Piotr Szul
 * @author John Grimes
 */
public record DefaultQueryDispatcher(@Nonnull FhirViewExecutor viewExecutor) implements
    QueryDispatcher {

  @Nonnull
  @Override
  public Dataset<Row> dispatch(@Nonnull final FhirView view) {
    return viewExecutor.buildQuery(view);
  }

}
