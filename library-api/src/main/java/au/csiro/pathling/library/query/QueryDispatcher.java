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

package au.csiro.pathling.library.query;

import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A class that can be used as a single point for running aggregate and extract queries.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class QueryDispatcher {

  @Nonnull
  private final FhirViewExecutor viewExecutor;

  public QueryDispatcher(
      @Nonnull final FhirViewExecutor viewExecutor) {
    this.viewExecutor = viewExecutor;
  }


  /**
   * Dispatches the given view request to the relevant executor and returns the result.
   *
   * @param view the request to execute
   * @return the result of the execution
   */
  @Nonnull
  public Dataset<Row> dispatch(@Nonnull final FhirView view) {
    return viewExecutor.buildQuery(view);
  }

}
