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
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A a single point for running queries. This exists to decouple the execution of queries from the
 * dispatch, allowing for easier testing and potentially extending the number of types of queries
 * that can be run in the future.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public interface QueryDispatcher {

  /**
   * Dispatches the given view request to be executed and returns the result.
   *
   * @param view the request to execute
   * @return the result of the execution
   */
  @Nonnull
  Dataset<Row> dispatch(@Nonnull FhirView view);
}
