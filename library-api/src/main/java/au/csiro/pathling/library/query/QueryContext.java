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

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.search.FhirSearchExecutor;
import au.csiro.pathling.views.FhirViewExecutor;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;

/**
 * Unified context for executing FHIR queries against a data source.
 *
 * <p>This class encapsulates the executors for both FHIR View and FHIR Search queries, providing a
 * single point of configuration for query execution.
 *
 * @author Piotr Szul
 */
public class QueryContext {

  @Nonnull private final FhirViewExecutor viewExecutor;

  @Nonnull private final FhirSearchExecutor searchExecutor;

  @Nonnull private final Gson gson;

  /**
   * Creates a new QueryContext with the specified dependencies.
   *
   * @param context the Pathling context providing configuration and dependencies
   * @param dataSource the data source to query against
   */
  public QueryContext(
      @Nonnull final PathlingContext context, @Nonnull final DataSource dataSource) {
    this.viewExecutor =
        new FhirViewExecutor(context.getFhirContext(), dataSource, context.getQueryConfiguration());
    this.searchExecutor =
        FhirSearchExecutor.withDefaultRegistry(context.getFhirContext(), dataSource);
    this.gson = context.getGson();
  }

  /**
   * Gets the FHIR view executor.
   *
   * @return the view executor
   */
  @Nonnull
  public FhirViewExecutor getViewExecutor() {
    return viewExecutor;
  }

  /**
   * Gets the FHIR search executor.
   *
   * @return the search executor
   */
  @Nonnull
  public FhirSearchExecutor getSearchExecutor() {
    return searchExecutor;
  }

  /**
   * Gets the Gson instance for JSON serialization.
   *
   * @return the Gson instance
   */
  @Nonnull
  public Gson getGson() {
    return gson;
  }
}
