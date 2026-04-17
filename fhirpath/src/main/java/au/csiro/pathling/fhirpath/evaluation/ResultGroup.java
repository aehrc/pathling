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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TraceResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TypedValue;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Represents the results and traces for a single evaluation scope. When a context expression is
 * provided, each context element produces its own {@code ResultGroup}. When no context expression
 * is provided, a single {@code ResultGroup} with a null context key is returned.
 *
 * @author John Grimes
 */
public class ResultGroup {

  @Nullable private final String contextKey;

  @Nonnull private final List<TypedValue> results;

  @Nonnull private final List<TraceResult> traces;

  /**
   * Creates a new ResultGroup.
   *
   * @param contextKey the context key identifying this group's context element, or null for
   *     non-context evaluation
   * @param results the typed result values from the evaluation
   * @param traces the trace entries collected during evaluation
   */
  public ResultGroup(
      @Nullable final String contextKey,
      @Nonnull final List<TypedValue> results,
      @Nonnull final List<TraceResult> traces) {
    this.contextKey = contextKey;
    this.results = results;
    this.traces = traces;
  }

  /**
   * Gets the context key identifying this group's context element.
   *
   * @return the context key, or null for non-context evaluation
   */
  @Nullable
  public String getContextKey() {
    return contextKey;
  }

  /**
   * Gets the typed result values from the evaluation.
   *
   * @return the list of typed values
   */
  @Nonnull
  public List<TypedValue> getResults() {
    return results;
  }

  /**
   * Gets the trace entries collected during evaluation of this context element.
   *
   * @return the list of trace results
   */
  @Nonnull
  public List<TraceResult> getTraces() {
    return traces;
  }
}
