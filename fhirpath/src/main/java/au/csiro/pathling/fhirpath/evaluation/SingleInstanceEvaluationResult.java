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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/**
 * Represents the result of evaluating a FHIRPath expression against a single FHIR resource.
 *
 * @author John Grimes
 */
public class SingleInstanceEvaluationResult {

  @Nonnull private final List<TypedValue> results;

  @Nonnull private final String expectedReturnType;

  @Nonnull private final List<TraceResult> traces;

  /**
   * Creates a new SingleInstanceEvaluationResult.
   *
   * @param results the typed result values from the evaluation
   * @param expectedReturnType the statically inferred return type of the expression
   * @param traces the trace entries collected during evaluation
   */
  public SingleInstanceEvaluationResult(
      @Nonnull final List<TypedValue> results,
      @Nonnull final String expectedReturnType,
      @Nonnull final List<TraceResult> traces) {
    this.results = results;
    this.expectedReturnType = expectedReturnType;
    this.traces = traces;
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
   * Gets the statically inferred return type of the expression.
   *
   * @return the expected return type as a FHIR type name
   */
  @Nonnull
  public String getExpectedReturnType() {
    return expectedReturnType;
  }

  /**
   * Gets the trace entries collected during evaluation.
   *
   * @return the list of trace results, or an empty list if no trace() calls were present
   */
  @Nonnull
  public List<TraceResult> getTraces() {
    return traces;
  }

  /**
   * Represents a single typed value in the evaluation result.
   *
   * @author John Grimes
   */
  public static class TypedValue {

    @Nonnull private final String type;

    @Nullable private final Object value;

    /**
     * Creates a new TypedValue.
     *
     * @param type the FHIR type name of the value
     * @param value the materialised value, or null if the value is null
     */
    public TypedValue(@Nonnull final String type, @Nullable final Object value) {
      this.type = type;
      this.value = value;
    }

    /**
     * Gets the FHIR type name of the value.
     *
     * @return the type name
     */
    @Nonnull
    public String getType() {
      return type;
    }

    /**
     * Gets the materialised value.
     *
     * @return the value, or null
     */
    @Nullable
    public Object getValue() {
      return value;
    }
  }

  /**
   * Represents a trace output from a {@code trace()} call during evaluation.
   *
   * @author John Grimes
   */
  public static class TraceResult {

    @Nonnull private final String label;

    @Nonnull private final List<TypedValue> values;

    /**
     * Creates a new TraceResult.
     *
     * @param label the trace label (the name argument to trace())
     * @param values the traced values as typed values
     */
    public TraceResult(@Nonnull final String label, @Nonnull final List<TypedValue> values) {
      this.label = label;
      this.values = values;
    }

    /**
     * Gets the trace label.
     *
     * @return the label
     */
    @Nonnull
    public String getLabel() {
      return label;
    }

    /**
     * Gets the traced values.
     *
     * @return the list of typed values
     */
    @Nonnull
    public List<TypedValue> getValues() {
      return values;
    }
  }
}
