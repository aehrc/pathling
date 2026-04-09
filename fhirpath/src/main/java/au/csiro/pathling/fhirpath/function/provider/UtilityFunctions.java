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

package au.csiro.pathling.fhirpath.function.provider;

import static org.apache.spark.sql.classic.ExpressionUtils.column;
import static org.apache.spark.sql.classic.ExpressionUtils.expression;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.sql.TraceCollector;
import au.csiro.pathling.sql.TraceExpression;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;

/**
 * Contains FHIRPath utility functions.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#utility-functions">FHIRPath Specification -
 *     Utility functions</a>
 */
@SuppressWarnings("unused")
public class UtilityFunctions {

  private UtilityFunctions() {}

  /**
   * Adds a string representation of the input collection to the diagnostic log, using the {@code
   * name} argument as the label in the log. Returns the input collection unchanged.
   *
   * <p>When a projection expression is provided, the projected value is logged instead of the input
   * value, but the input collection is still returned unchanged.
   *
   * <p>When a {@link TraceCollector} is available on the evaluation context, each traced value is
   * also added to the collector with the trace label and the FHIR type of the logged expression.
   *
   * <p><strong>Limitation:</strong> The {@code name} argument must be a string literal. Dynamic
   * expressions (e.g., {@code trace(someField)}) are not supported and will raise an error.
   *
   * @param input the input collection
   * @param name a {@link StringCollection} containing the diagnostic label for the trace output
   *     (must be a literal value)
   * @param projection an optional expression to evaluate on the input for logging; when null, the
   *     input value itself is logged
   * @param context the evaluation context, used to obtain the optional trace collector
   * @return the input collection, unchanged
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#tracename-string-projection-expression-collection">FHIRPath
   *     Specification - trace</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection trace(
      @Nonnull final Collection input,
      @Nonnull final StringCollection name,
      @Nullable final CollectionTransform projection,
      @Nonnull final EvaluationContext context) {
    final String label = name.toLiteralValue();
    @Nullable final TraceCollector collector = context.getTraceCollector().orElse(null);

    final Collection toLog = projection != null ? projection.apply(input) : input;
    final String fhirType = toLog.getFhirType().map(t -> t.toCode()).orElse("unknown");
    // Normalise the projected column so that empty arrays become null, preventing trace entries
    // for empty collections.
    final Column toLogColumn = toLog.getColumn().normaliseNull().getValue();

    return input.copyWith(
        input
            .getColumn()
            .call(inputCol -> wrapWithTrace(inputCol, toLogColumn, label, fhirType, collector)));
  }

  /**
   * Wraps a Spark Column with a {@link TraceExpression} that returns the pass-through value while
   * logging the projected value.
   *
   * @param passThrough the column whose value is returned
   * @param toLog the column whose value is logged
   * @param name the diagnostic label
   * @param fhirType the FHIR type code of the logged expression
   * @param collector the optional trace collector, or null
   * @return a new column that logs values during evaluation
   */
  @Nonnull
  private static Column wrapWithTrace(
      @Nonnull final Column passThrough,
      @Nonnull final Column toLog,
      @Nonnull final String name,
      @Nonnull final String fhirType,
      @Nullable final TraceCollector collector) {
    return column(
        new TraceExpression(expression(passThrough), expression(toLog), name, fhirType, collector));
  }
}
