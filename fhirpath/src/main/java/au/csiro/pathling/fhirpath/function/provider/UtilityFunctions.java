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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.sql.TraceExpression;
import jakarta.annotation.Nonnull;
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
   * @param input the input collection
   * @param name a {@link StringCollection} containing the diagnostic label for the trace output
   * @return the input collection, unchanged
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#tracename-string-projection-expression-collection">FHIRPath
   *     Specification - trace</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection trace(
      @Nonnull final Collection input, @Nonnull final StringCollection name) {
    final String label = name.toLiteralValue();
    return input.copyWith(input.getColumn().call(col -> wrapWithTrace(col, label)));
  }

  /**
   * Wraps a Spark Column with a {@link TraceExpression} that logs each evaluated value.
   *
   * @param col the column to wrap
   * @param name the diagnostic label
   * @return a new column that logs values during evaluation
   */
  @Nonnull
  private static Column wrapWithTrace(@Nonnull final Column col, @Nonnull final String name) {
    return column(new TraceExpression(expression(col), name));
  }
}
