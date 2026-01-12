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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import java.util.List;

/**
 * Represents the definition of a FHIR search parameter.
 * <p>
 * For polymorphic search parameters (e.g., Observation.effective which can be dateTime, Period,
 * Timing, or instant), multiple FHIRPath expressions can be specified. Each expression is
 * evaluated separately and the filter results are combined with OR logic.
 *
 * @param code the code that identifies this search parameter (e.g., "gender")
 * @param type the type of the search parameter
 * @param expressions the FHIRPath expressions that define the values for this parameter
 * @see <a href="https://hl7.org/fhir/searchparameter.html">SearchParameter</a>
 */
public record SearchParameterDefinition(
    @Nonnull String code,
    @Nonnull SearchParameterType type,
    @Nonnull List<String> expressions
) {

  /**
   * Compact constructor that ensures the expressions list is immutable.
   *
   * @param code the parameter code
   * @param type the parameter type
   * @param expressions the FHIRPath expressions
   */
  public SearchParameterDefinition {
    expressions = List.copyOf(expressions);
  }

  /**
   * Creates a search parameter definition with a single expression.
   * <p>
   * This is a convenience constructor for simple search parameters that have only one FHIRPath
   * expression.
   *
   * @param code the parameter code
   * @param type the parameter type
   * @param expression the FHIRPath expression
   */
  public SearchParameterDefinition(@Nonnull final String code,
      @Nonnull final SearchParameterType type,
      @Nonnull final String expression) {
    this(code, type, List.of(expression));
  }
}
