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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;

/**
 * Represents a binary operator in FHIRPath.
 *
 * @author John Grimes
 */
public interface FhirPathBinaryOperator {

  /**
   * Invokes this operator with the specified inputs.
   *
   * @param input An {@link BinaryOperatorInput} object
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  Collection invoke(@Nonnull BinaryOperatorInput input);

  /**
   * Invokes this operator with unevaluated operand paths.
   *
   * <p>This method allows operators to control when and how their operands are evaluated. The
   * default implementation evaluates both paths and delegates to {@link
   * #invoke(BinaryOperatorInput)}.
   *
   * <p>Operators with special evaluation needs (e.g., type operators that need to extract type
   * specifiers at evaluation time) can override this method.
   *
   * @param context the evaluation context
   * @param input the input collection
   * @param leftPath the unevaluated left operand path
   * @param rightPath the unevaluated right operand path
   * @return the result collection
   */
  @Nonnull
  default Collection invokeWithPaths(
      @Nonnull final EvaluationContext context,
      @Nonnull final Collection input,
      @Nonnull final FhirPath leftPath,
      @Nonnull final FhirPath rightPath) {
    // Default: evaluate both paths and call invoke()
    final Collection leftValue = leftPath.apply(input, context);
    final Collection rightValue = rightPath.apply(input, context);
    return invoke(new BinaryOperatorInput(context, leftValue, rightValue));
  }

  /**
   * Gets the name of this operator, typically the simple class name.
   *
   * @return the name of this operator
   */
  @Nonnull
  default String getOperatorName() {
    return this.getClass().getSimpleName();
  }
}
