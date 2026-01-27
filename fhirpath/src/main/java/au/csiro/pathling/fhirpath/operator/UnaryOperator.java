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

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;

/**
 * Represents a unary operator in FHIRPath.
 *
 * @author Piotr Szul
 */
public interface UnaryOperator {

  /**
   * Represents the input to a unary operator.
   *
   * @param input A {@link Collection} object representing the input to the operator
   */
  record UnaryOperatorInput(@Nonnull Collection input) {}

  /**
   * Invokes this operator with the specified input.
   *
   * @param input An {@link UnaryOperatorInput} object
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  Collection invoke(UnaryOperatorInput input);

  /**
   * Gets the name of this operator.
   *
   * @return the operator name, defaults to the simple class name
   */
  default String getOperatorName() {
    return this.getClass().getSimpleName();
  }
}
