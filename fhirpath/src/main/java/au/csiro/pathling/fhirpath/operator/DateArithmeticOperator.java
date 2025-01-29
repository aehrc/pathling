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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import jakarta.annotation.Nonnull;

/**
 * Provides the functionality of the family of math operators within FHIRPath, i.e. +, -, *, / and
 * mod.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#math">Math</a>
 */
public class DateArithmeticOperator implements BinaryOperator {

  @Nonnull
  private final MathOperation type;

  /**
   * @param type The type of math operation
   */
  public DateArithmeticOperator(@Nonnull final MathOperation type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    final Collection left = input.getLeft();
    final Collection right = input.getRight();
    checkUserInput(left instanceof Temporal,
        type + " operator does not support left operand: " + left.getExpression());

    checkUserInput(right instanceof QuantityCollection,
        type + " operator does not support right operand: " + right.getExpression());
    final QuantityCollection calendarDuration = (QuantityCollection) right;
    final Temporal temporal = (Temporal) left;

    return temporal.getDateArithmeticOperation(type)
        .apply(calendarDuration);
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}
