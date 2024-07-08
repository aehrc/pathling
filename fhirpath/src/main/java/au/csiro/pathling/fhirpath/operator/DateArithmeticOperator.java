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

import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import javax.annotation.Nonnull;

/**
 * Provides the functionality of the family of math operators within FHIRPath, i.e. +, -, *, / and
 * mod.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#math">Math</a>
 */
@NotImplemented
public class DateArithmeticOperator implements BinaryOperator {

  @Nonnull
  private final MathOperation type;

  /**
   * @param type The type of math operation
   */
  public DateArithmeticOperator(@Nonnull final MathOperation type) {
    this.type = type;
  }

  // TODO: implement with columns

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    return null;
  }
  //   final Collection left = input.getLeft();
  //   final Collection right = input.getRight();
  //   checkUserInput(left instanceof Temporal,
  //       type + " operator does not support left operand: " + left.getExpression());
  //
  //   checkUserInput(right instanceof QuantityLiteralPath,
  //       type + " operator does not support right operand: " + right.getExpression());
  //   final QuantityLiteralPath calendarDuration = (QuantityLiteralPath) right;
  //   checkUserInput(CalendarDurationUtils.isCalendarDuration(calendarDuration.getValue()),
  //       "Right operand of " + type + " operator must be a calendar duration");
  //   checkUserInput(left.isSingular(),
  //       "Left operand to " + type + " operator must be singular: " + left.getExpression());
  //   checkUserInput(right.isSingular(),
  //       "Right operand to " + type + " operator must be singular: " + right.getExpression());
  //
  //   final Temporal temporal = (Temporal) left;
  //   final String expression = buildExpression(input, type.toString());
  //
  //   return temporal.getDateArithmeticOperation(type, right.getDataset(), expression)
  //       .apply(calendarDuration);
  // }

}
