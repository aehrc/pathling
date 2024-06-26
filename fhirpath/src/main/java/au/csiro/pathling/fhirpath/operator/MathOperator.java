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

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.operator.Operator.buildExpression;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Provides the functionality of the family of math operators within FHIRPath, i.e. +, -, *, / and
 * mod.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#math">Math</a>
 */
public class MathOperator implements Operator {

  @Nonnull
  private final MathOperation type;

  /**
   * @param type The type of math operation
   */
  public MathOperator(@Nonnull final MathOperation type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();

    // Check whether this needs to be delegated off to the DateArithmeticOperator.
    if (left instanceof Temporal && right instanceof QuantityLiteralPath) {
      return new DateArithmeticOperator(type).invoke(input);
    }

    checkUserInput(left instanceof Numeric,
        type + " operator does not support left operand: " + left.getExpression());
    checkUserInput(right instanceof Numeric,
        type + " operator does not support right operand: " + right.getExpression());
    checkUserInput(left.isSingular(),
        "Left operand to " + type + " operator must be singular: " + left.getExpression());
    checkUserInput(right.isSingular(),
        "Right operand to " + type + " operator must be singular: " + right.getExpression());
    checkUserInput(left instanceof Comparable && right instanceof Comparable,
        "Left and right operands are not comparable: " + left.getExpression() + " "
            + type + " " + right.getExpression());
    final Comparable comparableLeft = (Comparable) left;
    final Comparable comparableRight = (Comparable) right;
    checkUserInput(comparableLeft.isComparableTo(comparableRight.getClass()),
        "Left and right operands are not comparable: " + left.getExpression() + " "
            + type + " " + right.getExpression());

    final String expression = buildExpression(input, type.toString());
    final Dataset<Row> dataset = join(input.getContext(), left, right, JoinType.LEFT_OUTER);

    final Numeric leftNumeric = (Numeric) left;
    final Numeric rightNumeric = (Numeric) right;
    return leftNumeric.getMathOperation(type, expression, dataset).apply(rightNumeric);
  }

}
