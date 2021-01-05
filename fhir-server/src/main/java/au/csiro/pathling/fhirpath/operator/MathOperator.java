/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.operator.Operator.buildExpression;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import javax.annotation.Nonnull;
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
    checkUserInput(left instanceof Numeric,
        type + " operator does not support left operand: " + left.getExpression());
    checkUserInput(right instanceof Numeric,
        type + " operator does not support right operand: " + right.getExpression());
    checkUserInput(left.isSingular(),
        "Left operand to " + type + " operator must be singular: " + left.getExpression());
    checkUserInput(right.isSingular(),
        "Right operand to " + type + " operator must be singular: " + right.getExpression());

    final String expression = buildExpression(input, type.toString());
    final Dataset<Row> dataset = join(input.getContext(), left, right, JoinType.LEFT_OUTER);

    final Numeric leftNumeric = (Numeric) left;
    final Numeric rightNumeric = (Numeric) right;
    return leftNumeric.getMathOperation(type, expression, dataset).apply(rightNumeric);
  }

}
