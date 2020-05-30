/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Provides the functionality of the family of boolean operators within FHIRPath, i.e. and, or, xor
 * and implies.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#boolean-logic">Boolean
 * logic</a>
 */
public class BooleanOperator implements Operator {

  private final BooleanOperatorType type;

  /**
   * @param type The type of operator
   */
  public BooleanOperator(final BooleanOperatorType type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public BooleanPath invoke(@Nonnull final OperatorInput input) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();

    checkUserInput(left instanceof BooleanPath || left instanceof BooleanLiteralPath,
        "Left operand to " + type + " operator must be Boolean: " + left.getExpression());
    checkUserInput(left.isSingular(), "Left operand to " + type + " must be singular");
    checkUserInput(right instanceof BooleanPath || right instanceof BooleanLiteralPath,
        "Right operand to " + type + " operator must be Boolean: " + left.getExpression());
    checkUserInput(left.isSingular(), "Right operand to " + type + " must be singular");

    final Column leftValue = left.getValueColumn();
    final Column rightValue = right.getValueColumn();

    // Based on the type of operator, create the correct column expression.
    Column valueColumn;
    switch (type) {
      case AND:
        valueColumn = leftValue.and(rightValue);
        break;
      case OR:
        valueColumn = leftValue.or(rightValue);
        break;
      case XOR:
        valueColumn = when(
            leftValue.isNull().or(rightValue.isNull()), null
        ).when(
            leftValue.equalTo(true).and(rightValue.equalTo(false)).or(
                leftValue.equalTo(false).and(rightValue.equalTo(true))
            ), true
        ).otherwise(false);
        break;
      case IMPLIES:
        valueColumn = when(
            leftValue.equalTo(true), rightValue
        ).when(
            leftValue.equalTo(false), true
        ).otherwise(
            when(rightValue.equalTo(true), true)
                .otherwise(null)
        );
        break;
      default:
        throw new AssertionError("Unsupported boolean operator encountered: " + type);
    }

    final String expression =
        left.getExpression() + " " + type + " " + right.getExpression();
    final Dataset<Row> dataset = QueryHelpers.joinOnId(left, right, JoinType.LEFT_OUTER)
        .withColumn("booleanResult", valueColumn);
    valueColumn = dataset.col("booleanResult");
    return new BooleanPath(expression, dataset, left.getIdColumn(), valueColumn, true,
        FHIRDefinedType.BOOLEAN);
  }

  /**
   * Represents a type of Boolean operator.
   */
  public enum BooleanOperatorType {
    /**
     * AND operation.
     */
    AND("and"),
    /**
     * OR operation.
     */
    OR("or"),
    /**
     * Exclusive OR operation.
     */
    XOR("xor"),
    /**
     * Material implication.
     */
    IMPLIES("implies");

    @Nonnull
    private final String fhirPath;

    BooleanOperatorType(@Nonnull final String fhirPath) {
      this.fhirPath = fhirPath;
    }

    @Override
    public String toString() {
      return fhirPath;
    }
    
  }

}
