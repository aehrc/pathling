/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
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
 * @see <a href="https://pathling.app/docs/fhirpath/operators.html#boolean-logic">Boolean logic</a>
 */
public class BooleanOperator implements BinaryOperator {

  public static final String AND = "and";
  public static final String OR = "or";
  public static final String XOR = "xor";
  public static final String IMPLIES = "implies";

  private String operator;

  public BooleanOperator(String operator) {
    this.operator = operator;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull BinaryOperatorInput input) {
    validateInput(input);
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();

    // Create a new dataset which joins left and right, using the given boolean operator within
    // the condition.
    Dataset<Row> leftDataset = left.getDataset(),
        rightDataset = right.getDataset();
    Column leftIdColumn = left.getIdColumn(),
        leftColumn = left.isLiteral()
                     ? lit(left.getJavaLiteralValue())
                     : left.getValueColumn(),
        rightIdColumn = right.getIdColumn(),
        rightColumn = right.isLiteral()
                      ? lit(right.getJavaLiteralValue())
                      : right.getValueColumn();

    // Based on the type of operator, create the correct column expression.
    Column expression = null;
    switch (operator) {
      case AND:
        expression = leftColumn.and(rightColumn);
        break;
      case OR:
        expression = leftColumn.or(rightColumn);
        break;
      case XOR:
        expression = when(
            leftColumn.isNull().or(rightColumn.isNull()), null
        ).when(
            leftColumn.equalTo(true).and(rightColumn.equalTo(false)).or(
                leftColumn.equalTo(false).and(rightColumn.equalTo(true))
            ), true
        ).otherwise(false);
        break;
      case IMPLIES:
        expression = when(
            leftColumn.equalTo(true), rightColumn
        ).when(
            leftColumn.equalTo(false), true
        ).otherwise(
            when(rightColumn.equalTo(true), true)
                .otherwise(null)
        );
        break;
      default:
        assert false : "Unsupported boolean operator encountered: " + operator;
    }

    Dataset<Row> dataset;
    Column idColumn;
    if (left.isLiteral()) {
      dataset = rightDataset;
      idColumn = rightIdColumn;
    } else if (right.isLiteral()) {
      dataset = leftDataset;
      idColumn = leftIdColumn;
    } else {
      dataset = leftDataset
          .join(rightDataset, leftIdColumn.equalTo(rightIdColumn), "left_outer");
      idColumn = leftIdColumn;
    }
    dataset = dataset.withColumn("booleanResult", expression);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setIdColumn(idColumn);
    result.setValueColumn(expression);
    return result;
  }

  private void validateInput(BinaryOperatorInput input) {
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();
    if (left.isLiteral() && right.isLiteral()) {
      throw new InvalidRequestException(
          "Cannot have two literal operands to " + operator + " operator: " + input
              .getExpression());
    }
    if (left.getFhirPathType() != FhirPathType.BOOLEAN || !left.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator must be singular Boolean: " + left
              .getFhirPath());
    }
    if (right.getFhirPathType() != FhirPathType.BOOLEAN || !right.isSingular()) {
      throw new InvalidRequestException(
          "Right operand to " + operator + " operator must be singular Boolean: " + right
              .getFhirPath());
    }
  }
}
