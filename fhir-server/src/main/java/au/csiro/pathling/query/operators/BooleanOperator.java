/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

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
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#boolean-logic">http://hl7.org/fhirpath/2018Sep/index.html#boolean-logic</a>
 */
public class BooleanOperator implements BinaryOperator {

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
        leftColumn = left.getValueColumn(),
        rightIdColumn = right.getIdColumn(),
        rightColumn = right.getValueColumn();
    Dataset<Row> dataset = leftDataset
        .join(rightDataset, leftIdColumn.equalTo(rightIdColumn), "left_outer");

    // Based on the type of operator, create the correct column expression.
    Column expression = null;
    switch (operator) {
      case "and":
        expression = leftColumn.and(rightColumn);
        break;
      case "or":
        expression = leftColumn.or(rightColumn);
        break;
      case "xor":
        expression = leftColumn.when(
            leftColumn.isNull().or(rightColumn.isNull()), null
        ).when(
            leftColumn.equalTo(true).and(rightColumn.equalTo(false)).or(
                leftColumn.equalTo(false).and(rightColumn.equalTo(true))
            ), true
        ).otherwise(false);
        break;
      case "implies":
        expression = leftColumn.when(
            leftColumn.equalTo(true), rightColumn
        ).when(
            leftColumn.equalTo(false), true
        ).otherwise(
            leftColumn.when(rightColumn.equalTo(true), true)
                .otherwise(null)
        );
        break;
      default:
        assert false : "Unsupported boolean operator encountered: " + operator;
    }

    Column valueColumn = expression;
    dataset = dataset.withColumn("booleanResult", valueColumn);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(input.getExpression());
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setIdColumn(leftIdColumn);
    result.setValueColumn(valueColumn);
    return result;
  }

  private void validateInput(BinaryOperatorInput input) {
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();
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
