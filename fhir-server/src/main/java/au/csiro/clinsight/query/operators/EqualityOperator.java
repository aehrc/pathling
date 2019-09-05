/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Provides the functionality of the family of equality operators within FHIRPath, i.e. = and !=.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#equality">http://hl7.org/fhirpath/2018Sep/index.html#equality</a>
 */
public class EqualityOperator implements BinaryOperator {

  private String operator;

  public EqualityOperator(String operator) {
    this.operator = operator;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull BinaryOperatorInput input) {
    validateInput(input);
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();

    String fhirPath =
        left.getFhirPath() + " " + operator + " " + right.getFhirPath();

    // Create a new dataset which joins left and right, using the given boolean operator within
    // the condition.
    Dataset<Row> leftDataset = left.getDataset();
    Dataset<Row> rightDataset = right.getDataset();
    Column leftIdColumn = leftDataset.col(left.getDatasetColumn() + "_id");
    Column leftColumn = leftDataset.col(left.getDatasetColumn());
    Column rightIdColumn = rightDataset.col(right.getDatasetColumn() + "_id");
    Column rightColumn = rightDataset.col(right.getDatasetColumn());
    String newHash = Strings.md5Short(fhirPath);
    Dataset<Row> newDataset = leftDataset
        .join(rightDataset, leftIdColumn.equalTo(rightIdColumn), "left_outer");

    // Based on the type of operator, create the correct column expression.
    Column expression;
    switch (operator) {
      case "=":
        leftColumn.equalTo(rightColumn);
        break;
      case "!=":
        leftColumn.notEqual(rightColumn);
        break;
      default:
        assert false : "Unsupported equality operator encountered: " + operator;
    }

    newDataset = newDataset
        .select(leftIdColumn.alias(newHash + "_id"), leftColumn.and(rightColumn));

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(fhirPath);
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FhirType.BOOLEAN);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(newDataset);
    result.setDatasetColumn(newHash);
    return result;
  }

  private void validateInput(BinaryOperatorInput input) {
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();
    if (!left.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator must be singular: " + left.getFhirPath());
    }
    if (!right.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator must be singular: " + right.getFhirPath());
    }
    if (left.getFhirPathType() != right.getFhirPathType()) {
      throw new InvalidRequestException(
          "Left and right operands within equality expression must be of same type: " + input
              .getExpression());
    }
  }
}
