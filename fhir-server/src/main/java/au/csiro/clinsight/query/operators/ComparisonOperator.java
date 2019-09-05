/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.*;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e. <=, <, >,
 * >=.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#comparison">http://hl7.org/fhirpath/2018Sep/index.html#comparison</a>
 */
public class ComparisonOperator implements BinaryOperator {

  private static final Set<FhirPathType> supportedTypes = new HashSet<FhirPathType>() {{
    add(STRING);
    add(INTEGER);
    add(DECIMAL);
    add(DATE_TIME);
    add(TIME);
  }};

  private final String operator;

  public ComparisonOperator(String operator) {
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
      case "<=":
        leftColumn.leq(rightColumn);
        break;
      case "<":
        leftColumn.lt(rightColumn);
        break;
      case ">=":
        leftColumn.geq(rightColumn);
        break;
      case ">":
        leftColumn.gt(rightColumn);
        break;
      default:
        assert false : "Unsupported comparison operator encountered: " + operator;
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
    if (!supportedTypes.contains(left.getFhirPathType()) || !left.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator is of unsupported type, or is not singular: "
              + left.getFhirPath());
    }
    if (!supportedTypes.contains(right.getFhirPathType()) || !right.isSingular()) {
      throw new InvalidRequestException(
          "Right operand to " + operator + " operator is of unsupported type, or is not singular: "
              + right.getFhirPath());
    }
    if (left.getFhirPathType() != right.getFhirPathType()) {
      throw new InvalidRequestException(
          "Left and right operands within comparison expression must be of same type: " + input
              .getExpression());
    }
  }
}
