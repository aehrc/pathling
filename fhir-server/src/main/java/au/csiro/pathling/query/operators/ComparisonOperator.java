/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.*;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Provides the functionality of the family of comparison operators within FHIRPath, i.e. <=, <, >,
 * >=.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#comparison">http://hl7.org/fhirpath/2018Sep/index.html#comparison</a>
 */
public class ComparisonOperator implements BinaryOperator {

  public static final String LESS_THAN_OR_EQUAL_TO = "<=";
  public static final String LESS_THAN = "<";
  public static final String GREATER_THAN_OR_EQUAL_TO = ">=";
  public static final String GREATER_THAN = ">";

  private static final Set<FhirPathType> supportedTypes = EnumSet.of(
      STRING,
      INTEGER,
      DECIMAL,
      DATE_TIME,
      DATE,
      TIME
  );

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
    Dataset<Row> leftDataset = left.getDataset(),
        rightDataset = right.getDataset();
    Column leftIdColumn = left.getIdColumn(),
        leftColumn = left.isLiteral() ? lit(left.getJavaLiteralValue()) : left.getValueColumn(),
        rightIdColumn = right.getIdColumn(),
        rightColumn = right.isLiteral() ? lit(right.getJavaLiteralValue()) : right.getValueColumn();

    if (left.getFhirPathType() == DATE_TIME || left.getFhirPathType() == DATE) {
      leftColumn = to_date(leftColumn);
      rightColumn = to_date(rightColumn);
    }

    // Based on the type of operator, create the correct column expression.
    Column expression = null;
    switch (operator) {
      case LESS_THAN_OR_EQUAL_TO:
        expression = leftColumn.leq(rightColumn);
        break;
      case LESS_THAN:
        expression = leftColumn.lt(rightColumn);
        break;
      case GREATER_THAN_OR_EQUAL_TO:
        expression = leftColumn.geq(rightColumn);
        break;
      case GREATER_THAN:
        expression = leftColumn.gt(rightColumn);
        break;
      default:
        assert false : "Unsupported comparison operator encountered: " + operator;
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
    dataset = dataset.withColumn("comparisonResult", expression);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(fhirPath);
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
