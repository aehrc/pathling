/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.*;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.FhirPathTypeSqlHelper;
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
 * Provides the functionality of the family of equality operators within FHIRPath, i.e. = and !=.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#equality">Equality</a>
 */
public class EqualityOperator implements BinaryOperator {

  public static final String EQUALS = "=";
  public static final String NOT_EQUALS = "!=";

  private static final Set<FhirPathType> SUPPORTED_TYPES = EnumSet.of(
      STRING,
      INTEGER,
      DECIMAL,
      BOOLEAN,
      DATE_TIME,
      DATE,
      CODING
  );

  private String operator;

  public EqualityOperator(String operator) {
    assert operator.equals(EQUALS) || operator.equals(NOT_EQUALS) :
        "Unsupported equality operator encountered: " + operator;
    this.operator = operator;
  }

  @Nonnull
  @Override
  public ParsedExpression invoke(@Nonnull BinaryOperatorInput input) {
    validateInput(input);
    ParsedExpression left = input.getLeft();
    ParsedExpression right = input.getRight();

    FhirPathTypeSqlHelper sqlHelper = FhirPathTypeSqlHelper.forType(left.getFhirPathType());
    String fhirPath =
        left.getFhirPath() + " " + operator + " " + right.getFhirPath();

    Dataset<Row> leftDataset = left.getDataset(),
        rightDataset = right.getDataset();
    Column leftIdColumn = left.getIdColumn(),
        leftColumn = left.isLiteral()
                     ? sqlHelper.getLiteralColumn(left)
                     : left.getValueColumn(),
        rightIdColumn = right.getIdColumn(),
        rightColumn =
            right.isLiteral()
            ? sqlHelper.getLiteralColumn(right)
            : right.getValueColumn();

    if (left.getFhirPathType() == DATE_TIME || left.getFhirPathType() == DATE) {
      leftColumn = to_date(leftColumn);
      rightColumn = to_date(rightColumn);
    }

    // Based on the type of operator, create the correct column expression. These expressions are
    // written to take account of the fact that an equality expression involving null will always
    // be null in Spark, whereas in FHIRPath { } = { } should be true and 'foo' = { } should be
    // false.
    Column nullTest = leftColumn.isNull().or(rightColumn.isNull()),
        equalityTest = sqlHelper.getEquality().apply(leftColumn, rightColumn);
    // Invert the test if the not equals operator has been invoked.
    if (operator.equals(NOT_EQUALS)) {
      equalityTest = not(equalityTest);
    }
    Column expression = when(nullTest, null).otherwise(equalityTest);

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
    dataset = dataset.withColumn("equalityResult", expression);

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
    if (left.isLiteral() && right.isLiteral()) {
      throw new InvalidRequestException(
          "Cannot have two literal operands to " + operator + " operator: " + input
              .getExpression());
    }
    if (!SUPPORTED_TYPES.contains(left.getFhirPathType()) || !left.isSingular()) {
      throw new InvalidRequestException(
          "Left operand to " + operator + " operator is of unsupported type, or is not singular: "
              + left.getFhirPath());
    }
    if (!SUPPORTED_TYPES.contains(right.getFhirPathType()) || !right.isSingular()) {
      throw new InvalidRequestException(
          "Right operand to " + operator + " operator is of unsupported type, or is not singular: "
              + right.getFhirPath());
    }
    if (left.getFhirPathType() != right.getFhirPathType()) {
      throw new InvalidRequestException(
          "Left and right operands within equality expression must be of same type: " + input
              .getExpression());
    }
  }

  public static Set<FhirPathType> getSupportedTypes() {
    return SUPPORTED_TYPES;
  }

}
