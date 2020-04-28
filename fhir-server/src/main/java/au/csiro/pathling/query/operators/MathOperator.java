/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.DECIMAL;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.INTEGER;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Provides the functionality of the family of math operators within FHIRPath, i.e. +, -, *, / and
 * mod.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#math">Math</a>
 */
public class MathOperator implements BinaryOperator {

  private static final Set<FhirPathType> supportedTypes = new HashSet<FhirPathType>() {{
    add(INTEGER);
    add(DECIMAL);
  }};
  public static final String ADDITION = "+";
  public static final String SUBTRACTION = "-";
  public static final String MULTIPLICATION = "*";
  public static final String DIVISION = "/";
  public static final String MODULUS = "mod";

  private final String operator;

  public MathOperator(String operator) {
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
        leftColumn = left.isLiteral()
                     ? lit(left.getJavaLiteralValue())
                     : left.getValueColumn(),
        rightIdColumn = right.getIdColumn(),
        rightColumn = right.isLiteral()
                      ? lit(right.getJavaLiteralValue())
                      : right.getValueColumn();

    // Based on the type of operator, create the correct column expression.
    Column expression = null;
    FhirPathType fhirPathType = null;
    FHIRDefinedType fhirType = null;
    switch (operator) {
      case ADDITION:
        expression = leftColumn.plus(rightColumn);
        // The result of the expression takes on the type of the left operand.
        if (left.getFhirPathType() == INTEGER && right.getFhirPathType() == DECIMAL) {
          expression = expression.cast("int");
        }
        fhirPathType = left.getFhirPathType();
        fhirType = left.getFhirType();
        break;
      case SUBTRACTION:
        expression = leftColumn.minus(rightColumn);
        // The result of the expression takes on the type of the left operand.
        if (left.getFhirPathType() == INTEGER && right.getFhirPathType() == DECIMAL) {
          expression = expression.cast("int");
        }
        fhirPathType = left.getFhirPathType();
        fhirType = left.getFhirType();
        break;
      case MULTIPLICATION:
        expression = leftColumn.multiply(rightColumn);
        // The result of the expression takes on the type of the left operand.
        if (left.getFhirPathType() == INTEGER && right.getFhirPathType() == DECIMAL) {
          expression = expression.cast("int");
        }
        fhirPathType = left.getFhirPathType();
        fhirType = left.getFhirType();
        break;
      case DIVISION:
        // The result of the expression should always be decimal.
        Column denominator = rightColumn;
        if (left.getFhirPathType() == INTEGER && right.getFhirPathType() == INTEGER) {
          denominator = denominator.cast("decimal");
        }
        expression = leftColumn.divide(denominator);
        fhirPathType = DECIMAL;
        fhirType = FHIRDefinedType.DECIMAL;
        break;
      case MODULUS:
        expression = leftColumn.mod(rightColumn);
        // The result of the expression should always be integer.
        if (left.getFhirPathType() == DECIMAL || right.getFhirPathType() == DECIMAL) {
          expression = expression.cast("int");
        }
        fhirPathType = INTEGER;
        fhirType = FHIRDefinedType.INTEGER;
        break;
      default:
        assert false : "Unsupported math operator encountered: " + operator;
    }
    // Ensure correct empty collection propagation.
    expression = when(leftColumn.isNull().or(rightColumn.isNull()), lit(null))
        .otherwise(expression);

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
    dataset = dataset.withColumn("mathResult", expression);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(fhirPath);
    result.setFhirPathType(fhirPathType);
    result.setFhirType(fhirType);
    result.setPrimitive(true);
    result.setSingular(true);
    result.setDataset(dataset);
    result.setHashedValue(idColumn, expression);
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
  }
}
