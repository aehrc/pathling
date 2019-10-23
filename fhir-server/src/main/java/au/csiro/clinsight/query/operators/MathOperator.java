/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.DECIMAL;
import static au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType.INTEGER;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
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
 * @see <a href="http://hl7.org/fhirpath/2018Sep/index.html#math">http://hl7.org/fhirpath/2018Sep/index.html#math</a>
 */
public class MathOperator implements BinaryOperator {

  private static final Set<FhirPathType> supportedTypes = new HashSet<FhirPathType>() {{
    add(INTEGER);
    add(DECIMAL);
  }};

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
        leftColumn = left.getValueColumn(),
        rightIdColumn = right.getIdColumn(),
        rightColumn = right.getValueColumn();
    Dataset<Row> dataset = leftDataset
        .join(rightDataset, leftIdColumn.equalTo(rightIdColumn), "left_outer");

    // Based on the type of operator, create the correct column expression.
    Column expression = null;
    FhirPathType fhirPathType = null;
    FHIRDefinedType fhirType = null;
    switch (operator) {
      case "+":
        expression = leftColumn.plus(rightColumn);
        fhirPathType = left.getFhirPathType();
        fhirType = left.getFhirType();
        break;
      case "-":
        expression = leftColumn.minus(rightColumn);
        fhirPathType = left.getFhirPathType();
        fhirType = left.getFhirType();
        break;
      case "*":
        expression = leftColumn.multiply(rightColumn);
        fhirPathType = left.getFhirPathType();
        fhirType = left.getFhirType();
        // TODO: Implement smart multiplication for Quantity.
        break;
      case "/":
        expression = leftColumn.divide(rightColumn);
        fhirPathType = DECIMAL;
        fhirType = FHIRDefinedType.DECIMAL;
        break;
      case "mod":
        expression = leftColumn.mod(rightColumn);
        fhirPathType = INTEGER;
        fhirType = FHIRDefinedType.INTEGER;
        break;
      default:
        assert false : "Unsupported math operator encountered: " + operator;
    }

    Column valueColumn = expression;
    dataset = dataset.withColumn("mathResult", valueColumn);

    // Construct a new parse result.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(fhirPath);
    result.setFhirPathType(fhirPathType);
    result.setFhirType(fhirType);
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
