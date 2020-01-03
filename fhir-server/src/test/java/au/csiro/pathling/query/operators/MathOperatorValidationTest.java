/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.DATE_TIME;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.DECIMAL;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.INTEGER;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.STRING;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalInteger;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class MathOperatorValidationTest {

  @Test
  public void operandIsNotCorrectType() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.DATETIME, DATE_TIME)
        .build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER).build();
    left.setSingular(true);
    right.setSingular(true);
    left.setFhirPath("foo");
    right.setFhirPath("bar");

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Left operand to + operator is of unsupported type, or is not singular: " + left
                .getFhirPath());

    // Now test the right operand.
    input.getLeft().setFhirType(FHIRDefinedType.DECIMAL);
    input.getLeft().setFhirPathType(DECIMAL);
    input.getRight().setFhirType(FHIRDefinedType.STRING);
    input.getRight().setFhirPathType(STRING);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Right operand to + operator is of unsupported type, or is not singular: " + right
                .getFhirPath());
  }

  @Test
  public void operandIsNotSingular() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER).build();
    left.setSingular(false);
    right.setSingular(true);
    left.setFhirPath("foo");
    right.setFhirPath("bar");

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Left operand to + operator is of unsupported type, or is not singular: " + left
                .getFhirPath());

    // Now test the right operand.
    input.getLeft().setSingular(true);
    input.getRight().setSingular(false);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Right operand to + operator is of unsupported type, or is not singular: " + right
                .getFhirPath());
  }

  @Test
  public void bothOperandsAreLiteral() {
    ParsedExpression literalLeft = literalInteger(1);
    ParsedExpression literalRight = literalInteger(1);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literalLeft);
    input.setRight(literalRight);
    input.setExpression("1 + 1");

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Cannot have two literal operands to + operator: 1 + 1");
  }

}
