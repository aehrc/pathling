/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.INTEGER;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.STRING;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.TIME;
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
public class EqualityOperatorValidationTest {

  @Test
  public void operandIsNotCorrectType() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.TIME, TIME)
        .build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING).build();
    left.setSingular(true);
    right.setSingular(true);
    left.setFhirPath("foo");
    right.setFhirPath("bar");

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    EqualityOperator equalityOperator = new EqualityOperator(EqualityOperator.EQUALS);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> equalityOperator.invoke(input))
        .withMessage(
            "Left operand to = operator is of unsupported type, or is not singular: " + left
                .getFhirPath());

    // Now test the right operand.
    input.setLeft(right);
    input.setRight(left);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> equalityOperator.invoke(input))
        .withMessage(
            "Right operand to = operator is of unsupported type, or is not singular: " + left
                .getFhirPath());
  }

  @Test
  public void operandIsNotSingular() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING).build();
    right.setSingular(true);
    left.setFhirPath("foo");
    right.setFhirPath("bar");

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    EqualityOperator equalityOperator = new EqualityOperator(EqualityOperator.EQUALS);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> equalityOperator.invoke(input))
        .withMessage(
            "Left operand to = operator is of unsupported type, or is not singular: " + left
                .getFhirPath());

    // Now test the right operand.
    input.getLeft().setSingular(true);
    input.getRight().setSingular(false);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> equalityOperator.invoke(input))
        .withMessage(
            "Right operand to = operator is of unsupported type, or is not singular: " + right
                .getFhirPath());
  }

  @Test
  public void bothOperandsAreLiteral() {
    ParsedExpression literalLeft = literalInteger(1);
    ParsedExpression literalRight = literalInteger(1);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literalLeft);
    input.setRight(literalRight);
    input.setExpression("1 = 1");

    EqualityOperator equalityOperator = new EqualityOperator(EqualityOperator.EQUALS);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> equalityOperator.invoke(input))
        .withMessage(
            "Cannot have two literal operands to = operator: 1 = 1");
  }

  @Test
  public void operandsAreNotSameType() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING).build();
    left.setSingular(true);
    right.setSingular(true);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("multipleBirthInteger = gender");

    EqualityOperator equalityOperator = new EqualityOperator(EqualityOperator.EQUALS);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> equalityOperator.invoke(input))
        .withMessage(
            "Left and right operands within equality expression must be of same type: multipleBirthInteger = gender");
  }

}
