/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalBoolean;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class BooleanOperatorValidationTest {

  @Test
  public void operandIsNotSingular() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
        FhirPathType.BOOLEAN).build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN)
            .build();
    left.setSingular(false);
    right.setSingular(true);
    left.setFhirPath("estimatedAge");
    right.setFhirPath("deceasedBoolean");

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage("Left operand to and operator must be singular Boolean: estimatedAge");

    // Now test the right operand.
    input.getLeft().setSingular(true);
    input.getRight().setSingular(false);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage("Right operand to and operator must be singular Boolean: deceasedBoolean");
  }

  @Test
  public void operandIsNotBoolean() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING).build(),
        right = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN)
            .build();
    left.setSingular(true);
    right.setSingular(true);
    left.setFhirPath("estimatedAge");
    right.setFhirPath("deceasedBoolean");

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage("Left operand to and operator must be singular Boolean: estimatedAge");

    // Now test the right operand.
    input.getLeft().setFhirType(FHIRDefinedType.BOOLEAN);
    input.getLeft().setFhirPathType(FhirPathType.BOOLEAN);
    input.getRight().setFhirType(FHIRDefinedType.INTEGER);
    input.getRight().setFhirPathType(FhirPathType.INTEGER);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage("Right operand to and operator must be singular Boolean: deceasedBoolean");
  }

  @Test
  public void bothOperandsAreLiteral() {
    ParsedExpression literalLeft = literalBoolean(true);
    ParsedExpression literalRight = literalBoolean(true);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literalLeft);
    input.setRight(literalRight);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage(
            "Cannot have two literal operands to and operator: estimatedAge and deceasedBoolean");
  }

}
