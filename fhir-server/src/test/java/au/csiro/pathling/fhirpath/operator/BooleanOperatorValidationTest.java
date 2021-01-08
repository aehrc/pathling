/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
public class BooleanOperatorValidationTest {

  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  @Test
  public void operandIsNotSingular() {
    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(false)
        .expression("estimatedAge")
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(true)
        .expression("deceasedBoolean")
        .build();

    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("and");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> booleanOperator.invoke(input));
    assertEquals(
        "Left operand to and operator must be singular: estimatedAge",
        error.getMessage());

    // Now test the right operand.
    final OperatorInput reversedInput = new OperatorInput(parserContext, right, left);
    final InvalidUserInputError reversedError = assertThrows(
        InvalidUserInputError.class,
        () -> booleanOperator.invoke(reversedInput));
    assertEquals(
        "Right operand to and operator must be singular: estimatedAge",
        reversedError.getMessage());
  }

  @Test
  public void operandIsNotBoolean() {
    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .singular(true)
        .expression("estimatedAge")
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(true)
        .expression("deceasedBoolean")
        .build();

    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("and");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> booleanOperator.invoke(input));
    assertEquals(
        "Left operand to and operator must be Boolean: estimatedAge",
        error.getMessage());

    // Now test the right operand.
    final OperatorInput reversedInput = new OperatorInput(parserContext, right, left);
    final InvalidUserInputError reversedError = assertThrows(
        InvalidUserInputError.class,
        () -> booleanOperator.invoke(reversedInput));
    assertEquals(
        "Right operand to and operator must be Boolean: estimatedAge",
        reversedError.getMessage());
  }

}
