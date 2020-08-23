/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
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
public class ComparisonOperatorValidationTest {

  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  @Test
  public void operandsAreNotComparable() {
    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(true)
        .expression("foo")
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .singular(true)
        .expression("bar")
        .build();

    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator comparisonOperator = Operator.getInstance(">");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> comparisonOperator.invoke(input));
    assertEquals("Left operand to > operator is not comparable to right operand: foo > bar",
        error.getMessage());

    // Now test the right operand.
    final OperatorInput reversedInput = new OperatorInput(parserContext, right, left);
    final InvalidUserInputError reversedError = assertThrows(
        InvalidUserInputError.class,
        () -> comparisonOperator.invoke(reversedInput));
    assertEquals(
        "Left operand to > operator is not comparable to right operand: bar > foo",
        reversedError.getMessage());
  }

}
