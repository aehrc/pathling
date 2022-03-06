/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
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
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class MathOperatorValidationTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder(spark, fhirContext).build();
  }

  @Test
  void operandIsNotCorrectType() {
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DATETIME)
        .singular(true)
        .expression("foo")
        .build();
    final ElementPath right = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .singular(true)
        .expression("bar")
        .build();

    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator mathOperator = Operator.getInstance("+");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> mathOperator.invoke(input));
    assertEquals("+ operator does not support left operand: foo",
        error.getMessage());

    // Now test the right operand.
    final OperatorInput reversedInput = new OperatorInput(parserContext, right, left);
    final InvalidUserInputError reversedError = assertThrows(
        InvalidUserInputError.class,
        () -> mathOperator.invoke(reversedInput));
    assertEquals(
        "+ operator does not support right operand: foo",
        reversedError.getMessage());
  }

  @Test
  void operandIsNotSingular() {
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .singular(false)
        .expression("foo")
        .build();
    final ElementPath right = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .singular(true)
        .expression("bar")
        .build();

    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator mathOperator = Operator.getInstance("+");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> mathOperator.invoke(input));
    assertEquals("Left operand to + operator must be singular: foo",
        error.getMessage());

    // Now test the right operand.
    final OperatorInput reversedInput = new OperatorInput(parserContext, right, left);
    final InvalidUserInputError reversedError = assertThrows(
        InvalidUserInputError.class,
        () -> mathOperator.invoke(reversedInput));
    assertEquals(
        "Right operand to + operator must be singular: foo",
        reversedError.getMessage());
  }

}
