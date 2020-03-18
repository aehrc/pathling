/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.codeableConceptStructType;
import static au.csiro.pathling.TestUtilities.getSparkSession;
import static au.csiro.pathling.TestUtilities.rowFromCodeableConcept;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.ComplexExpressionBuilder;
import au.csiro.pathling.test.DatasetBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class EmptyFunctionTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = getSparkSession();
  }

  @Test
  public void returnsCorrectResults() {
    Coding coding1 = new Coding(TestUtilities.SNOMED_URL, "840546002", "Exposure to COVID-19");
    CodeableConcept concept1 = new CodeableConcept(coding1);
    Coding coding2 = new Coding(TestUtilities.SNOMED_URL, "248427009", "Fever symptoms");
    CodeableConcept concept2 = new CodeableConcept(coding2);

    ParsedExpression input = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", codeableConceptStructType())
        .withRow("Observation/abc1", null)
        .withRow("Observation/abc2", null)
        .withRow("Observation/abc2", null)
        .withRow("Observation/abc3", rowFromCodeableConcept(concept1))
        .withRow("Observation/abc4", rowFromCodeableConcept(concept1))
        .withRow("Observation/abc4", null)
        .withRow("Observation/abc5", rowFromCodeableConcept(concept1))
        .withRow("Observation/abc5", rowFromCodeableConcept(concept2))
        .build();

    // Set up the function input.
    FunctionInput emptyInput = new FunctionInput();
    emptyInput.setInput(input);
    emptyInput.setExpression("empty()");

    // Invoke the function.
    EmptyFunction emptyFunction = new EmptyFunction();
    ParsedExpression result = emptyFunction.invoke(emptyInput);

    // Check the result.
    assertThat(result).hasFhirPath("empty()");
    assertThat(result).isSingular();
    assertThat(result).isOfBooleanType();
    assertThat(result).isSelection();

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withColumn("789wxyz", DataTypes.BooleanType)
        .withRow("Observation/abc1", true)
        .withRow("Observation/abc2", true)
        .withRow("Observation/abc3", false)
        .withRow("Observation/abc4", false)
        .withRow("Observation/abc5", false)
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
    assertThat(result)
        .aggByIdResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void inputMustNotContainArguments() {
    FunctionInput emptyInput = new FunctionInput();
    emptyInput.setInput(mock(ParsedExpression.class));
    emptyInput.setExpression("empty('some argument')");
    emptyInput.getArguments().add(mock(ParsedExpression.class));

    // Execute the function and assert that it throws the right exception.
    EmptyFunction emptyFunction = new EmptyFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> emptyFunction.invoke(emptyInput))
        .withMessage("Arguments can not be passed to empty function: empty('some argument')");
  }

  @Test
  public void inputMustNotBeSingular() {
    ParsedExpression input = mock(ParsedExpression.class);
    when(input.getFhirPath()).thenReturn("some.path");
    when(input.isSingular()).thenReturn(true);

    FunctionInput emptyInput = new FunctionInput();
    emptyInput.setInput(input);

    // Execute the function and assert that it throws the right exception.
    EmptyFunction emptyFunction = new EmptyFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> emptyFunction.invoke(emptyInput))
        .withMessage("Input to empty function must not be singular: some.path");
  }

}