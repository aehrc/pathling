/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.getSparkSession;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import au.csiro.pathling.test.ResourceExpressionBuilder;
import au.csiro.pathling.test.fixtures.PatientResourceRowFixture;
import au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
public class FirstFunctionTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = getSparkSession();
  }

  @Test
  public void testGetsFirstResourceCorrectly() {
    Dataset<Row> dataset = PatientResourceRowFixture.createCompleteDataset(spark);
    // Build up an input for the function.
    ParsedExpression input =
        new ResourceExpressionBuilder(ResourceType.PATIENT, FHIRDefinedType.PATIENT)
            .withDataset(dataset).build();

    FunctionInput firstInput = new FunctionInput();
    firstInput.setInput(input);
    firstInput.setExpression("first()");

    // Execute the first function.
    FirstFunction firstFunction = new FirstFunction();
    ParsedExpression result = firstFunction.invoke(firstInput);
    assertThat(result).isResultFor(firstInput).hasSameTypeAs(input)
        .isResourceOfType(ResourceType.PATIENT, FHIRDefinedType.PATIENT).isSelection()
        .isAggregation();
    // Check that the correct rows were included in the result.
    assertThat(result).selectResult().hasRows(PatientResourceRowFixture.PATIENT_ALL_ROWS);
  }

  @Test
  public void testSelectsFirstValuePerResourceFromListOfValues() {
    Dataset<Row> dataset = StringPrimitiveRowFixture.createCompleteDataset(spark);

    // Build up an input for the function.
    ParsedExpression input =
        new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, FhirPathType.STRING)
            .withDataset(dataset).build();
    FunctionInput firstInput = new FunctionInput();
    firstInput.setInput(input);
    firstInput.setExpression("name.family.first()");

    // Execute the fist function.
    ParsedExpression result = new FirstFunction().invoke(firstInput);

    // Check that the correct rows were included in the result.
    assertThat(result).isResultFor(firstInput).hasSameTypeAs(input).isPrimitive().isSingular()
        .isSelection().isAggregation();

    List<Row> expectedRows =
        Arrays.asList(STRING_1_JUDE, STRING_2_SAMUEL, STRING_3_NULL, STRING_4_ADAM, STRING_5_NULL);
    assertThat(result).selectResult().hasRows(expectedRows);
    assertThat(result).aggByIdResult().hasRows(expectedRows);
    assertThat(result).aggResult().isValue().isEqualTo((STRING_1_JUDE.getString(1)));
  }

  @Test
  public void inputMustNotContainArguments() {
    // Build up an input for the function.
    ExpressionParserContext expressionParserContext = new ExpressionParserContext();
    expressionParserContext.getGroupings().add(mock(ParsedExpression.class));

    FunctionInput functionInput = new FunctionInput();
    functionInput.setInput(mock(ParsedExpression.class));
    functionInput.setExpression("first('some argument')");
    functionInput.setContext(expressionParserContext);
    functionInput.getArguments().add(mock(ParsedExpression.class));

    // Execute the function and assert that it throws the right exception.
    FirstFunction countFunction = new FirstFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> countFunction.invoke(functionInput))
        .withMessage("Arguments can not be passed to first function: first('some argument')");
  }
}
