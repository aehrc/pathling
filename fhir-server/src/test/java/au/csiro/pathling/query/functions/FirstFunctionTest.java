/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.StringPrimitiveRowFixture.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.query.parsing.ExpressionParserContext;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.FunctionTest;
import au.csiro.pathling.test.PatientResourceRowFixture;
import au.csiro.pathling.test.StringPrimitiveRowFixture;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
public class FirstFunctionTest extends FunctionTest {


  @Test
  public void testGetsFirstResouceCorrectly() {
    Dataset<Row> dataset = PatientResourceRowFixture.createCompleteDataset(spark);
    // Build up an input for the function.
    ParsedExpression input = createResourceParsedExpression(dataset, ResourceType.PATIENT);

    FunctionInput firstInput = new FunctionInput();
    firstInput.setInput(input);
    firstInput.setExpression("first()");

    // Execute the first function.
    FirstFunction firstFunction = new FirstFunction();
    ParsedExpression result = firstFunction.invoke(firstInput);
    assertThat(result)
        .isResultFor(firstInput)
        .hasSameTypeAs(input)
        .isResource()
        .isSelection()
        .isAggreation();
    // Check that the correct rows were included in the result
    assertThat(result).selectResult().hasRows(PatientResourceRowFixture.PATIENT_ALL_ROWS);
    //TODO: Not sure if aggregation make any sense here
  }

  @Test
  public void testSelectsFirstValuePerResourceFromListOfValues() {
    Dataset<Row> dataset = StringPrimitiveRowFixture.createCompleteDataset(spark);

    // Build up an input for the functionm
    ParsedExpression input = createPrimitiveParsedExpression(dataset);
    FunctionInput firstInput = new FunctionInput();
    firstInput.setInput(input);
    firstInput.setExpression("name.family.first()");

    // Execute the fist function.
    ParsedExpression result = new FirstFunction().invoke(firstInput);

    assertThat(result)
        .isResultFor(firstInput)
        .hasSameTypeAs(input)
        .isPrimitive()
        .isSingular()
        .isSelection()
        .isAggreation();
    // Check that the correct rows were included in the result

    List<Row> expectedRows = Arrays
        .asList(STRING_1_1_JUDE, STRING_2_1_SAMUEL, STRING_3_1_NULL, STRING_4_1_ADAM,
            STRING_5_1_NULL);
    assertThat(result).selectResult().hasRows(expectedRows);
    assertThat(result).aggByIdResult().hasRows(expectedRows);
    assertThat(result).aggResult().isValue().isEqualTo((STRING_1_1_JUDE.getString(1)));
  }

  @Test
  public void testProducesEmptyListFromEmptyListOfValues() {

    Dataset<Row> dataset = StringPrimitiveRowFixture.createEmptyDataset(spark);
    // Build up an input for the function.
    ParsedExpression input = createPrimitiveParsedExpression(dataset);

    FunctionInput firstInput = new FunctionInput();
    firstInput.setInput(input);
    firstInput.setExpression("name.family.first()");

    ParsedExpression result = new FirstFunction().invoke(firstInput);

    assertThat(result)
        .isResultFor(firstInput)
        .hasSameTypeAs(input)
        .isPrimitive()
        .isSingular()
        .isSelection()
        .isAggreation();

    // Check that the correct rows were included in the result
    assertThat(result).selectResult().isEmpty();
    assertThat(result).aggByIdResult().isEmpty();
    assertThat(result).aggResult().isValue().isNull();
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