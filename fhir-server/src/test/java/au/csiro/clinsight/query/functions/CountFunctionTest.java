package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.test.FunctionTest;
import au.csiro.clinsight.test.PatientResourceRowFixture;
import au.csiro.clinsight.test.StringPrimitiveRowFixture;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * @author John Grimes
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class CountFunctionTest  extends FunctionTest {

  @Test
  public void testCountsResourcesCorrectly() {
    // Build a Dataset with several rows in it.
		Dataset<Row> dataset = PatientResourceRowFixture.createCompleteDataset(spark);
		// Build up an input for the function.
		ParsedExpression input = createResourceParsedExpression(dataset, ResourceType.PATIENT);
		
		FunctionInput functionInput = new FunctionInput();
		functionInput.setInput(input);
		functionInput.setExpression("count()");

		// Execute the first function.
		Function function = new CountFunction();
		ParsedExpression result = function.invoke(functionInput);		
		
		assertThat(result)
		.isResultFor(functionInput)
		.isOfType(FHIRDefinedType.UNSIGNEDINT, FhirPathType.INTEGER)
		.isPrimitive()
		.isSingular()
		.isSelection()
		.isAggreation();
		
		// check results
		assertThat(result).aggResult().isValue().isEqualTo(3L);
		assertThat(result).aggByIdResult().hasRows(
				RowFactory.create("abc1", 1L), 
				RowFactory.create("abc2", 1L),
				RowFactory.create("abc3", 1L) 				
		);
  }

  @Test
  public void testCountsElementsCorrectly() {
		Dataset<Row> dataset = StringPrimitiveRowFixture.createCompleteDataset(spark);

		// Build up an input for the functionm
		ParsedExpression input = createPrimitiveParsedExpression(dataset);
		FunctionInput functionInput = new FunctionInput();
		functionInput.setInput(input);
		functionInput.setExpression("name.family.count()");
	  
		// TODO: is this necessaey
		ExpressionParserContext expressionParserContext = new ExpressionParserContext();
    expressionParserContext.getGroupings().add(mock(ParsedExpression.class));
    functionInput.setContext(expressionParserContext);

		// Execute the fist function.
		ParsedExpression result = new CountFunction().invoke(functionInput);
    
		assertThat(result)
		.isResultFor(functionInput)
		.isOfType(FHIRDefinedType.UNSIGNEDINT, FhirPathType.INTEGER)
		.isPrimitive()
		.isSingular()
		.isSelection()
		.isAggreation();

		// check results
		assertThat(result).aggResult().isValue().isEqualTo(4L);
		assertThat(result).aggByIdResult().hasRows(
				RowFactory.create("abc1", 1L), 
				RowFactory.create("abc2", 2L),
				RowFactory.create("abc4", 1L) 				
		);
  }

  @Test
  public void inputMustNotContainArguments() {
    // Build up an input for the function.
    ExpressionParserContext expressionParserContext = new ExpressionParserContext();
    expressionParserContext.getGroupings().add(mock(ParsedExpression.class));

    FunctionInput countInput = new FunctionInput();
    countInput.setInput(mock(ParsedExpression.class));
    countInput.setExpression("count('some argument')");
    countInput.setContext(expressionParserContext);
    countInput.getArguments().add(mock(ParsedExpression.class));

    // Execute the function and assert that it throws the right exception.
    CountFunction countFunction = new CountFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> countFunction.invoke(countInput))
        .withMessage("Arguments can not be passed to count function: count('some argument')");
  }
}