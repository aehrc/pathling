/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.getFhirContext;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ExpressionParserContext;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.ComplexExpressionBuilder;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import au.csiro.pathling.test.ResourceExpressionBuilder;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.assertj.core.api.Assertions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class ReverseResolveFunctionTest {

  @Test
  public void reverseResolve() {
    // Build an expression which represents the input to the function.
    ParsedExpression inputExpression = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", RowFactory.create("female", true))
        .withRow("Patient/abc2", RowFactory.create("female", false))
        .withRow("Patient/abc3", RowFactory.create("male", true))
        .buildWithStructValue("123abcd");
    inputExpression.setSingular(true);
    inputExpression.setOrigin(inputExpression);

    // Build an expression to serve as the origin within the argument.
    ParsedExpression originExpression = new ResourceExpressionBuilder(ResourceType.ENCOUNTER,
        FHIRDefinedType.ENCOUNTER)
        .withColumn("456mnop_id", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("Encounter/xyz1", "triaged"))
        .withRow("Encounter/xyz2", RowFactory.create("Encounter/xyz2", "onleave"))
        .withRow("Encounter/xyz3", RowFactory.create("Encounter/xyz3", "planned"))
        .buildWithStructValue("456mnop");
    originExpression.setSingular(true);

    // Build an expression that resembles what would be passed as an argument.
    BaseRuntimeChildDefinition childDefinition = getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("subject");
    Assertions.assertThat(childDefinition).isInstanceOf(RuntimeChildResourceDefinition.class);
    Dataset<Row> argumentDataset = new DatasetBuilder()
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructColumn("reference", DataTypes.StringType)
        .withStructColumn("display", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("Patient/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create("Patient/abc2", null))
        .withRow("Encounter/xyz3", RowFactory.create("Patient/abc2", null))
        .buildWithStructValue("789wxyz");
    ParsedExpression argumentExpression = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructColumn("reference", DataTypes.StringType)
        .withStructColumn("display", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("Patient/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create("Patient/abc2", null))
        .withRow("Encounter/xyz3", RowFactory.create("Patient/abc2", null))
        .buildWithStructValue("789wxyz");
    // This is required, as this function assumes that the value column from the origin dataset will
    // be present in the argument dataset.
    Dataset<Row> joinedArgumentDataset = originExpression.getDataset()
        .join(argumentExpression.getDataset(),
            originExpression.getIdColumn().equalTo(argumentExpression.getIdColumn()),
            "left_outer");
    argumentExpression.setDataset(joinedArgumentDataset);
    argumentExpression.setSingular(true);
    argumentExpression.setDefinition(childDefinition, "subject");
    argumentExpression.setOrigin(originExpression);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    FunctionInput reverseResolveInput = new FunctionInput();
    reverseResolveInput.setContext(parserContext);
    reverseResolveInput.setInput(inputExpression);
    reverseResolveInput.getArguments().add(argumentExpression);
    reverseResolveInput.setExpression("reverseResolve(Encounter.subject)");

    // Execute the function.
    ReverseResolveFunction reverseResolveFunction = new ReverseResolveFunction();
    ParsedExpression result = reverseResolveFunction.invoke(reverseResolveInput);

    // Check the result.
    assertThat(result).hasFhirPath("reverseResolve(Encounter.subject)");
    assertThat(result).isNotSingular();
    assertThat(result).isResourceOfType(ResourceType.ENCOUNTER, FHIRDefinedType.ENCOUNTER);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Patient/abc1", RowFactory.create("Encounter/xyz1", "triaged"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/xyz3", "planned"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/xyz2", "onleave"))
        .withRow("Patient/abc3", null)
        .buildWithStructValue("123abcd");
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorIfInputNotResource() {
    ParsedExpression input = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .build();
    input.setFhirPath("gender");
    ParsedExpression argument = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE)
        .build();

    FunctionInput reverseResolveInput = new FunctionInput();
    reverseResolveInput.setInput(input);
    reverseResolveInput.getArguments().add(argument);

    ReverseResolveFunction reverseResolveFunction = new ReverseResolveFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> reverseResolveFunction.invoke(reverseResolveInput))
        .withMessage("Input to reverseResolve function must be Resource: gender");
  }

  @Test
  public void throwsErrorIfArgumentIsNotReference() {
    ParsedExpression input = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .build();
    ParsedExpression argument = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .build();
    argument.setFhirPath("gender");

    FunctionInput reverseResolveInput = new FunctionInput();
    reverseResolveInput.setInput(input);
    reverseResolveInput.getArguments().add(argument);

    ReverseResolveFunction reverseResolveFunction = new ReverseResolveFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> reverseResolveFunction.invoke(reverseResolveInput))
        .withMessage("Argument to reverseResolve function must be Reference: gender");
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    ParsedExpression input = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .build();
    ParsedExpression argument1 = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE).build(),
        argument2 = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE).build();

    FunctionInput reverseResolveInput = new FunctionInput();
    reverseResolveInput.setInput(input);
    reverseResolveInput.getArguments().add(argument1);
    reverseResolveInput.getArguments().add(argument2);
    reverseResolveInput
        .setExpression("reverseResolve(Encounter.subject, Encounter.participant.individual)");

    ReverseResolveFunction reverseResolveFunction = new ReverseResolveFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> reverseResolveFunction.invoke(reverseResolveInput))
        .withMessage(
            "reverseResolve function accepts one argument: reverseResolve(Encounter.subject, Encounter.participant.individual)");
  }

  @Test
  public void throwsErrorIfArgumentTypeDoesNotMatchInput() {
    ParsedExpression input = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .build();

    BaseRuntimeChildDefinition childDefinition = getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("episodeOfCare");
    ParsedExpression argument = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE).build();
    argument.setDefinition(childDefinition, "episodeOfCare");

    FunctionInput reverseResolveInput = new FunctionInput();
    reverseResolveInput.setInput(input);
    reverseResolveInput.getArguments().add(argument);
    reverseResolveInput.setExpression("reverseResolve(Encounter.episodeOfCare)");

    ReverseResolveFunction reverseResolveFunction = new ReverseResolveFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> reverseResolveFunction.invoke(reverseResolveInput))
        .withMessage(
            "Reference in argument to reverseResolve does not support input resource type: reverseResolve(Encounter.episodeOfCare)");
  }

}