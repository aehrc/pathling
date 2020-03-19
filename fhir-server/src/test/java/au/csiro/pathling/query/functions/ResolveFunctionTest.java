/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.getFhirContext;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.query.ResourceReader;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.ComplexExpressionBuilder;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Arrays;
import java.util.HashSet;
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
public class ResolveFunctionTest {

  @Test
  public void simpleResolve() {
    // Build an expression which represents the input to the function.
    BaseRuntimeChildDefinition childDefinition = getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("episodeOfCare");
    Assertions.assertThat(childDefinition).isInstanceOf(RuntimeChildResourceDefinition.class);
    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructColumn("reference", DataTypes.StringType)
        .withStructColumn("display", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create("EpisodeOfCare/abc3", null))
        .withRow("Encounter/xyz3", RowFactory.create("EpisodeOfCare/abc2", null))
        .withRow("Encounter/xyz4", RowFactory.create("EpisodeOfCare/abc2", null))
        .buildWithStructValue("789wxyz");
    inputExpression.setSingular(true);
    inputExpression.setDefinition(childDefinition, "episodeOfCare");

    // Build a Dataset with several rows in it, and create a mock reader which returns it as the
    // set of all EpisodeOfCare resources.
    Dataset<Row> episodeOfCareDataset = new DatasetBuilder()
        .withColumn("id", DataTypes.StringType)
        .withColumn("status", DataTypes.StringType)
        .withRow("EpisodeOfCare/abc1", "planned")
        .withRow("EpisodeOfCare/abc2", "waitlist")
        .withRow("EpisodeOfCare/abc3", "active")
        .build();
    ResourceReader mockReader = mock(ResourceReader.class);
    when(mockReader.read(ResourceType.EPISODEOFCARE))
        .thenReturn(episodeOfCareDataset);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);
    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setContext(parserContext);
    resolveInput.setInput(inputExpression);
    resolveInput.setExpression("resolve()");

    // Execute the function.
    ResolveFunction resolveFunction = new ResolveFunction();
    ParsedExpression result = resolveFunction.invoke(resolveInput);

    // Check the result.
    assertThat(result).hasFhirPath("resolve()");
    assertThat(result).isSingular();
    assertThat(result).isResourceOfType(ResourceType.EPISODEOFCARE, FHIRDefinedType.EPISODEOFCARE);
    assertThat(result).isNotPolymorphic();

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc1", "planned"))
        .withRow("Encounter/xyz2", RowFactory.create("EpisodeOfCare/abc3", "active"))
        .withRow("Encounter/xyz3", RowFactory.create("EpisodeOfCare/abc2", "waitlist"))
        .withRow("Encounter/xyz4", RowFactory.create("EpisodeOfCare/abc2", "waitlist"))
        .buildWithStructValue("123abcd");
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void polymorphicResolve() {
    // Build a dataset which represents the input to the function.
    BaseRuntimeChildDefinition childDefinition = getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("subject");
    Assertions.assertThat(childDefinition).isInstanceOf(RuntimeChildResourceDefinition.class);
    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructColumn("reference", DataTypes.StringType)
        .withStructColumn("display", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("Patient/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create("Patient/abc3", null))
        .withRow("Encounter/xyz3", RowFactory.create("Patient/abc2", null))
        .withRow("Encounter/xyz4", RowFactory.create("Patient/abc2", null))
        .withRow("Encounter/xyz5", RowFactory.create("Group/def1", null))
        .buildWithStructValue("789wxyz");
    inputExpression.setSingular(true);
    inputExpression.setDefinition(childDefinition, "subject");

    // Build a Dataset with several rows in it, and create a mock reader which returns it as the
    // set of all Patient resources.
    Dataset<Row> patientDataset = new DatasetBuilder()
        .withColumn("id", DataTypes.StringType)
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", "female", true)
        .withRow("Patient/abc2", "female", false)
        .withRow("Patient/abc3", "male", true)
        .build();
    ResourceReader mockReader = mock(ResourceReader.class);
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);

    // Build a Dataset with several rows in it, and create a mock reader which returns it as the
    // set of all Group resources.
    Dataset<Row> groupDataset = new DatasetBuilder()
        .withColumn("id", DataTypes.StringType)
        .withColumn("name", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Group/def1", "Some group", true)
        .build();
    when(mockReader.read(ResourceType.GROUP))
        .thenReturn(groupDataset);

    // Mock out the call to check available resource types using the resource reader.
    when(mockReader.getAvailableResourceTypes())
        .thenReturn(new HashSet<>(Arrays.asList(ResourceType.PATIENT, ResourceType.GROUP)));

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);
    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setContext(parserContext);
    resolveInput.setInput(inputExpression);
    resolveInput.setExpression("resolve()");

    // Execute the function.
    ResolveFunction resolveFunction = new ResolveFunction();
    ParsedExpression result = resolveFunction.invoke(resolveInput);

    // Check the result.
    assertThat(result).hasFhirPath("resolve()");
    assertThat(result).isSingular();
    assertThat(result).isPolymorphic();
    assertThat(result).isOfType(null, null);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd_type", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("Encounter/xyz1", "Patient", "Patient/abc1")
        .withRow("Encounter/xyz2", "Patient", "Patient/abc3")
        .withRow("Encounter/xyz3", "Patient", "Patient/abc2")
        .withRow("Encounter/xyz4", "Patient", "Patient/abc2")
        .withRow("Encounter/xyz5", "Group", "Group/def1")
        .build();
    assertThat(result)
        .selectPolymorphicResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void polymorphicResolveAnyType() {
    // Build a dataset which represents the input to the function.
    BaseRuntimeElementDefinition<?> elementDefinition = getFhirContext()
        .getResourceDefinition("Condition")
        .getChildByName("evidence")
        .getChildByName("evidence");
    BaseRuntimeChildDefinition childDefinition = ((BaseRuntimeElementCompositeDefinition) elementDefinition)
        .getChildByName("detail");
    Assertions.assertThat(childDefinition).isInstanceOf(RuntimeChildResourceDefinition.class);
    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.REFERENCE)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructColumn("reference", DataTypes.StringType)
        .withStructColumn("display", DataTypes.StringType)
        .withRow("Condition/xyz1", RowFactory.create("Observation/abc1", null))
        .withRow("Condition/xyz2", RowFactory.create("ClinicalImpression/def1", null))
        .buildWithStructValue("789wxyz");
    inputExpression.setDefinition(childDefinition, "detail");

    // Build a Dataset and create a mock reader which returns it as the set of all Observation
    // resources.
    Dataset<Row> observationDataset = new DatasetBuilder()
        .withColumn("id", DataTypes.StringType)
        .withColumn("status", DataTypes.StringType)
        .withRow("Observation/abc1", "registered")
        .build();
    ResourceReader mockReader = mock(ResourceReader.class);
    when(mockReader.read(ResourceType.OBSERVATION))
        .thenReturn(observationDataset);

    // Build a Dataset and create a mock reader which returns it as the set of all
    // ClinicalImpression resources.
    Dataset<Row> clinicalImpressionDataset = new DatasetBuilder()
        .withColumn("id", DataTypes.StringType)
        .withColumn("status", DataTypes.StringType)
        .withRow("ClinicalImpression/def1", "in-progress")
        .build();
    when(mockReader.read(ResourceType.CLINICALIMPRESSION))
        .thenReturn(clinicalImpressionDataset);

    // Mock out the call to check available resource types using the resource reader.
    when(mockReader.getAvailableResourceTypes())
        .thenReturn(new HashSet<>(
            Arrays.asList(ResourceType.OBSERVATION, ResourceType.CLINICALIMPRESSION)));

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);
    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setContext(parserContext);
    resolveInput.setInput(inputExpression);
    resolveInput.setExpression("resolve()");

    // Execute the function.
    ResolveFunction resolveFunction = new ResolveFunction();
    ParsedExpression result = resolveFunction.invoke(resolveInput);

    // Check the result.
    assertThat(result).hasFhirPath("resolve()");
    assertThat(result).isNotSingular();
    assertThat(result).isPolymorphic();
    assertThat(result).isOfType(null, null);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd_type", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("Condition/xyz1", "Observation", "Observation/abc1")
        .withRow("Condition/xyz2", "ClinicalImpression", "ClinicalImpression/def1")
        .build();
    assertThat(result)
        .selectPolymorphicResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwExceptionWhenInputNotReference() {
    ParsedExpression input = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING).build();
    input.setFhirPath("gender");

    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setInput(input);
    ResolveFunction resolveFunction = new ResolveFunction();

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> resolveFunction.invoke(resolveInput))
        .withMessage("Input to resolve function must be a Reference: gender");
  }

  @Test
  public void throwExceptionWhenArgumentSupplied() {
    ParsedExpression input = new PrimitiveExpressionBuilder(FHIRDefinedType.REFERENCE, null)
        .build();
    ParsedExpression argument = PrimitiveExpressionBuilder.literalString("foo");

    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setInput(input);
    resolveInput.getArguments().add(argument);
    ResolveFunction resolveFunction = new ResolveFunction();

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> resolveFunction.invoke(resolveInput))
        .withMessage("resolve function does not accept arguments");
  }

}