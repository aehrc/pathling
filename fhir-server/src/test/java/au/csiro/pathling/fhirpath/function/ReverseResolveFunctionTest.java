/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.joinOnId;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.FhirHelpers.getFhirContext;
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static au.csiro.pathling.utilities.Preconditions.check;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class ReverseResolveFunctionTest {

  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void reverseResolve() {
    final Dataset<Row> patientDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/1", "female", true)
        .withRow("Patient/2", "female", false)
        .withRow("Patient/3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "Patient", false);

    final DatasetBuilder encounterDatasetBuilder = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Encounter/1", "planned")
        .withRow("Encounter/2", "arrived")
        .withRow("Encounter/3", "triaged")
        .withRow("Encounter/4", "in-progress")
        .withRow("Encounter/5", "onleave");
    final Dataset<Row> encounterDataset = encounterDatasetBuilder.build();
    when(mockReader.read(ResourceType.ENCOUNTER)).thenReturn(encounterDataset);
    final ResourcePath originPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.ENCOUNTER, "Encounter", false);

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "subject");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> argumentDatasetPreJoin = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/1", RowFactory.create(null, "Patient/1", null))
        .withRow("Encounter/2", RowFactory.create(null, "Patient/3", null))
        .withRow("Encounter/3", RowFactory.create(null, "Patient/2", null))
        .withRow("Encounter/4", RowFactory.create(null, "Patient/2", null))
        .withRow("Encounter/5", RowFactory.create(null, "Group/def1", null))
        .buildWithStructValue();
    final Column valueColumn = argumentDatasetPreJoin.col("value");

    check(originPath.getIdColumn().isPresent());
    final Dataset<Row> argumentDataset = joinOnId(originPath.getDataset(),
        originPath.getIdColumn().get(),
        argumentDatasetPreJoin, argumentDatasetPreJoin.col("id"), JoinType.LEFT_OUTER);
    final FhirPath argumentPath = new ElementPathBuilder()
        .dataset(argumentDataset)
        .idColumn(originPath.getIdColumn().get())
        .valueColumn(valueColumn)
        .expression("Encounter.subject")
        .singular(false)
        .parentPath(originPath)
        .definition(definition)
        .buildDefined();

    check(inputPath.getIdColumn().isPresent());
    final ParserContext parserContext = new ParserContextBuilder()
        .idColumn(inputPath.getIdColumn().get())
        .resourceReader(mockReader)
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction reverseResolve = NamedFunction.getInstance("reverseResolve");
    final FhirPath result = reverseResolve.invoke(reverseResolveInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("reverseResolve(Encounter.subject)")
        .isNotSingular()
        .hasResourceType(ResourceType.ENCOUNTER);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Patient/1", RowFactory.create("Encounter/1", "planned"))
        .withRow("Patient/2", RowFactory.create("Encounter/3", "triaged"))
        .withRow("Patient/2", RowFactory.create("Encounter/4", "in-progress"))
        .withRow("Patient/3", RowFactory.create("Encounter/2", "arrived"))
        .buildWithStructValue();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorIfInputNotResource() {
    final ElementPath input = new ElementPathBuilder()
        .expression("gender")
        .fhirType(FHIRDefinedType.CODE)
        .build();
    final ElementPath argument = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "Input to reverseResolve function must be a resource: gender",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentIsNotReference() {
    final ResourcePath input = new ResourcePathBuilder().build();
    final ElementPath argument = new ElementPathBuilder()
        .expression("gender")
        .fhirType(FHIRDefinedType.CODE)
        .build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "Argument to reverseResolve function must be a Reference: gender",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    final ResourcePath input = new ResourcePathBuilder()
        .expression("Patient")
        .build();
    final ElementPath argument1 = new ElementPathBuilder()
        .expression("Encounter.subject")
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();
    final ElementPath argument2 = new ElementPathBuilder()
        .expression("Encounter.participant.individual")
        .fhirType(FHIRDefinedType.REFERENCE)
        .build();

    final ParserContext parserContext = new ParserContextBuilder()
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Arrays.asList(argument1, argument2));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "reverseResolve function accepts a single argument: reverseResolve(Encounter.subject, Encounter.participant.individual)",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentTypeDoesNotMatchInput() {
    final ResourcePath input = new ResourcePathBuilder()
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();
    final BaseRuntimeChildDefinition childDefinition = getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("episodeOfCare");
    final ElementDefinition definition = ElementDefinition
        .build(childDefinition, "episodeOfCare");
    final ElementPath argument = new ElementPathBuilder()
        .expression("Encounter.episodeOfCare")
        .fhirType(FHIRDefinedType.REFERENCE)
        .definition(definition)
        .buildDefined();

    final ParserContext parserContext = new ParserContextBuilder()
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput reverseResolveInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction reverseResolveFunction = NamedFunction.getInstance("reverseResolve");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> reverseResolveFunction.invoke(reverseResolveInput));
    assertEquals(
        "Reference in argument to reverseResolve does not support input resource type: reverseResolve(Encounter.episodeOfCare)",
        error.getMessage());
  }

}