/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.builders.UntypedResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class OfTypeFunctionTest {

  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
    mockReader = mock(ResourceReader.class);
  }

  @Test
  void resolvesPolymorphicReference() {
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withTypeColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/xyz1", "Patient", RowFactory.create(null, "Patient/abc1", null))
        .withRow("Encounter/xyz2", "Patient", RowFactory.create(null, "Patient/abc3", null))
        .withRow("Encounter/xyz3", "Patient", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz4", "Patient", RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz5", "Group", RowFactory.create(null, "Group/def1", null))
        .buildWithStructValue();
    final Column idColumn = inputDataset.col(inputDataset.columns()[0]);
    final Column typeColumn = inputDataset.col("type");
    final Column valueColumn = inputDataset.col(inputDataset.columns()[2]);
    final UntypedResourcePath inputPath = UntypedResourcePath
        .build("subject.resolve()", inputDataset, idColumn, valueColumn, true,
            Optional.empty(), typeColumn,
            new HashSet<>(Arrays.asList(ResourceType.PATIENT, ResourceType.GROUP)));

    final Dataset<Row> argumentDataset = new DatasetBuilder()
        .withIdColumn("id")
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", "female", true)
        .withRow("Patient/abc2", "female", false)
        .withRow("Patient/abc3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(argumentDataset);
    final ResourcePath argumentPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "Patient", false);

    final ParserContext parserContext = new ParserContextBuilder()
        .idColumn(inputPath.getIdColumn())
        .build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction count = NamedFunction.getInstance("ofType");
    final FhirPath result = count.invoke(ofTypeInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("subject.resolve().ofType(Patient)")
        .isSingular()
        .hasResourceType(ResourceType.PATIENT);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("id", DataTypes.StringType)
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Encounter/xyz1", "Patient/abc1", "female", true)
        .withRow("Encounter/xyz2", "Patient/abc3", "male", true)
        .withRow("Encounter/xyz3", "Patient/abc2", "female", false)
        .withRow("Encounter/xyz4", "Patient/abc2", "female", false)
        .withRow("Encounter/xyz5", null, null, null)
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorIfInputNotPolymorphic() {
    final ResourcePath input = new ResourcePathBuilder()
        .expression("Patient")
        .build();
    final ResourcePath argument = new ResourcePathBuilder().build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction ofTypeFunction = NamedFunction.getInstance("ofType");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> ofTypeFunction.invoke(ofTypeInput));
    assertEquals(
        "Input to ofType function must be a polymorphic resource type: Patient",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    final UntypedResourcePath input = new UntypedResourcePathBuilder()
        .expression("subject")
        .build();
    final ResourcePath argument1 = new ResourcePathBuilder()
        .expression("Patient")
        .build();
    final ResourcePath argument2 = new ResourcePathBuilder()
        .expression("Condition")
        .build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, input,
        Arrays.asList(argument1, argument2));

    final NamedFunction ofTypeFunction = NamedFunction.getInstance("ofType");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> ofTypeFunction.invoke(ofTypeInput));
    assertEquals(
        "ofType function must have one argument: subject.ofType(Patient, Condition)",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentNotResource() {
    final UntypedResourcePath input = new UntypedResourcePathBuilder()
        .expression("subject")
        .build();
    final StringLiteralPath argument = StringLiteralPath
        .fromString("'some string'", input);

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction ofTypeFunction = NamedFunction.getInstance("ofType");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> ofTypeFunction.invoke(ofTypeInput));
    assertEquals("Argument to ofType function must be a resource type: 'some string'",
        error.getMessage());
  }

}