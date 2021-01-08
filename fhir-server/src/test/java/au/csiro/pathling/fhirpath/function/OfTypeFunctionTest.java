/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
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
import au.csiro.pathling.test.builders.*;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
        .withEidColumn()
        .withTypeColumn()
        .withStructTypeColumns(referenceStructType())
        .withRow("encounter-1", makeEid(1), "Patient",
            RowFactory.create(null, "Patient/patient-1", null))
        .withRow("encounter-1", makeEid(0), "Patient",
            RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-2", makeEid(0), "Patient",
            RowFactory.create(null, "Patient/patient-3", null))
        .withRow("encounter-2", makeEid(1), "Group",
            RowFactory.create(null, "Group/group-1", null))
        .withRow("encounter-3", makeEid(0), "Patient",
            RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-4", makeEid(0), "Patient",
            RowFactory.create(null, "Patient/patient-2", null))
        .withRow("encounter-5", makeEid(0), "Group",
            RowFactory.create(null, "Group/group-1", null))
        .withRow("encounter-6", null, null, null)
        .buildWithStructValue();
    final UntypedResourcePath inputPath = new UntypedResourcePathBuilder()
        .expression("subject.resolve()")
        .dataset(inputDataset)
        .idEidTypeAndValueColumns()
        .singular(false)
        .possibleTypes(new HashSet<>(Arrays.asList(ResourceType.PATIENT, ResourceType.GROUP)))
        .build();

    final Dataset<Row> argumentDataset = new ResourceDatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withColumn(DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(argumentDataset);
    final ResourcePath argumentPath = new ResourcePathBuilder()
        .resourceReader(mockReader)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();

    final ParserContext parserContext = new ParserContextBuilder()
        .idColumn(inputPath.getIdColumn())
        .build();
    final NamedFunctionInput ofTypeInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction ofType = NamedFunction.getInstance("ofType");
    final FhirPath result = ofType.invoke(ofTypeInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("subject.resolve().ofType(Patient)")
        .isNotSingular()
        .hasResourceType(ResourceType.PATIENT);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withIdColumn() // this represents value for a resource
        .withRow("encounter-1", makeEid(0), "patient-2")
        .withRow("encounter-1", makeEid(1), "patient-1")
        .withRow("encounter-2", makeEid(0), "patient-3")
        .withRow("encounter-2", makeEid(1), null)
        .withRow("encounter-3", makeEid(0), "patient-2")
        .withRow("encounter-4", makeEid(0), "patient-2")
        .withRow("encounter-5", makeEid(0), null)
        .withRow("encounter-6", null, null)
        .build();
    assertThat(result)
        .selectOrderedResultWithEid()
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