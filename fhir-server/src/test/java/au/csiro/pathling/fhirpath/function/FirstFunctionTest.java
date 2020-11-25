/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.*;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.EpisodeOfCare;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author Piotr Szul
 */
@Tag("UnitTest")
public class FirstFunctionTest {

  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void firstOfRootResources() {

    final Dataset<Row> patientDataset = new ResourceDatasetBuilder()
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

    final ParserContext parserContext = new ParserContextBuilder()
        .inputExpression("Patient")
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("first()")
        .isSingular()
        .hasResourceType(ResourceType.PATIENT);

    @SuppressWarnings("UnnecessaryLocalVariable")
    final Dataset<Row> expectedDataset = patientDataset;

    assertThat(result)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void firstOfUngroupedSubResources() {

    // @TODO: Merge : FIX
    /*
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(EpisodeOfCare.class);
    final ResourceDefinition resourceDefinition = new ResourceDefinition(ResourceType.EPISODEOFCARE,
        hapiDefinition);

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", makeEid(0, 0),
            RowFactory.create("EpisodeOfCare/abc1", "planned"))
        .withRow("Encounter/xyz1", makeEid(0, 1),
            RowFactory.create("EpisodeOfCare/abc2", "planned"))
        .withRow("Encounter/xyz1", makeEid(0, 2), RowFactory.create("EpisodeOfCare/abc4", "active"))
        .withRow("Encounter/xyz1", makeEid(0, 3), RowFactory.create("EpisodeOfCare/abc5", "active"))
        .withRow("Encounter/xyz2", makeEid(0, 0), RowFactory.create("EpisodeOfCare/abc3", "active"))
        .withRow("Encounter/xyz3", makeEid(0, 0), null)
        .withRow("Encounter/xyz3", makeEid(0, 1),
            RowFactory.create("EpisodeOfCare/abc3", "waitlist"))
        .withRow("Encounter/xyz4", null, null)
        .buildWithStructValue()
        .repartition(3);

    final Column idColumn = inputDataset.col("id");
    final Column valueColumn = inputDataset.col("value");
    final Column eidColumn = inputDataset.col("eid");

    final ResourcePath inputPath = new ResourcePath("Encounter.episodeOfCare.resolve()",
        inputDataset,
        Optional.of(idColumn), Optional.of(eidColumn), valueColumn, false, Optional.empty(),
        resourceDefinition);

    final ParserContext parserContext = new ParserContextBuilder()
        .inputExpression("Encounter")
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("Encounter.episodeOfCare.resolve().first()")
        .isSingular()
        .hasResourceType(ResourceType.EPISODEOFCARE);

    // expected result dataset
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", null,
            RowFactory.create("EpisodeOfCare/abc1", "planned"))
        .withRow("Encounter/xyz2", null, RowFactory.create("EpisodeOfCare/abc3", "active"))
        .withRow("Encounter/xyz3", null,
            RowFactory.create("EpisodeOfCare/abc3", "waitlist"))
        .withRow("Encounter/xyz4", null, null)
        .buildWithStructValue();

    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);

     */
  }

  @Test
  public void firstOfUngroupedElements() {

    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/abc1", makeEid(0, 3), "Jude")   // when: "two values"  expect: "Jude"
        .withRow("Patient/abc1", makeEid(0, 2), "Mark")
        .withRow("Patient/abc1", makeEid(0, 1), "Mark")
        .withRow("Patient/abc1", makeEid(0, 0), "Zaak")
        .withRow("Patient/abc2", makeEid(0, 0), "Samuel") // when: "single value" expect: "Samuel"
        .withRow("Patient/abc3", makeEid(0, 1), "Adam") // when: "leading null" expect: "Adam"
        .withRow("Patient/abc3", makeEid(0, 0), null)
        .withRow("Patient/abc4", makeEid(0, 1), null) // when: "trailing null" expect: "John
        .withRow("Patient/abc4", makeEid(0, 0), "John")
        .withRow("Patient/abc5", null, null)    // when: "single null" expect: null
        .withRow("Patient/abc6", null, null)    // when: "many nulls" expect: null
        .withRow("Patient/abc6", null, null)
        .build();

    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .eidColumn()
        .expression("Patient.name")
        .build();

    final ParserContext parserContext = new ParserContextBuilder()
        .inputExpression("Patient.name")
        .build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());

    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final FhirPath result = firstFunction.invoke(firstInput);

    assertTrue(result instanceof StringPath);
    assertThat((ElementPath) result)
        .hasExpression("first()")
        .isSingular()
        .hasFhirType(FHIRDefinedType.STRING);

    // expected result dataset
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/abc1", null, "Zaak")
        .withRow("Patient/abc2", null, "Samuel")
        .withRow("Patient/abc3", null, "Adam")
        .withRow("Patient/abc4", null, "John")
        .withRow("Patient/abc5", null, null)
        .withRow("Patient/abc6", null, null)
        .build();

    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void illegalToCallFirstOnGrouping() {
    final Dataset<Row> inputDataset = new ResourceDatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/1", "female", true)
        .withRow("Patient/2", "female", false)
        .withRow("Patient/2", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT)).thenReturn(inputDataset);
    final ResourcePath inputPath = new ResourcePathBuilder()
        .resourceReader(mockReader)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();

    final Column groupingColumn = inputPath.getElementColumn("gender");

    final ParserContext parserContext = new ParserContextBuilder()
        .groupingColumns(Collections.singletonList(groupingColumn))
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction firstFunction = NamedFunction.getInstance("first");

    final IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> firstFunction.invoke(firstInput));
    assertEquals(
        "Orderable path expected",
        error.getMessage());
  }

  @Test
  public void inputMustNotContainArguments() {
    final ElementPath inputPath = new ElementPathBuilder().build();
    final ElementPath argumentPath = new ElementPathBuilder().build();
    final ParserContext parserContext = new ParserContextBuilder().build();

    final NamedFunctionInput firstInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));

    final NamedFunction firstFunction = NamedFunction.getInstance("first");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> firstFunction.invoke(firstInput));
    assertEquals(
        "Arguments can not be passed to first function",
        error.getMessage());
  }
}
