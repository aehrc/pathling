/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
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
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author Piotr Szul
 */
@Tag("UnitTest")
public class FirstFunctionTest {

  private FhirContext fhirContext;

  @BeforeEach
  public void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
  }

  @Test
  public void firstOfRootResources() {
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(Patient.class);
    final ResourceDefinition resourceDefinition = new ResourceDefinition(ResourceType.PATIENT,
        hapiDefinition);
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", RowFactory.create("Patient/abc1", "female", true))
        .withRow("Patient/abc2", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Patient/abc3", RowFactory.create("Patient/abc3", "male", true))
        .buildWithStructValue();

    final Column idColumn = inputDataset.col("id");
    final Column valueColumn = inputDataset.col("value");
    final ResourcePath inputPath = new ResourcePath("Patient", inputDataset,
        Optional.of(idColumn), valueColumn, false, Optional.empty(), resourceDefinition);

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

    @SuppressWarnings("UnnecessaryLocalVariable") final Dataset<Row> expectedDataset = inputDataset;

    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void firstOfUngroupedSubResources() {

    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(EpisodeOfCare.class);
    final ResourceDefinition resourceDefinition = new ResourceDefinition(ResourceType.EPISODEOFCARE,
        hapiDefinition);

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc1", "planned"))
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc2", "planned"))
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc4", "active"))
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc5", "active"))
        .withRow("Encounter/xyz2", RowFactory.create("EpisodeOfCare/abc3", "active"))
        .withRow("Encounter/xyz3", null)
        .withRow("Encounter/xyz3", RowFactory.create("EpisodeOfCare/abc3", "waitlist"))
        .withRow("Encounter/xyz4", null)
        .buildWithStructValue()
        .repartition(3);

    final Column idColumn = inputDataset.col("id");
    final Column valueColumn = inputDataset.col("value");

    final ResourcePath inputPath = new ResourcePath("Encounter.episodeOfCare.resolve()",
        inputDataset,
        Optional.of(idColumn), valueColumn, false, Optional.empty(), resourceDefinition);

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
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc1", "planned"))
        .withRow("Encounter/xyz2", RowFactory.create("EpisodeOfCare/abc3", "active"))
        .withRow("Encounter/xyz3", RowFactory.create("EpisodeOfCare/abc3", "waitlist"))
        .withRow("Encounter/xyz4", null)
        .buildWithStructValue();

    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void firstOfUngroupedElements() {

    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")   // when: "two values"  expect: "Jude"
        .withRow("Patient/abc1", "Mark")
        .withRow("Patient/abc1", "Mark")
        .withRow("Patient/abc1", "Mark")
        .withRow("Patient/abc2", "Samuel") // when: "single value" expect: "Samuel"
        .withRow("Patient/abc3", null)     // when: "leading null" expect: "Adam"
        .withRow("Patient/abc3", "Adam")
        .withRow("Patient/abc4", "John")  // when: "trailing null" expect: "John"
        .withRow("Patient/abc4", null)
        .withRow("Patient/abc5", null)    // when: "single null" expect: null
        .withRow("Patient/abc6", null)    // when: "many nulls" expect: null
        .withRow("Patient/abc6", null)
        .build()
        .repartition(3);

    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
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
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")
        .withRow("Patient/abc2", "Samuel")
        .withRow("Patient/abc3", "Adam")
        .withRow("Patient/abc4", "John")
        .withRow("Patient/abc5", null)
        .withRow("Patient/abc6", null)
        .build();

    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void firstOfGrouping() {
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(Patient.class);
    final ResourceDefinition resourceDefinition = new ResourceDefinition(ResourceType.PATIENT,
        hapiDefinition);
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender_value", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc2", "female", RowFactory.create("Patient/abc2", "female", true))
        .withRow("Patient/abc1", "female", RowFactory.create("Patient/abc1", "female", false))
        .withRow("Patient/abc3", "male", RowFactory.create("Patient/abc3", "male", false))
        .buildWithStructValue();

    final Column idColumn = inputDataset.col("id");
    final Column valueColumn = inputDataset.col("value");
    final Column groupingColumn = inputDataset.col("gender_value");
    final ResourcePath inputPath = new ResourcePath("Patient", inputDataset,
        Optional.of(idColumn), valueColumn, false, Optional.empty(), resourceDefinition);

    final ParserContext parserContext = new ParserContextBuilder()
        .groupingColumns(Collections.singletonList(groupingColumn))
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("first");
    final FhirPath result = count.invoke(countInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("first()")
        .isSingular()
        .hasResourceType(ResourceType.PATIENT);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("gender_value", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("female", RowFactory.create("Patient/abc2", "female", true))
        .withRow("male", RowFactory.create("Patient/abc3", "male", false))
        .buildWithStructValue();

    assertThat(result)
        .selectGroupingResult(Collections.singletonList(groupingColumn))
        .hasRows(expectedDataset);
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
