/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.assertions.Assertions.assertTrue;
import static au.csiro.pathling.test.helpers.SparkHelpers.referenceStructType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.TestElementPath;
import au.csiro.pathling.test.TestParserContext;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Encounter;
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
        .withRow("Patient/abc1", "female", true)
        .withRow("Patient/abc2", "female", false)
        .withRow("Patient/abc3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "", false);

    final DatasetBuilder encounterDatasetBuilder = new DatasetBuilder()
        .withIdColumn()
        .withColumn("status", DataTypes.StringType);

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "subject");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> argumentDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(TestElementPath.ORIGIN_COLUMN, encounterDatasetBuilder.getStructType())
        .withStructTypeColumns(referenceStructType())
        .withRow("Encounter/xyz1", RowFactory.create("Encounter/xyz1", "planned"),
            RowFactory.create(null, "Patient/abc1", null))
        .withRow("Encounter/xyz2", RowFactory.create("Encounter/xyz2", "arrived"),
            RowFactory.create(null, "Patient/abc3", null))
        .withRow("Encounter/xyz3", RowFactory.create("Encounter/xyz3", "triaged"),
            RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz4", RowFactory.create("Encounter/xyz4", "in-progress"),
            RowFactory.create(null, "Patient/abc2", null))
        .withRow("Encounter/xyz5", RowFactory.create("Encounter/xyz5", "onleave"),
            RowFactory.create(null, "Group/def1", null))
        .buildWithStructValue();
    final ResourceDefinition originDefinition = new ResourceDefinition(ResourceType.ENCOUNTER,
        fhirContext.getResourceDefinition(Encounter.class));
    final FhirPath argumentPath = TestElementPath
        .build("Encounter.subject", argumentDataset, false, originDefinition, definition);

    final ParserContext parserContext = TestParserContext.builder()
        .idColumn(inputPath.getIdColumn())
        .resourceReader(mockReader)
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));
    final NamedFunction count = NamedFunction.getInstance("reverseResolve");
    final FhirPath result = count.invoke(countInput);

    assertTrue(result instanceof ResourcePath);
    assertThat((ResourcePath) result)
        .hasExpression("reverseResolve(Encounter.subject)")
        .isNotSingular()
        .hasResourceType(ResourceType.ENCOUNTER);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Patient/abc1", RowFactory.create("Encounter/xyz1", "planned"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/xyz3", "triaged"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/xyz4", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/xyz2", "arrived"))
        .buildWithStructValue();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

}