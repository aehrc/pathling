/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.assertions.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.TestAggregationParserContext;
import au.csiro.pathling.test.TestParserContext;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Collections;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class CountFunctionTest {

  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    fhirContext = FhirHelpers.getFhirContext();
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void countsByResourceIdentity() {
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

    final ParserContext parserContext = TestParserContext.builder()
        .idColumn(inputPath.getIdColumn())
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final FhirPath result = count.invoke(countInput);

    assertTrue(result instanceof IntegerPath);
    assertThat((ElementPath) result)
        .hasExpression("count()")
        .isSingular()
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.LongType)
        .withRow("Patient/abc1", 1L)
        .withRow("Patient/abc2", 1L)
        .withRow("Patient/abc3", 1L)
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void countsByGrouping() {
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(Patient.class);
    final ResourceDefinition resourceDefinition = new ResourceDefinition(ResourceType.PATIENT,
        hapiDefinition);
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", "female", RowFactory.create("Patient/abc1", "female", true))
        .withRow("Patient/abc2", "female", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Patient/abc2", "female", RowFactory.create("Patient/abc3", "male", true))
        .buildWithStructValue();
    final Column idColumn = inputDataset.col("id");
    final Column valueColumn = inputDataset.col("value");
    final Column groupingColumn = inputDataset.col("gender");
    final ResourcePath inputPath = new ResourcePath("", inputDataset, idColumn, valueColumn,
        false, resourceDefinition);

    final ParserContext parserContext = TestAggregationParserContext
        .build(Collections.singletonList(groupingColumn));
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final FhirPath result = count.invoke(countInput);

    assertTrue(result instanceof IntegerPath);
    assertThat((ElementPath) result)
        .hasExpression("count()")
        .isSingular()
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("count", DataTypes.LongType)
        .withRow("female", 2L)
        .withRow("male", 1L)
        .build();
    assertThat(expectedDataset)
        .hasRows(expectedDataset);
  }
}