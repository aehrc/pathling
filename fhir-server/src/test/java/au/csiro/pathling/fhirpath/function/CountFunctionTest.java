/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.*;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class CountFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;
  private ResourceReader mockReader;

  @BeforeEach
  void setUp() {
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void countsByResourceIdentity() {
    final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-3", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath inputPath = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "Patient", false);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputPath.getIdColumn())
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final FhirPath result = count.invoke(countInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.LongType)
        .withRow("patient-1", 1L)
        .withRow("patient-2", 1L)
        .withRow("patient-3", 1L)
        .build();

    assertThat(result)
        .hasExpression("count()")
        .isSingular()
        .isElementPath(IntegerPath.class)
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void countsByGrouping() {
    final Dataset<Row> inputDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", "female", true)
        .withRow("patient-2", "female", false)
        .withRow("patient-2", "male", true)
        .build();
    when(mockReader.read(ResourceType.PATIENT)).thenReturn(inputDataset);
    final ResourcePath inputPath = new ResourcePathBuilder(spark)
        .resourceReader(mockReader)
        .resourceType(ResourceType.PATIENT)
        .expression("Patient")
        .build();
    final Column groupingColumn = inputPath.getElementColumn("gender");

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(groupingColumn))
        .inputExpression("Patient")
        .build();
    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.emptyList());
    final NamedFunction count = NamedFunction.getInstance("count");
    final FhirPath result = count.invoke(countInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withColumn(DataTypes.StringType)
        .withColumn(DataTypes.LongType)
        .withRow("female", 2L)
        .withRow("male", 1L)
        .build();

    assertThat(result)
        .hasExpression("count()")
        .isSingular()
        .isElementPath(IntegerPath.class)
        .hasFhirType(FHIRDefinedType.UNSIGNEDINT)
        .selectGroupingResult(Collections.singletonList(groupingColumn))
        .hasRows(expectedDataset);

  }

  @Test
  public void inputMustNotContainArguments() {
    final ElementPath inputPath = new ElementPathBuilder(spark).build();
    final ElementPath argumentPath = new ElementPathBuilder(spark).build();
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();

    final NamedFunctionInput countInput = new NamedFunctionInput(parserContext, inputPath,
        Collections.singletonList(argumentPath));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> NamedFunction.getInstance("count").invoke(countInput));
    assertEquals("Arguments can not be passed to count function", error.getMessage());
  }
}