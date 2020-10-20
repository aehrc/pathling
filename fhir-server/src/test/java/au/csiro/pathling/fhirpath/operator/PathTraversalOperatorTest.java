/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
public class PathTraversalOperatorTest {

  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  @Test
  public void simpleTraversal() {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", "female", true)
        .withRow("Patient/abc2", null, null)
        .build();
    final ResourceReader resourceReader = mock(ResourceReader.class);
    when(resourceReader.read(ResourceType.PATIENT)).thenReturn(leftDataset);
    final ResourcePath left = new ResourcePathBuilder()
        .fhirContext(FhirHelpers.getFhirContext())
        .resourceType(ResourceType.PATIENT)
        .resourceReader(resourceReader)
        .singular(true)
        .build();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "gender");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", makeEid(0, 0), "female")
        .withRow("Patient/abc2", null, null)
        .build();
    assertThat(result)
        .isElementPath(StringPath.class)
        .isSingular()
        .selectResultPreserveOrder()
        .hasRows(expectedDataset);
  }

  @Test
  public void manyTraversal() {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn("name", DataTypes.createArrayType(DataTypes.StringType))
        .withColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", Arrays.asList(null, "Marie", null, "Anne"), true)
        .withRow("Patient/abc2", Arrays.asList(), true)
        .withRow("Patient/abc3", null, true)
        .build();
    final ResourceReader resourceReader = mock(ResourceReader.class);
    when(resourceReader.read(ResourceType.PATIENT)).thenReturn(leftDataset);
    final ResourcePath left = new ResourcePathBuilder()
        .fhirContext(FhirHelpers.getFhirContext())
        .resourceType(ResourceType.PATIENT)
        .resourceReader(resourceReader)
        .singular(true)
        .build();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "name");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", makeEid(0, 0), null)
        .withRow("Patient/abc1", makeEid(0, 1), "Marie")
        .withRow("Patient/abc1", makeEid(0, 2), null)
        .withRow("Patient/abc1", makeEid(0, 3), "Anne")
        .withRow("Patient/abc2", null, null)
        .withRow("Patient/abc3", null, null)
        .build();
    assertThat(result)
        .isElementPath(ElementPath.class)
        .hasExpression("Patient.name")
        .hasFhirType(FHIRDefinedType.HUMANNAME)
        .isNotSingular()
        .selectResultPreserveOrder()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorOnNonExistentChild() {
    final ResourcePath left = new ResourcePathBuilder()
        .fhirContext(FhirHelpers.getFhirContext())
        .resourceType(ResourceType.ENCOUNTER)
        .expression("Encounter")
        .build();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "reason");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> new PathTraversalOperator().invoke(input));
    assertEquals("No such child: Encounter.reason",
        error.getMessage());
  }
}
