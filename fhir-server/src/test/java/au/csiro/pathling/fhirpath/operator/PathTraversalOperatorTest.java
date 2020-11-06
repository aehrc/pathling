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
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import java.util.Arrays;
import java.util.Optional;
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
public class PathTraversalOperatorTest {

  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  @Test
  public void singularTraversalFromSingular() {
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
        .withRow("Patient/abc1", null, "female")
        .withRow("Patient/abc2", null, null)
        .build();
    assertThat(result)
        .isElementPath(StringPath.class)
        .isSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void manyTraversalFromSingular() {
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
        .withRow("Patient/abc1", makeEid(0), null)
        .withRow("Patient/abc1", makeEid(1), "Marie")
        .withRow("Patient/abc1", makeEid(2), null)
        .withRow("Patient/abc1", makeEid(3), "Anne")
        .withRow("Patient/abc2", null, null)
        .withRow("Patient/abc3", null, null)
        .build();
    assertThat(result)
        .isElementPath(ElementPath.class)
        .hasExpression("Patient.name")
        .hasFhirType(FHIRDefinedType.HUMANNAME)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void manyTraversalFromNonSingular() {
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("given", DataTypes.createArrayType(DataTypes.StringType))
        // patient with two names
        .withRow("Patient/abc1", makeEid(1), RowFactory.create(Arrays.asList("Jude", "Adam")))
        .withRow("Patient/abc1", makeEid(0), RowFactory.create(Arrays.asList("Mark", "Alen", null)))
        // patient with empty list of given names
        .withRow("Patient/abc2", makeEid(0), RowFactory.create(Arrays.asList()))
        // no name in the first place
        .withRow("Patient/abc5", null, null)
        .buildWithStructValue();

    final Optional<ElementDefinition> definition = FhirHelpers
        .getChildOfResource("Patient", "name");

    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .eidColumn()
        .expression("Patient.name")
        .definition(definition.get())
        .buildDefined();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "given");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", makeEid(0, 0), "Mark")
        .withRow("Patient/abc1", makeEid(0, 1), "Alen")
        .withRow("Patient/abc1", makeEid(0, 2), null)
        .withRow("Patient/abc1", makeEid(1, 0), "Jude")
        .withRow("Patient/abc1", makeEid(1, 1), "Adam")
        .withRow("Patient/abc2", null, null)
        .withRow("Patient/abc5", null, null)
        .build();

    assertThat(result)
        .isElementPath(ElementPath.class)
        .hasExpression("Patient.name.given")
        .hasFhirType(FHIRDefinedType.STRING)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void singularTraversalFromNonSingular() {
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("family", DataTypes.StringType)
        // patient with two names
        .withRow("Patient/abc1", makeEid(1), RowFactory.create("Jude"))
        .withRow("Patient/abc1", makeEid(0), RowFactory.create("Mark"))
        // patient with some null values
        .withRow("Patient/abc2", makeEid(1), RowFactory.create("Adam"))
        .withRow("Patient/abc2", makeEid(0), RowFactory.create((String) null))
        // patient with empty list of given names
        .withRow("Patient/abc5", null, null)
        .buildWithStructValue();

    final Optional<ElementDefinition> definition = FhirHelpers
        .getChildOfResource("Patient", "name");

    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .eidColumn()
        .expression("Patient.name")
        .definition(definition.get())
        .buildDefined();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "family");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", makeEid(0), "Mark")
        .withRow("Patient/abc1", makeEid(1), "Jude")
        .withRow("Patient/abc2", makeEid(0), null)
        .withRow("Patient/abc2", makeEid(1), "Adam")
        .withRow("Patient/abc5", null, null)
        .build();

    assertThat(result)
        .isElementPath(ElementPath.class)
        .hasExpression("Patient.name.family")
        .hasFhirType(FHIRDefinedType.STRING)
        .isNotSingular()
        .selectOrderedResultWithEid()
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
