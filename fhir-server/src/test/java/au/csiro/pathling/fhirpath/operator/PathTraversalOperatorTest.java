/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
class PathTraversalOperatorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  ParserContext parserContext;
  Database database;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder(spark, fhirContext).build();
    database = mock(Database.class);
  }

  @Test
  void singularTraversalFromSingular() {
    final Dataset<Row> leftDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withRow("patient-1", "female")
        .withRow("patient-2", null)
        .build();
    when(database.read(ResourceType.PATIENT)).thenReturn(leftDataset);
    final ResourcePath left = new ResourcePathBuilder(spark)
        .fhirContext(fhirContext)
        .resourceType(ResourceType.PATIENT)
        .resourceReader(database)
        .singular(true)
        .build();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "gender");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", null, "female")
        .withRow("patient-2", null, null)
        .build();
    assertThat(result)
        .isElementPath(StringPath.class)
        .isSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  void manyTraversalFromSingular() {
    final Dataset<Row> leftDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("name", DataTypes.createArrayType(DataTypes.StringType))
        .withColumn("active", DataTypes.BooleanType)
        .withRow("patient-1", Arrays.asList(null, "Marie", null, "Anne"), true)
        .withRow("patient-2", Collections.emptyList(), true)
        .withRow("patient-3", null, true)
        .build();
    when(database.read(ResourceType.PATIENT)).thenReturn(leftDataset);
    final ResourcePath left = new ResourcePathBuilder(spark)
        .fhirContext(fhirContext)
        .resourceType(ResourceType.PATIENT)
        .resourceReader(database)
        .singular(true)
        .build();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "name");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0), null)
        .withRow("patient-1", makeEid(1), "Marie")
        .withRow("patient-1", makeEid(2), null)
        .withRow("patient-1", makeEid(3), "Anne")
        .withRow("patient-2", null, null)
        .withRow("patient-3", null, null)
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
  void manyTraversalFromNonSingular() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("given", DataTypes.createArrayType(DataTypes.StringType))
        // patient with two names
        .withRow("patient-1", makeEid(1), RowFactory.create(Arrays.asList("Jude", "Adam")))
        .withRow("patient-1", makeEid(0), RowFactory.create(Arrays.asList("Mark", "Alen", null)))
        // patient with empty list of given names and null list
        .withRow("patient-2", makeEid(1), RowFactory.create(Collections.emptyList()))
        .withRow("patient-2", makeEid(0), null)
        // no name in the first place
        .withRow("patient-5", null, null)
        .buildWithStructValue();

    final Optional<ElementDefinition> definition = FhirHelpers
        .getChildOfResource(fhirContext, "Patient", "name");

    assertTrue(definition.isPresent());
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Patient.name")
        .definition(definition.get())
        .buildDefined();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "given");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0, 0), "Mark")
        .withRow("patient-1", makeEid(0, 1), "Alen")
        .withRow("patient-1", makeEid(0, 2), null)
        .withRow("patient-1", makeEid(1, 0), "Jude")
        .withRow("patient-1", makeEid(1, 1), "Adam")
        .withRow("patient-2", makeEid(0, 0), null)
        .withRow("patient-2", makeEid(1, 0), null)
        .withRow("patient-5", null, null)
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
  void singularTraversalFromNonSingular() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("family", DataTypes.StringType)
        // patient with two names
        .withRow("patient-1", makeEid(1), RowFactory.create("Jude"))
        .withRow("patient-1", makeEid(0), RowFactory.create("Mark"))
        // patient with some null values
        .withRow("patient-2", makeEid(1), RowFactory.create("Adam"))
        .withRow("patient-2", makeEid(0), RowFactory.create((String) null))
        // patient with empty list of given names
        .withRow("patient-5", null, null)
        .buildWithStructValue();

    final Optional<ElementDefinition> definition = FhirHelpers
        .getChildOfResource(fhirContext, "Patient", "name");

    assertTrue(definition.isPresent());
    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Patient.name")
        .definition(definition.get())
        .buildDefined();

    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "family");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0), "Mark")
        .withRow("patient-1", makeEid(1), "Jude")
        .withRow("patient-2", makeEid(0), null)
        .withRow("patient-2", makeEid(1), "Adam")
        .withRow("patient-5", null, null)
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
  void throwsErrorOnNonExistentChild() {
    final ResourcePath left = new ResourcePathBuilder(spark)
        .fhirContext(fhirContext)
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
