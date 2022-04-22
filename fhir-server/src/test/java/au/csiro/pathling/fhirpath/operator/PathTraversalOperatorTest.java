/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.MANY_EXT_ROW_1;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.MANY_EXT_ROW_2;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.ONE_MY_EXTENSION;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.nullEntryMap;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.oneEntryMap;
import static au.csiro.pathling.test.fixtures.ExtensionFixture.toElementDataset;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
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
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
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
        .database(database)
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
        .database(database)
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
  void testExtensionTraversalOnResources() {
    final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withColumn("gender", DataTypes.StringType)
        .withColumn("active", DataTypes.BooleanType)
        .withFidColumn()
        .withExtensionColumn()
        .withRow("patient-1", "female", true, 1,
            ImmutableMap.builder().put(1, Arrays.asList(MANY_EXT_ROW_1, MANY_EXT_ROW_2)).build())
        .withRow("patient-2", "female", false, 1,
            ImmutableMap.builder().put(1, Collections.singletonList(MANY_EXT_ROW_1))
                .put(2, Collections.singletonList(MANY_EXT_ROW_2)).build())
        .withRow("patient-3", "male", false, 1, oneEntryMap(2, ONE_MY_EXTENSION))
        .withRow("patient-4", "male", false, 1, nullEntryMap(1))
        .withRow("patient-5", "male", true, 1, null)
        .build();
    when(database.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);
    final ResourcePath left = ResourcePath
        .build(fhirContext, database, ResourceType.PATIENT, "Patient", false);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "extension");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    assertThat(result)
        .hasExpression("Patient.extension")
        .isNotSingular()
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.EXTENSION)
        .selectOrderedResult()
        .hasRows(
            // Many many extensions for this resource _fix
            RowFactory.create("patient-1", MANY_EXT_ROW_1),
            RowFactory.create("patient-1", MANY_EXT_ROW_2),
            // One extension with matching _fid and some with other fids
            RowFactory.create("patient-2", MANY_EXT_ROW_1),
            // Not extensions with matching _fid
            RowFactory.create("patient-3", null),
            // Empty list of extension for matching _fix
            RowFactory.create("patient-4", null),
            // No extensions present all together (empty extension map)
            RowFactory.create("patient-5", null)
        );
  }


  @Test
  void testExtensionTraversalOnElements() {
    final Map<Object, Object> manyToFidZeroMap = ImmutableMap.builder()
        .put(0, Arrays.asList(MANY_EXT_ROW_1, MANY_EXT_ROW_2)).build();

    final ImmutableMap<Object, Object> onePerFidZeroAndOneMap = ImmutableMap.builder()
        .put(0, Collections.singletonList(MANY_EXT_ROW_1))
        .put(1, Collections.singletonList(MANY_EXT_ROW_2)).build();

    // Construct element dataset from the resource dataset so that the resource path can be used as 
    // the current resource for this element path.
    // Note: this resource path is not singular as this will be a base for elements.

    final Dataset<Row> resourceLikeDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructColumn("name", DataTypes.StringType)
        .withStructColumn("_fid", DataTypes.IntegerType)
        .withStructValueColumn()
        .withExtensionColumn()
        .withRow("observation-1", makeEid(0), RowFactory.create("name1", 0), manyToFidZeroMap)
        .withRow("observation-2", makeEid(0), RowFactory.create("name2-1", 0),
            onePerFidZeroAndOneMap)
        .withRow("observation-2", makeEid(1), RowFactory.create("name2-2", 1),
            onePerFidZeroAndOneMap)
        .withRow("observation-3", makeEid(0), RowFactory.create("name3", 0),
            oneEntryMap(2, ONE_MY_EXTENSION))
        .withRow("observation-4", makeEid(0), RowFactory.create("name4", 0), nullEntryMap(1))
        .withRow("observation-5", makeEid(0), RowFactory.create("name5", 0), null)
        .build();

    when(database.read(ResourceType.OBSERVATION))
        .thenReturn(resourceLikeDataset);
    final ResourcePath baseResourcePath = ResourcePath
        .build(fhirContext, database, ResourceType.OBSERVATION, "Observation", false);

    final Dataset<Row> elementDataset = toElementDataset(resourceLikeDataset, baseResourcePath);

    final ElementDefinition codeDefinition = checkPresent(FhirHelpers
        .getChildOfResource(fhirContext, "Observation", "code"));

    final ElementPath left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .definition(codeDefinition)
        .dataset(elementDataset)
        .idAndEidAndValueColumns()
        .expression("code")
        .singular(false)
        .currentResource(baseResourcePath)
        .buildDefined();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final PathTraversalInput input = new PathTraversalInput(parserContext, left, "extension");
    final FhirPath result = new PathTraversalOperator().invoke(input);

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(DatasetBuilder.SIMPLE_EXTENSION_TYPE)
        .withRow("observation-1", makeEid(0, 0), MANY_EXT_ROW_1)
        .withRow("observation-1", makeEid(0, 1), MANY_EXT_ROW_2)
        .withRow("observation-2", makeEid(0, 0), MANY_EXT_ROW_1)
        .withRow("observation-2", makeEid(1, 0), MANY_EXT_ROW_2)
        .withRow("observation-3", makeEid(0, 0), null)
        .withRow("observation-4", makeEid(0, 0), null)
        .withRow("observation-5", makeEid(0, 0), null)
        .buildWithStructValue();

    assertThat(result)
        .hasExpression("code.extension")
        .isNotSingular()
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.EXTENSION)
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);

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
