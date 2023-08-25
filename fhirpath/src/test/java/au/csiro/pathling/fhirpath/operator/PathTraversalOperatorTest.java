/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.SpringBootUnitTest;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@NotImplemented
class PathTraversalOperatorTest {

  // TODO: implement with columns

  //  
  //   @Autowired
  //   SparkSession spark;
  //
  //   @Autowired
  //   FhirContext fhirContext;
  //
  //   @MockBean
  //   DataSource dataSource;
  //
  //   ParserContext parserContext;
  //
  //   @BeforeEach
  //   void setUp() {
  //     parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //   }
  //
  //   @Test
  //   void singularTraversalFromSingular() {
  //     final Dataset<Row> leftDataset = new ResourceDatasetBuilder(spark)
  //         .withIdColumn()
  //         .withColumn("gender", DataTypes.StringType)
  //         .withRow("patient-1", "female")
  //         .withRow("patient-2", null)
  //         .build();
  //     when(dataSource.read(ResourceType.PATIENT)).thenReturn(leftDataset);
  //     final ResourceCollection left = new ResourcePathBuilder(spark)
  //         .fhirContext(fhirContext)
  //         .resourceType(ResourceType.PATIENT)
  //         .database(dataSource)
  //         .singular(true)
  //         .build();
  //
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "gender");
  //     final Collection result = new PathTraversalOperator().invoke(input);
  //
  //     final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withColumn(DataTypes.StringType)
  //         .withRow("patient-1", null, "female")
  //         .withRow("patient-2", null, null)
  //         .build();
  //     assertThat(result)
  //         .isElementPath(StringCollection.class)
  //         .isSingular()
  //         .selectOrderedResultWithEid()
  //         .hasRows(expectedDataset);
  //   }
  //
  //   @Test
  //   void manyTraversalFromSingular() {
  //     final Dataset<Row> leftDataset = new ResourceDatasetBuilder(spark)
  //         .withIdColumn()
  //         .withColumn("name", DataTypes.createArrayType(DataTypes.StringType))
  //         .withColumn("active", DataTypes.BooleanType)
  //         .withRow("patient-1", Arrays.asList(null, "Marie", null, "Anne"), true)
  //         .withRow("patient-2", Collections.emptyList(), true)
  //         .withRow("patient-3", null, true)
  //         .build();
  //     when(dataSource.read(ResourceType.PATIENT)).thenReturn(leftDataset);
  //     final ResourceCollection left = new ResourcePathBuilder(spark)
  //         .fhirContext(fhirContext)
  //         .resourceType(ResourceType.PATIENT)
  //         .database(dataSource)
  //         .singular(true)
  //         .build();
  //
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "name");
  //     final Collection result = new PathTraversalOperator().invoke(input);
  //
  //     final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withColumn(DataTypes.StringType)
  //         .withRow("patient-1", makeEid(0), null)
  //         .withRow("patient-1", makeEid(1), "Marie")
  //         .withRow("patient-1", makeEid(2), null)
  //         .withRow("patient-1", makeEid(3), "Anne")
  //         .withRow("patient-2", null, null)
  //         .withRow("patient-3", null, null)
  //         .build();
  //     assertThat(result)
  //         .isElementPath(PrimitivePath.class)
  //         .hasExpression("Patient.name")
  //         .hasFhirType(FHIRDefinedType.HUMANNAME)
  //         .isNotSingular()
  //         .selectOrderedResultWithEid()
  //         .hasRows(expectedDataset);
  //   }
  //
  //   @Test
  //   void manyTraversalFromNonSingular() {
  //     final Dataset<Row> inputDataset = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withStructColumn("given", DataTypes.createArrayType(DataTypes.StringType))
  //         // patient with two names
  //         .withRow("patient-1", makeEid(1), RowFactory.create(Arrays.asList("Jude", "Adam")))
  //         .withRow("patient-1", makeEid(0), RowFactory.create(Arrays.asList("Mark", "Alen", null)))
  //         // patient with empty list of given names and null list
  //         .withRow("patient-2", makeEid(1), RowFactory.create(Collections.emptyList()))
  //         .withRow("patient-2", makeEid(0), null)
  //         // no name in the first place
  //         .withRow("patient-5", null, null)
  //         .buildWithStructValue();
  //
  //     final Optional<ElementDefinition> definition = FhirHelpers
  //         .getChildOfResource(fhirContext, "Patient", "name");
  //
  //     assertTrue(definition.isPresent());
  //     final PrimitivePath left = new ElementPathBuilder(spark)
  //         .fhirType(FHIRDefinedType.STRING)
  //         .dataset(inputDataset)
  //         .idAndEidAndValueColumns()
  //         .expression("Patient.name")
  //         .definition(definition.get())
  //         .buildDefined();
  //
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "given");
  //     final Collection result = new PathTraversalOperator().invoke(input);
  //
  //     final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withColumn(DataTypes.StringType)
  //         .withRow("patient-1", makeEid(0, 0), "Mark")
  //         .withRow("patient-1", makeEid(0, 1), "Alen")
  //         .withRow("patient-1", makeEid(0, 2), null)
  //         .withRow("patient-1", makeEid(1, 0), "Jude")
  //         .withRow("patient-1", makeEid(1, 1), "Adam")
  //         .withRow("patient-2", makeEid(0, 0), null)
  //         .withRow("patient-2", makeEid(1, 0), null)
  //         .withRow("patient-5", null, null)
  //         .build();
  //
  //     assertThat(result)
  //         .isElementPath(PrimitivePath.class)
  //         .hasExpression("Patient.name.given")
  //         .hasFhirType(FHIRDefinedType.STRING)
  //         .isNotSingular()
  //         .selectOrderedResultWithEid()
  //         .hasRows(expectedDataset);
  //   }
  //
  //   @Test
  //   void singularTraversalFromNonSingular() {
  //     final Dataset<Row> inputDataset = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withStructColumn("family", DataTypes.StringType)
  //         // patient with two names
  //         .withRow("patient-1", makeEid(1), RowFactory.create("Jude"))
  //         .withRow("patient-1", makeEid(0), RowFactory.create("Mark"))
  //         // patient with some null values
  //         .withRow("patient-2", makeEid(1), RowFactory.create("Adam"))
  //         .withRow("patient-2", makeEid(0), RowFactory.create((String) null))
  //         // patient with empty list of given names
  //         .withRow("patient-5", null, null)
  //         .buildWithStructValue();
  //
  //     final Optional<ElementDefinition> definition = FhirHelpers
  //         .getChildOfResource(fhirContext, "Patient", "name");
  //
  //     assertTrue(definition.isPresent());
  //     final PrimitivePath left = new ElementPathBuilder(spark)
  //         .fhirType(FHIRDefinedType.STRING)
  //         .dataset(inputDataset)
  //         .idAndEidAndValueColumns()
  //         .expression("Patient.name")
  //         .definition(definition.get())
  //         .buildDefined();
  //
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "family");
  //     final Collection result = new PathTraversalOperator().invoke(input);
  //
  //     final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withColumn(DataTypes.StringType)
  //         .withRow("patient-1", makeEid(0), "Mark")
  //         .withRow("patient-1", makeEid(1), "Jude")
  //         .withRow("patient-2", makeEid(0), null)
  //         .withRow("patient-2", makeEid(1), "Adam")
  //         .withRow("patient-5", null, null)
  //         .build();
  //
  //     assertThat(result)
  //         .isElementPath(PrimitivePath.class)
  //         .hasExpression("Patient.name.family")
  //         .hasFhirType(FHIRDefinedType.STRING)
  //         .isNotSingular()
  //         .selectOrderedResultWithEid()
  //         .hasRows(expectedDataset);
  //   }
  //
  //   @Test
  //   void testExtensionTraversalOnResources() {
  //     final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
  //         .withIdColumn()
  //         .withColumn("gender", DataTypes.StringType)
  //         .withColumn("active", DataTypes.BooleanType)
  //         .withFidColumn()
  //         .withExtensionColumn()
  //         .withRow("patient-1", "female", true, 1,
  //             ImmutableMap.builder().put(1, Arrays.asList(MANY_EXT_ROW_1, MANY_EXT_ROW_2)).build())
  //         .withRow("patient-2", "female", false, 1,
  //             ImmutableMap.builder().put(1, Collections.singletonList(MANY_EXT_ROW_1))
  //                 .put(2, Collections.singletonList(MANY_EXT_ROW_2)).build())
  //         .withRow("patient-3", "male", false, 1, oneEntryMap(2, ONE_MY_EXTENSION))
  //         .withRow("patient-4", "male", false, 1, nullEntryMap(1))
  //         .withRow("patient-5", "male", true, 1, null)
  //         .build();
  //     when(dataSource.read(ResourceType.PATIENT))
  //         .thenReturn(patientDataset);
  //     final ResourceCollection left = ResourceCollection
  //         .build(fhirContext, dataSource, ResourceType.PATIENT, "Patient");
  //
  //     final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "extension");
  //     final Collection result = new PathTraversalOperator().invoke(input);
  //
  //     assertThat(result)
  //         .hasExpression("Patient.extension")
  //         .isNotSingular()
  //         .isElementPath(PrimitivePath.class)
  //         .hasFhirType(FHIRDefinedType.EXTENSION)
  //         .selectOrderedResult()
  //         .hasRows(
  //             // Many many extensions for this resource _fix
  //             RowFactory.create("patient-1", MANY_EXT_ROW_1),
  //             RowFactory.create("patient-1", MANY_EXT_ROW_2),
  //             // One extension with matching _fid and some with other fids
  //             RowFactory.create("patient-2", MANY_EXT_ROW_1),
  //             // Not extensions with matching _fid
  //             RowFactory.create("patient-3", null),
  //             // Empty list of extension for matching _fix
  //             RowFactory.create("patient-4", null),
  //             // No extensions present all together (empty extension map)
  //             RowFactory.create("patient-5", null)
  //         );
  //   }
  //
  //
  //   @Test
  //   void testExtensionTraversalOnElements() {
  //     final Map<Object, Object> manyToFidZeroMap = ImmutableMap.builder()
  //         .put(0, Arrays.asList(MANY_EXT_ROW_1, MANY_EXT_ROW_2)).build();
  //
  //     final ImmutableMap<Object, Object> onePerFidZeroAndOneMap = ImmutableMap.builder()
  //         .put(0, Collections.singletonList(MANY_EXT_ROW_1))
  //         .put(1, Collections.singletonList(MANY_EXT_ROW_2)).build();
  //
  //     // Construct element dataset from the resource dataset so that the resource path can be used as 
  //     // the current resource for this element path.
  //     // Note: this resource path is not singular as this will be a base for elements.
  //
  //     final Dataset<Row> resourceLikeDataset = new ResourceDatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withStructColumn("name", DataTypes.StringType)
  //         .withStructColumn("_fid", DataTypes.IntegerType)
  //         .withStructValueColumn()
  //         .withExtensionColumn()
  //         .withRow("observation-1", makeEid(0), RowFactory.create("name1", 0), manyToFidZeroMap)
  //         .withRow("observation-2", makeEid(0), RowFactory.create("name2-1", 0),
  //             onePerFidZeroAndOneMap)
  //         .withRow("observation-2", makeEid(1), RowFactory.create("name2-2", 1),
  //             onePerFidZeroAndOneMap)
  //         .withRow("observation-3", makeEid(0), RowFactory.create("name3", 0),
  //             oneEntryMap(2, ONE_MY_EXTENSION))
  //         .withRow("observation-4", makeEid(0), RowFactory.create("name4", 0), nullEntryMap(1))
  //         .withRow("observation-5", makeEid(0), RowFactory.create("name5", 0), null)
  //         .build();
  //
  //     when(dataSource.read(ResourceType.OBSERVATION))
  //         .thenReturn(resourceLikeDataset);
  //     final ResourceCollection baseResourceCollection = ResourceCollection
  //         .build(fhirContext, dataSource, ResourceType.OBSERVATION, "Observation");
  //
  //     final Dataset<Row> elementDataset = toElementDataset(resourceLikeDataset,
  //         baseResourceCollection);
  //
  //     final ElementDefinition codeDefinition = checkPresent(FhirHelpers
  //         .getChildOfResource(fhirContext, "Observation", "code"));
  //
  //     final PrimitivePath left = new ElementPathBuilder(spark)
  //         .fhirType(FHIRDefinedType.CODEABLECONCEPT)
  //         .definition(codeDefinition)
  //         .dataset(elementDataset)
  //         .idAndEidAndValueColumns()
  //         .expression("code")
  //         .singular(false)
  //         .currentResource(baseResourceCollection)
  //         .buildDefined();
  //
  //     final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "extension");
  //     final Collection result = new PathTraversalOperator().invoke(input);
  //
  //     final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //         .withIdColumn()
  //         .withEidColumn()
  //         .withStructTypeColumns(DatasetBuilder.SIMPLE_EXTENSION_TYPE)
  //         .withRow("observation-1", makeEid(0, 0), MANY_EXT_ROW_1)
  //         .withRow("observation-1", makeEid(0, 1), MANY_EXT_ROW_2)
  //         .withRow("observation-2", makeEid(0, 0), MANY_EXT_ROW_1)
  //         .withRow("observation-2", makeEid(1, 0), MANY_EXT_ROW_2)
  //         .withRow("observation-3", makeEid(0, 0), null)
  //         .withRow("observation-4", makeEid(0, 0), null)
  //         .withRow("observation-5", makeEid(0, 0), null)
  //         .buildWithStructValue();
  //
  //     assertThat(result)
  //         .hasExpression("code.extension")
  //         .isNotSingular()
  //         .isElementPath(PrimitivePath.class)
  //         .hasFhirType(FHIRDefinedType.EXTENSION)
  //         .selectOrderedResultWithEid()
  //         .hasRows(expectedResult);
  //
  //   }
  //
  //   @Test
  //   void throwsErrorOnNonExistentChild() {
  //     final ResourceCollection left = new ResourcePathBuilder(spark)
  //         .fhirContext(fhirContext)
  //         .resourceType(ResourceType.ENCOUNTER)
  //         .expression("Encounter")
  //         .build();
  //
  //     final PathTraversalInput input = new PathTraversalInput(parserContext, left, "reason");
  //     final InvalidUserInputError error = assertThrows(
  //         InvalidUserInputError.class,
  //         () -> new PathTraversalOperator().invoke(input));
  //     assertEquals("No such child: Encounter.reason",
  //         error.getMessage());
  //   }
}
