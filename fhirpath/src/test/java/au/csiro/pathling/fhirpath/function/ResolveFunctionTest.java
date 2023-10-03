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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.SpringBootUnitTest;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@NotImplemented
class ResolveFunctionTest {

  // TODO: implement with columns

  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Autowired
  // FhirEncoders fhirEncoders;
  //
  // @MockBean
  // DataSource dataSource;
  //
  // @Test
  // void simpleResolve() {
  //   final Optional<ElementDefinition> optionalDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Encounter", "episodeOfCare");
  //   assertTrue(optionalDefinition.isPresent());
  //   final ElementDefinition definition = optionalDefinition.get();
  //
  //   final Dataset<Row> referenceDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withStructTypeColumns(referenceStructType())
  //       .withRow("encounter-1", makeEid(0),
  //           RowFactory.create(null, "EpisodeOfCare/episodeofcare-1", null))
  //       .withRow("encounter-2", makeEid(0),
  //           RowFactory.create(null, "EpisodeOfCare/episodeofcare-3", null))
  //       .withRow("encounter-3", makeEid(0),
  //           RowFactory.create(null, "EpisodeOfCare/episodeofcare-2", null))
  //       .withRow("encounter-4", makeEid(0),
  //           RowFactory.create(null, "EpisodeOfCare/episodeofcare-2", null))
  //       .buildWithStructValue();
  //   final PrimitivePath referencePath = new ElementPathBuilder(spark)
  //       .expression("Encounter.episodeOfCare")
  //       .dataset(referenceDataset)
  //       .idAndEidAndValueColumns()
  //       .singular(false)
  //       .definition(definition)
  //       .buildDefined();
  //
  //   final Dataset<Row> episodeOfCareDataset = new ResourceDatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("episodeofcare-1", "planned")
  //       .withRow("episodeofcare-2", "waitlist")
  //       .withRow("episodeofcare-3", "active")
  //       .build();
  //   when(dataSource.read(ResourceType.EPISODEOFCARE)).thenReturn(episodeOfCareDataset);
  //
  //   final NamedFunctionInput resolveInput = buildFunctionInput(referencePath);
  //   final Collection result = invokeResolve(resolveInput);
  //
  //   assertTrue(result instanceof ResourceCollection);
  //   assertThat((ResourceCollection) result)
  //       .hasExpression("Encounter.episodeOfCare.resolve()")
  //       .isNotSingular()
  //       .hasResourceType(ResourceType.EPISODEOFCARE);
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", "episodeofcare-1")
  //       .withRow("encounter-2", "episodeofcare-3")
  //       .withRow("encounter-3", "episodeofcare-2")
  //       .withRow("encounter-4", "episodeofcare-2")
  //       .build();
  //   assertThat(result)
  //       .selectOrderedResult()
  //       .hasRows(expectedDataset);
  // }
  //
  // @Test
  // void polymorphicResolve() {
  //   final Optional<ElementDefinition> optionalDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Encounter", "subject");
  //   assertTrue(optionalDefinition.isPresent());
  //   final ElementDefinition definition = optionalDefinition.get();
  //
  //   final Dataset<Row> referenceDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withStructTypeColumns(referenceStructType())
  //       .withRow("encounter-1", RowFactory.create(null, "Patient/patient-1", null))
  //       .withRow("encounter-2", RowFactory.create(null, "Patient/patient-3", null))
  //       .withRow("encounter-3", RowFactory.create(null, "Patient/patient-2", null))
  //       .withRow("encounter-4", RowFactory.create(null, "Patient/patient-2", null))
  //       .withRow("encounter-5", RowFactory.create(null, "Group/group-1", null))
  //       .buildWithStructValue();
  //   final PrimitivePath referencePath = new ElementPathBuilder(spark)
  //       .expression("Encounter.subject")
  //       .dataset(referenceDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .definition(definition)
  //       .buildDefined();
  //
  //   final Dataset<Row> patientDataset = new ResourceDatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("patient-1", "female", true)
  //       .withRow("patient-2", "female", false)
  //       .withRow("patient-3", "male", true)
  //       .build();
  //   when(dataSource.read(ResourceType.PATIENT))
  //       .thenReturn(patientDataset);
  //
  //   final Dataset<Row> groupDataset = new ResourceDatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("group-1", "Some group", true)
  //       .build();
  //   when(dataSource.read(ResourceType.GROUP))
  //       .thenReturn(groupDataset);
  //
  //   final NamedFunctionInput resolveInput = buildFunctionInput(referencePath);
  //   final Collection result = invokeResolve(resolveInput);
  //
  //   assertTrue(result instanceof UntypedResourcePath);
  //   assertThat(result)
  //       .hasExpression("Encounter.subject.resolve()")
  //       .isSingular();
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withStructTypeColumns(referenceStructType())
  //       .withRow("encounter-1", RowFactory.create(null, "Patient/patient-1", null))
  //       .withRow("encounter-2", RowFactory.create(null, "Patient/patient-3", null))
  //       .withRow("encounter-3", RowFactory.create(null, "Patient/patient-2", null))
  //       .withRow("encounter-4", RowFactory.create(null, "Patient/patient-2", null))
  //       .withRow("encounter-5", RowFactory.create(null, "Group/group-1", null))
  //       .buildWithStructValue();
  //   assertThat(result)
  //       .selectResult()
  //       .hasRows(expectedDataset);
  // }
  //
  // @Test
  // void polymorphicResolveAnyType() {
  //   final Optional<ElementDefinition> optionalDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Condition", "evidence")
  //       .flatMap(child -> child.getChildElement("detail"));
  //   assertTrue(optionalDefinition.isPresent());
  //   final ElementDefinition definition = optionalDefinition.get();
  //   TestHelpers.mockAllEmptyResources(dataSource, spark, fhirEncoders);
  //
  //   final Dataset<Row> referenceDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withStructTypeColumns(referenceStructType())
  //       .withRow("condition-1", makeEid(0),
  //           RowFactory.create(null, "Observation/observation-1", null))
  //       .withRow("condition-2", makeEid(0),
  //           RowFactory.create(null, "ClinicalImpression/clinicalimpression-1", null))
  //       .buildWithStructValue();
  //   final PrimitivePath referencePath = new ElementPathBuilder(spark)
  //       .expression("Condition.evidence.detail")
  //       .dataset(referenceDataset)
  //       .idAndEidAndValueColumns()
  //       .singular(false)
  //       .definition(definition)
  //       .buildDefined();
  //
  //   final Dataset<Row> observationDataset = new ResourceDatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("observation-1", "registered")
  //       .build();
  //   when(dataSource.read(ResourceType.OBSERVATION))
  //       .thenReturn(observationDataset);
  //
  //   final Dataset<Row> clinicalImpressionDataset = new ResourceDatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("clinicalimpression-1", "in-progress")
  //       .build();
  //   when(dataSource.read(ResourceType.CLINICALIMPRESSION))
  //       .thenReturn(clinicalImpressionDataset);
  //
  //   final NamedFunctionInput resolveInput = buildFunctionInput(referencePath);
  //   final Collection result = invokeResolve(resolveInput);
  //
  //   assertTrue(result instanceof UntypedResourcePath);
  //   assertThat(result)
  //       .hasExpression("Condition.evidence.detail.resolve()")
  //       .isNotSingular();
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withStructTypeColumns(referenceStructType())
  //       .withRow("condition-1", RowFactory.create(null, "Observation/observation-1", null))
  //       .withRow("condition-2",
  //           RowFactory.create(null, "ClinicalImpression/clinicalimpression-1", null))
  //       .buildWithStructValue();
  //   assertThat(result)
  //       .selectResult()
  //       .hasRows(expectedDataset);
  // }
  //
  //
  // @Test
  // void throwExceptionWhenInputNotReference() {
  //   final Dataset<Row> patientDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.StringType)
  //       .build();
  //   final PrimitivePath genderPath = new ElementPathBuilder(spark)
  //       .expression("Patient.gender")
  //       .dataset(patientDataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .fhirType(FHIRDefinedType.CODE)
  //       .build();
  //
  //   final NamedFunctionInput resolveInput = buildFunctionInput(genderPath);
  //
  //   assertThrows(InvalidUserInputError.class, () -> invokeResolve(resolveInput),
  //       "Input to resolve function must be a Reference: gender");
  // }
  //
  // @Test
  // void throwExceptionWhenArgumentSupplied() {
  //   final PrimitivePath referencePath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.REFERENCE)
  //       .build();
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .build();
  //   final StringLiteralPath stringLiteralPath = StringCollection
  //       .fromLiteral("'foo'", parserContext.getInputContext());
  //   final NamedFunctionInput resolveInput = new NamedFunctionInput(parserContext, referencePath,
  //       Collections.singletonList(stringLiteralPath));
  //
  //   assertThrows(InvalidUserInputError.class, () -> invokeResolve(resolveInput),
  //       "resolve function does not accept arguments");
  // }
  //
  // @Nonnull
  // NamedFunctionInput buildFunctionInput(@Nonnull final NonLiteralPath inputPath) {
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .idColumn(inputPath.getIdColumn())
  //       .database(dataSource)
  //       .build();
  //   return new NamedFunctionInput(parserContext, inputPath, Collections.emptyList());
  // }
  //
  // @Nonnull
  // Collection invokeResolve(@Nonnull final NamedFunctionInput resolveInput) {
  //   final NamedFunction resolve = NamedFunction.getInstance("resolve");
  //   return resolve.invoke(resolveInput);
  // }
  //
}
