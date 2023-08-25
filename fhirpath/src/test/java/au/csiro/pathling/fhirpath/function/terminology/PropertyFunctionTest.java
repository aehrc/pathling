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

package au.csiro.pathling.fhirpath.function.terminology;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.SpringBootUnitTest;

/**
 * @author Piotr Szul
 */
@SpringBootUnitTest
@NotImplemented
class PropertyFunctionTest extends AbstractTerminologyTestBase {

  // TODO: implement with columns

  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Autowiredø
  // TerminologyServiceFactory terminologyServiceFactory;
  //
  // @Autowired
  // TerminologyService terminologyService;
  //
  // @BeforeEach
  // void setUp() {
  //   reset(terminologyService);
  // }
  //
  // private void checkPropertyOfCoding(final String propertyCode,
  //     final Optional<String> maybePropertyType,
  //     final Optional<String> maybeLanguage,
  //     final Type[] propertyAFhirValues, final Type[] propertyBFhirValues,
  //     final FHIRDefinedType expectedResultType,
  //     final Dataset<Row> expectedResult) {
  //
  //   // check the that if type is provided if language is provided
  //   assertTrue(maybeLanguage.isEmpty() || maybePropertyType.isPresent());
  //
  //   TerminologyServiceHelpers.setupLookup(terminologyService)
  //       .withProperty(CODING_A, "propertyA", maybeLanguage.orElse(null), propertyAFhirValues)
  //       .withProperty(CODING_B, "propertyB", maybeLanguage.orElse(null), propertyBFhirValues);
  //
  //   final Optional<ElementDefinition> optionalDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Encounter", "class");
  //   assertTrue(optionalDefinition.isPresent());
  //   final ElementDefinition definition = optionalDefinition.get();
  //
  //   final Dataset<Row> inputDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withStructTypeColumns(codingStructType())
  //       .withRow("encounter-1", makeEid(0), rowFromCoding(CODING_A))
  //       .withRow("encounter-1", makeEid(1), rowFromCoding(INVALID_CODING_0))
  //       .withRow("encounter-2", makeEid(0), rowFromCoding(CODING_B))
  //       .withRow("encounter-3", null, null)
  //       .buildWithStructValue();
  //
  //   final CodingCollection inputExpression = (CodingCollection) new ElementPathBuilder(spark)
  //       .dataset(inputDataset)
  //       .idAndEidAndValueColumns()
  //       .expression("Encounter.class")
  //       .singular(false)
  //       .definition(definition)
  //       .buildDefined();
  //
  //   // Prepare the inputs to the function.
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .idColumn(inputExpression.getIdColumn())
  //       .terminologyClientFactory(terminologyServiceFactory)
  //       .build();
  //
  //   final List<String> parameterLiterals = Stream.of(Optional.of(propertyCode),
  //           maybePropertyType,
  //           maybeLanguage).flatMap(Optional::stream)
  //       .map(StringLiteral::toLiteral).collect(Collectors.toUnmodifiableList());
  //
  //   final List<Collection> arguments = parameterLiterals.stream()
  //       .map(lit -> StringCollection.fromLiteral(lit, inputExpression))
  //       .collect(Collectors.toUnmodifiableList());
  //
  //   final NamedFunctionInput propertyInput = new NamedFunctionInput(parserContext, inputExpression,
  //       arguments);
  //
  //   // Invoke the function.
  //   final Collection result = NamedFunction.getInstance("property").invoke(propertyInput);
  //
  //   // Check the result.
  //   assertThat(result).hasExpression(
  //           String.format("Encounter.class.property(%s)",
  //               String.join(", ", parameterLiterals)))
  //       .isElementPath(PrimitivePath.class)
  //       .hasFhirType(expectedResultType)
  //       .isNotSingular()
  //       .selectOrderedResultWithEid()
  //       .hasRows(expectedResult);
  //
  //   // Check that the ElementPath for CODING type has the correct Coding definition.
  //   if (FHIRDefinedType.CODING.equals(expectedResultType)) {
  //     assertThat(result)
  //         .isElementPath(PrimitivePath.class)
  //         .hasDefinition(definition);
  //   }
  // }
  //
  // @SuppressWarnings("unused")
  // @ParameterizedTest
  // @MethodSource("propertyParameters")
  // public void propertyAOfCoding(final String propertyType, final DataType resultDataType,
  //     final Type[] propertyAFhirValues, final Type[] propertyBFhirValues,
  //     final Object[] propertyASqlValues, final Object[] propertyBSqlValues) {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(resultDataType)
  //       .withRow("encounter-1", makeEid(0, 0), propertyASqlValues[0])
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), null)
  //       .withRow("encounter-3", null, null)
  //       .build();
  //   checkPropertyOfCoding("propertyA",
  //       Optional.of(propertyType),
  //       Optional.empty(),
  //       propertyAFhirValues, propertyBFhirValues,
  //       FHIRDefinedType.fromCode(propertyType),
  //       expectedResult);
  // }
  //
  // @SuppressWarnings("unused")
  // @ParameterizedTest
  // @MethodSource("propertyParameters")
  // public void propertyBOfCoding(final String propertyType, final DataType resultDataType,
  //     final Type[] propertyAFhirValues, final Type[] propertyBFhirValues,
  //     final Object[] propertyASqlValues, final Object[] propertyBSqlValues) {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(resultDataType)
  //       .withRow("encounter-1", makeEid(0, 0), null)
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), propertyBSqlValues[0])
  //       .withRow("encounter-2", makeEid(0, 1), propertyBSqlValues[1])
  //       .withRow("encounter-3", null, null)
  //       .build();
  //
  //   checkPropertyOfCoding("propertyB",
  //       Optional.of(propertyType),
  //       Optional.empty(),
  //       propertyAFhirValues,
  //       propertyBFhirValues,
  //       FHIRDefinedType.fromCode(propertyType),
  //       expectedResult);
  // }
  //
  // @Test
  // public void propertyAOfCodingWithDefaultType() {
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", makeEid(0, 0), "value_1")
  //       .withRow("encounter-1", makeEid(0, 1), "value_2")
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), null)
  //       .withRow("encounter-3", null, null)
  //       .build();
  //
  //   checkPropertyOfCoding("propertyA",
  //       Optional.empty(),
  //       Optional.empty(),
  //       new Type[]{new StringType("value_1"), new StringType("value_2")}, new Type[]{},
  //       FHIRDefinedType.STRING,
  //       expectedResult);
  // }
  //
  //
  // @SuppressWarnings("unused")
  // @ParameterizedTest
  // @MethodSource("propertyParameters")
  // public void propertyBOfCodingLanguage(final String propertyType, final DataType resultDataType,
  //     final Type[] propertyAFhirValues, final Type[] propertyBFhirValues,
  //     final Object[] propertyASqlValues, final Object[] propertyBSqlValues) {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(resultDataType)
  //       .withRow("encounter-1", makeEid(0, 0), null)
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), propertyBSqlValues[0])
  //       .withRow("encounter-2", makeEid(0, 1), propertyBSqlValues[1])
  //       .withRow("encounter-3", null, null)
  //       .build();
  //
  //   checkPropertyOfCoding("propertyB",
  //       Optional.of(propertyType),
  //       Optional.of("de"),
  //       propertyAFhirValues,
  //       propertyBFhirValues,
  //       FHIRDefinedType.fromCode(propertyType),
  //       expectedResult);
  // }
  //
  // @Test
  // void throwsErrorIfInputTypeIsUnsupported() {
  //   final Collection mockContext = new ElementPathBuilder(spark).build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .expression("name.given")
  //       .build();
  //   final Collection argument = StringCollection.fromLiteral("some-property", mockContext);
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(mock(TerminologyServiceFactory.class))
  //       .build();
  //
  //   final NamedFunctionInput propertyInput = new NamedFunctionInput(parserContext, input,
  //       Collections.singletonList(argument));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new PropertyFunction().invoke(propertyInput));
  //   assertEquals("Input to property function must be Coding but is: name.given",
  //       error.getMessage());
  // }
  //
  // void assertThrowsErrorForArguments(@Nonnull final String expectedError,
  //     @Nonnull final Function<PrimitivePath, List<Collection>> argsFactory) {
  //
  //   final Optional<ElementDefinition> optionalDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Encounter", "class");
  //   assertTrue(optionalDefinition.isPresent());
  //   final ElementDefinition definition = optionalDefinition.get();
  //
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .definition(definition)
  //       .buildDefined();
  //
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(mock(TerminologyServiceFactory.class))
  //       .build();
  //
  //   final NamedFunctionInput propertyInput = new NamedFunctionInput(context, input,
  //       argsFactory.apply(input));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new PropertyFunction().invoke(propertyInput));
  //   assertEquals(expectedError,
  //       error.getMessage());
  //
  // }
  //
  // @Test
  // void throwsErrorIfNoArguments() {
  //   assertThrowsErrorForArguments(
  //       "property function accepts one required and one optional arguments",
  //       input -> Collections.emptyList());
  // }
  //
  // @Test
  // void throwsErrorIfFirstArgumentIsNotString() {
  //   assertThrowsErrorForArguments("Function `property` expects `String literal` as argument 1",
  //       input -> Collections.singletonList(
  //           IntegerLiteralPath.fromString("4", input)));
  // }
  //
  // @Test
  // void throwsErrorIfSecondArgumentIsNotBoolean() {
  //   assertThrowsErrorForArguments("Function `property` expects `String literal` as argument 2",
  //       input -> Arrays.asList(
  //           StringCollection.fromLiteral("'foo'", input),
  //           IntegerLiteralPath.fromString("5", input)));
  // }
  //
  // @Test
  // void throwsErrorIfThirdArgumentIsNotBoolean() {
  //   assertThrowsErrorForArguments("Function `property` expects `String literal` as argument 3",
  //       input -> Arrays.asList(
  //           StringCollection.fromLiteral("'foo'", input),
  //           StringCollection.fromLiteral("'foo'", input),
  //           IntegerLiteralPath.fromString("5", input)));
  // }
  //
  // @Test
  // void throwsErrorIfTooManyArguments() {
  //   assertThrowsErrorForArguments(
  //       "property function accepts one required and one optional arguments",
  //       input -> Arrays.asList(
  //           StringCollection.fromLiteral("'foo'", input),
  //           StringCollection.fromLiteral("'false'", input),
  //           StringCollection.fromLiteral("'false'", input),
  //           StringCollection.fromLiteral("'false'", input)
  //       ));
  // }
  //
  // @Test
  // void throwsErrorIfCannotParsePropertyType() {
  //   assertThrowsErrorForArguments(
  //       "Unknown FHIRDefinedType code 'not-an-fhir-type'",
  //       input -> Arrays.asList(
  //           StringCollection.fromLiteral("'foo'", input),
  //           StringCollection.fromLiteral("'not-an-fhir-type'", input)
  //       ));
  // }
  //
  // @Test
  // void throwsErrorIfTerminologyServiceNotConfigured() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .build();
  //   final Collection argument = StringCollection.fromLiteral("some string", input);
  //
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .build();
  //
  //   final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
  //       Collections.singletonList(argument));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new TranslateFunction().invoke(translateInput));
  //   assertEquals(
  //       "Attempt to call terminology function translate when terminology service has not been configured",
  //       error.getMessage());
  // }
}
