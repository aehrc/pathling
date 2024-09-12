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
class DesignationFunctionTest extends AbstractTerminologyTestBase {

  // TODO: implement with columns
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Autowired
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
  //
  // private void checkDesignationsOfCoding(
  //     @Nonnull final Optional<Coding> maybeUse,
  //     @Nonnull final Optional<String> maybeLanguage,
  //     @Nonnull final Dataset<Row> expectedResult) {
  //
  //   assertTrue(maybeLanguage.isEmpty() || maybeUse.isPresent(),
  //       "'use' is required when 'language' is provided.");
  //
  //   TerminologyServiceHelpers.setupLookup(terminologyService)
  //       .withDesignation(CODING_A, CODING_C, "en", "A_C_en")
  //       .withDesignation(CODING_A, CODING_D, "en", "A_D_en")
  //       .withDesignation(CODING_A, null, null, "A_?_??")
  //       .withDesignation(CODING_B, CODING_D, "en", "B_D_en")
  //       .withDesignation(CODING_B, CODING_D, "fr", "B_D_fr.0", "B_D_fr.1")
  //       .done();
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
  //   final Optional<String> maybeUseLiteral = maybeUse.map(CodingLiteral::toLiteral);
  //   final Optional<String> maybeLanguageLiteral = maybeLanguage.map(lang -> "'" + lang + "'");
  //
  //   final List<Collection> arguments = Stream.of(
  //       maybeUseLiteral
  //           .map(useLiteral -> CodingCollection.fromLiteral(useLiteral, inputExpression)),
  //       maybeLanguageLiteral.map(
  //           languageLiteral -> StringCollection.fromLiteral(languageLiteral, inputExpression))
  //   ).flatMap(Optional::stream).collect(Collectors.toUnmodifiableList());
  //
  //   final NamedFunctionInput propertyInput = new NamedFunctionInput(parserContext, inputExpression,
  //       arguments);
  //
  //   // Invoke the function.
  //   final Collection result = NamedFunction.getInstance("designation").invoke(propertyInput);
  //
  //   final String expectedExpression = String.format("Encounter.class.designation(%s)",
  //       Stream.of(maybeUseLiteral, maybeLanguageLiteral)
  //           .flatMap(Optional::stream).collect(Collectors.joining(", ")));
  //   // Check the result.
  //   assertThat(result)
  //       .hasExpression(expectedExpression)
  //       .isElementPath(PrimitivePath.class)
  //       .hasFhirType(FHIRDefinedType.STRING)
  //       .isNotSingular()
  //       .selectOrderedResultWithEid()
  //       .hasRows(expectedResult);
  // }
  //
  // @Test
  // public void designationWithASingleResult() {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", makeEid(0, 0), "A_C_en")
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), null)
  //       .withRow("encounter-3", null, null)
  //       .build();
  //   checkDesignationsOfCoding(Optional.of(CODING_C), Optional.of("en"),
  //       expectedResult);
  // }
  //
  // @Test
  // public void designationWithMultipleResults() {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", makeEid(0, 0), null)
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), "B_D_fr.0")
  //       .withRow("encounter-2", makeEid(0, 1), "B_D_fr.1")
  //       .withRow("encounter-3", null, null)
  //       .build();
  //   checkDesignationsOfCoding(Optional.of(CODING_D), Optional.of("fr"),
  //       expectedResult);
  // }
  //
  //
  // @Test
  // public void designationWithDefaultLanguage() {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", makeEid(0, 0), "A_D_en")
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), "B_D_en")
  //       .withRow("encounter-2", makeEid(0, 1), "B_D_fr.0")
  //       .withRow("encounter-2", makeEid(0, 2), "B_D_fr.1")
  //       .withRow("encounter-3", null, null)
  //       .build();
  //   checkDesignationsOfCoding(Optional.of(CODING_D), Optional.empty(),
  //       expectedResult);
  // }
  //
  // @Test
  // public void designationWithDefaultLanguageAndUse() {
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", makeEid(0, 0), "A_C_en")
  //       .withRow("encounter-1", makeEid(0, 1), "A_D_en")
  //       .withRow("encounter-1", makeEid(0, 2), "A_?_??")
  //       .withRow("encounter-1", makeEid(1, 0), null)
  //       .withRow("encounter-2", makeEid(0, 0), "B_D_en")
  //       .withRow("encounter-2", makeEid(0, 1), "B_D_fr.0")
  //       .withRow("encounter-2", makeEid(0, 2), "B_D_fr.1")
  //       .withRow("encounter-3", null, null)
  //       .build();
  //   checkDesignationsOfCoding(Optional.empty(), Optional.empty(),
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
  //   final NamedFunctionInput designationInput = new NamedFunctionInput(parserContext, input,
  //       Collections.singletonList(argument));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new DesignationFunction().invoke(designationInput));
  //   assertEquals("Input to designation function must be Coding but is: name.given",
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
  //   final NamedFunctionInput designationInput = new NamedFunctionInput(context, input,
  //       argsFactory.apply(input));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new DesignationFunction().invoke(designationInput));
  //   assertEquals(expectedError,
  //       error.getMessage());
  // }
  //
  // @Test
  // void throwsErrorIfFirstArgumentIsNotCoding() {
  //   assertThrowsErrorForArguments("Function `designation` expects `Coding literal` as argument 1",
  //       input -> Collections.singletonList(
  //           IntegerLiteralPath.fromString("4", input)));
  // }
  //
  // @Test
  // void throwsErrorIfSecondArgumentIsNotBoolean() {
  //   assertThrowsErrorForArguments("Function `designation` expects `String literal` as argument 2",
  //       input -> Arrays.asList(
  //           CodingCollection.fromLiteral("system|code", input),
  //           IntegerLiteralPath.fromString("5", input)));
  // }
  //
  //
  // @Test
  // void throwsErrorIfTooManyArguments() {
  //   assertThrowsErrorForArguments(
  //       "designation function accepts two optional arguments",
  //       input -> Arrays.asList(
  //           CodingCollection.fromLiteral("system|code", input),
  //           StringCollection.fromLiteral("'false'", input),
  //           StringCollection.fromLiteral("'false'", input)
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
