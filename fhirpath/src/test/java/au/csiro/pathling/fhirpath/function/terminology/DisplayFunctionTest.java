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
import au.csiro.pathling.test.SpringBootUnitTest;

/**
 * @author Piotr Szul
 */
@SpringBootUnitTest
@NotImplemented
class DisplayFunctionTest {

  // TODO: implement with columns
  // public static final String LC_55915_3_DE_DISPLAY = "LC_55915_3 (DE)";
  // public static final String CD_SNOMED_VER_63816008_DE_DISPLAY = "CD_SNOMED_VER_63816008 (DE)";
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
  //   SharedMocks.resetAll();
  // }
  //
  //
  // private void checkDisplayCoding(final Optional<String> maybeLanguage,
  //     final String display_LC_55915_3, final String display_CD_SNOMED_VER_63816008) {
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
  //       .withRow("encounter-1", makeEid(0), rowFromCoding(LC_55915_3))
  //       .withRow("encounter-1", makeEid(1), rowFromCoding(INVALID_CODING_0))
  //       .withRow("encounter-2", makeEid(0), rowFromCoding(CD_SNOMED_VER_63816008))
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
  //   final Optional<StringLiteralPath> maybeArgumentExpression = maybeLanguage.map(
  //       lang -> StringCollection
  //           .fromLiteral("'" + lang + "'", inputExpression));
  //
  //   final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, inputExpression,
  //       maybeArgumentExpression.stream().collect(Collectors.toUnmodifiableList()));
  //
  //   // Invoke the function.
  //   final Collection result = new DisplayFunction().invoke(displayInput);
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.StringType)
  //       .withRow("encounter-1", makeEid(0), display_LC_55915_3)
  //       .withRow("encounter-1", makeEid(1), null)
  //       .withRow("encounter-2", makeEid(0), display_CD_SNOMED_VER_63816008)
  //       .withRow("encounter-3", null, null)
  //       .build();
  //
  //   // Check the result.
  //   assertThat(result)
  //       .hasExpression("Encounter.class.display(" + maybeArgumentExpression.map(
  //           StringLiteralPath::getExpression).orElse("") + ")")
  //       .isElementPath(PrimitivePath.class)
  //       .hasFhirType(FHIRDefinedType.STRING)
  //       .isNotSingular()
  //       .selectOrderedResultWithEid()
  //       .hasRows(expectedResult);
  // }
  //
  // @Test
  // public void displayCoding() {
  //   // Setup mocks
  //   TerminologyServiceHelpers.setupLookup(terminologyService)
  //       .withDisplay(LC_55915_3)
  //       .withDisplay(CD_SNOMED_VER_63816008);
  //   checkDisplayCoding(Optional.empty(), LC_55915_3.getDisplay(),
  //       CD_SNOMED_VER_63816008.getDisplay());
  // }
  //
  // @Test
  // public void displayCodingLanguage() {
  //
  //   // Setup mocks
  //   TerminologyServiceHelpers.setupLookup(terminologyService)
  //       .withDisplay(LC_55915_3, LC_55915_3_DE_DISPLAY, "de")
  //       .withDisplay(CD_SNOMED_VER_63816008, CD_SNOMED_VER_63816008_DE_DISPLAY, "de");
  //
  //   checkDisplayCoding(Optional.of("de"), LC_55915_3_DE_DISPLAY, CD_SNOMED_VER_63816008_DE_DISPLAY);
  // }
  //
  // @Test
  // void throwsErrorIfTerminologyServiceNotConfigured() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .build();
  //
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .build();
  //
  //   final NamedFunctionInput displayInput = new NamedFunctionInput(context, input,
  //       Collections.emptyList());
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new DisplayFunction().invoke(displayInput));
  //   assertEquals(
  //       "Attempt to call terminology function display when terminology service has not been configured",
  //       error.getMessage());
  // }
  //
  // @Test
  // void inputMustNotContainTwoArguments() {
  //   final PrimitivePath input = new ElementPathBuilder(spark).build();
  //   final StringLiteralPath argument1 = StringCollection
  //       .fromLiteral("'some argument'", input);
  //   final StringLiteralPath argument2 = StringCollection
  //       .fromLiteral("'some argument'", input);
  //   final List<Collection> arguments = new ArrayList<>();
  //   arguments.add(argument1);
  //   arguments.add(argument2);
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //   final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, input,
  //       arguments);
  //
  //   final NamedFunction displayFunction = NamedFunction.getInstance("display");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> displayFunction.invoke(displayInput));
  //   assertEquals(
  //       "display function accepts one optional language argument",
  //       error.getMessage());
  // }
  //
  // @Test
  // void inputMustNotContainNonStringArgument() {
  //   final PrimitivePath input = new ElementPathBuilder(spark).build();
  //   final IntegerLiteralPath argument = IntegerLiteralPath
  //       .fromString("9493948", input);
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //   final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, input,
  //       Collections.singletonList(argument));
  //
  //   final NamedFunction displayFunction = NamedFunction.getInstance("display");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> displayFunction.invoke(displayInput));
  //   assertEquals(
  //       "Function `display` expects `String literal` as argument 1",
  //       error.getMessage());
  // }
  //
  // @Test
  // void throwsErrorIfInputNotCoding() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.INTEGER)
  //       .expression("valueInteger")
  //       .build();
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark,
  //       fhirContext).terminologyClientFactory(terminologyServiceFactory)
  //       .build();
  //   final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, input,
  //       Collections.emptyList());
  //
  //   final NamedFunction displayFunction = NamedFunction.getInstance("display");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> displayFunction.invoke(displayInput));
  //   assertEquals(
  //       "Input to display function must be Coding but is: valueInteger",
  //       error.getMessage());
  // }
}
