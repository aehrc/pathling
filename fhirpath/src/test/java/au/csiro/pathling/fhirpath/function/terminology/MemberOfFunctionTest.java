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
 * @author John Grimes
 */
@SpringBootUnitTest
@NotImplemented
class MemberOfFunctionTest {

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
  //   SharedMocks.resetAll();
  // }
  //
  // static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";
  //
  // @Test
  // void memberOfCoding() {
  //   final Coding coding1 = new Coding(MY_VALUE_SET_URL, "AMB", "ambulatory");
  //   final Coding coding2 = new Coding(MY_VALUE_SET_URL, "EMER", null);
  //   final Coding coding3 = new Coding(MY_VALUE_SET_URL, "IMP", "inpatient encounter");
  //   final Coding coding4 = new Coding(MY_VALUE_SET_URL, "IMP", null);
  //   final Coding coding5 = new Coding(MY_VALUE_SET_URL, "ACUTE", "inpatient acute");
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
  //       .withRow("encounter-1", makeEid(1), rowFromCoding(coding1))
  //       .withRow("encounter-1", makeEid(0), rowFromCoding(coding5))
  //       .withRow("encounter-2", makeEid(0), rowFromCoding(coding2))
  //       .withRow("encounter-3", makeEid(0), rowFromCoding(coding3))
  //       .withRow("encounter-4", makeEid(0), rowFromCoding(coding4))
  //       .withRow("encounter-5", makeEid(0), rowFromCoding(coding5))
  //       .withRow("encounter-6", null, null)
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
  //   final StringLiteralPath argumentExpression = StringCollection
  //       .fromLiteral("'" + MY_VALUE_SET_URL + "'", inputExpression);
  //
  //   // Setup mocks
  //   TerminologyServiceHelpers.setupValidate(terminologyService)
  //       .withValueSet(MY_VALUE_SET_URL, coding2, coding5);
  //   // Prepare the inputs to the function.
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .idColumn(inputExpression.getIdColumn())
  //       .terminologyClientFactory(terminologyServiceFactory)
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
  //       Collections.singletonList(argumentExpression));
  //
  //   // Invoke the function.
  //   final Collection result = new MemberOfFunction().invoke(memberOfInput);
  //
  //   // The outcome is somehow random with regard to the sequence passed to MemberOfMapperAnswerer.
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("encounter-1", makeEid(0), true)
  //       .withRow("encounter-1", makeEid(1), false)
  //       .withRow("encounter-2", makeEid(0), true)
  //       .withRow("encounter-3", makeEid(0), false)
  //       .withRow("encounter-4", makeEid(0), false)
  //       .withRow("encounter-5", makeEid(0), true)
  //       .withRow("encounter-6", null, null)
  //       .build();
  //
  //   // Check the result.
  //   assertThat(result)
  //       .hasExpression("Encounter.class.memberOf('" + MY_VALUE_SET_URL + "')")
  //       .isElementPath(BooleanCollection.class)
  //       .hasFhirType(FHIRDefinedType.BOOLEAN)
  //       .isNotSingular()
  //       .selectOrderedResultWithEid()
  //       .hasRows(expectedResult);
  //
  //   verify(terminologyService).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding1));
  //   verify(terminologyService).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding2));
  //   verify(terminologyService).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding3));
  //   verify(terminologyService).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding4));
  //   verify(terminologyService, times(2)).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding5));
  //   verifyNoMoreInteractions(terminologyService);
  // }
  //
  //
  // @Test
  // void memberOfEmptyCodingDatasetDoesNotCallTerminology() {
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
  //   final StringLiteralPath argumentExpression = StringCollection
  //       .fromLiteral("'" + MY_VALUE_SET_URL + "'", inputExpression);
  //
  //   // Prepare the inputs to the function.
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .idColumn(inputExpression.getIdColumn())
  //       .terminologyClientFactory(terminologyServiceFactory)
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
  //       Collections.singletonList(argumentExpression));
  //
  //   // Invoke the function.
  //   final Collection result = new MemberOfFunction().invoke(memberOfInput);
  //
  //   // The outcome is somehow random with regard to the sequence passed to MemberOfMapperAnswerer.
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.BooleanType)
  //       .build();
  //
  //   // Check the result.
  //   assertThat(result)
  //       .hasExpression("Encounter.class.memberOf('" + MY_VALUE_SET_URL + "')")
  //       .isElementPath(BooleanCollection.class)
  //       .hasFhirType(FHIRDefinedType.BOOLEAN)
  //       .isNotSingular()
  //       .selectOrderedResultWithEid()
  //       .hasRows(expectedResult);
  //
  //   verifyNoMoreInteractions(terminologyService);
  // }
  //
  // @Test
  // void memberOfCodeableConcept() {
  //   final Coding coding1 = new Coding(LOINC_URL, "10337-4",
  //       "Procollagen type I [Mass/volume] in Serum");
  //   final Coding coding2 = new Coding(LOINC_URL, "10428-1",
  //       "Varicella zoster virus immune globulin given [Volume]");
  //   final Coding coding3 = new Coding(LOINC_URL, "10555-1", null);
  //   final Coding coding4 = new Coding(LOINC_URL, "10665-8",
  //       "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
  //   final Coding coding5 = new Coding(SNOMED_URL, "416399002",
  //       "Procollagen type I amino-terminal propeptide level");
  //
  //   final CodeableConcept codeableConcept1 = new CodeableConcept(coding1);
  //   codeableConcept1.addCoding(coding5);
  //   final CodeableConcept codeableConcept2 = new CodeableConcept(coding2);
  //   final CodeableConcept codeableConcept3 = new CodeableConcept(coding3);
  //   final CodeableConcept codeableConcept4 = new CodeableConcept(coding3);
  //   final CodeableConcept codeableConcept5 = new CodeableConcept(coding4);
  //   final CodeableConcept codeableConcept6 = new CodeableConcept(coding1);
  //
  //   final Optional<ElementDefinition> optionalDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "DiagnosticReport", "code");
  //   assertTrue(optionalDefinition.isPresent());
  //   final ElementDefinition definition = optionalDefinition.get();
  //
  //   final Dataset<Row> inputDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withStructTypeColumns(codeableConceptStructType())
  //       .withRow("diagnosticreport-1", rowFromCodeableConcept(codeableConcept1))
  //       .withRow("diagnosticreport-2", rowFromCodeableConcept(codeableConcept2))
  //       .withRow("diagnosticreport-3", rowFromCodeableConcept(codeableConcept3))
  //       .withRow("diagnosticreport-4", rowFromCodeableConcept(codeableConcept4))
  //       .withRow("diagnosticreport-5", rowFromCodeableConcept(codeableConcept5))
  //       .withRow("diagnosticreport-6", rowFromCodeableConcept(codeableConcept6))
  //       .withRow("diagnosticreport-7", null)
  //       .buildWithStructValue();
  //
  //   final PrimitivePath inputExpression = new ElementPathBuilder(spark)
  //       .dataset(inputDataset)
  //       .idAndValueColumns()
  //       .expression("DiagnosticReport.code")
  //       .singular(true)
  //       .definition(definition)
  //       .buildDefined();
  //
  //   final StringLiteralPath argumentExpression = StringCollection
  //       .fromLiteral("'" + MY_VALUE_SET_URL + "'", inputExpression);
  //
  //   // Setup mocks: true for (codeableConcept1, codeableConcept3, codeableConcept4)
  //   TerminologyServiceHelpers.setupValidate(terminologyService)
  //       .withValueSet(MY_VALUE_SET_URL, coding1, coding3, coding5);
  //   // Prepare the inputs to the function.
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(terminologyServiceFactory)
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
  //       Collections.singletonList(argumentExpression));
  //
  //   // Invoke the function.
  //   final Collection result = new MemberOfFunction().invoke(memberOfInput);
  //
  //   final Dataset<Row> expectedResult = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("diagnosticreport-1", true)
  //       .withRow("diagnosticreport-2", false)
  //       .withRow("diagnosticreport-3", true)
  //       .withRow("diagnosticreport-4", true)
  //       .withRow("diagnosticreport-5", false)
  //       .withRow("diagnosticreport-6", true)
  //       .withRow("diagnosticreport-7", null)
  //       .build();
  //
  //   // Check the result.
  //   assertTrue(result instanceof BooleanCollection);
  //   assertThat((BooleanCollection) result)
  //       .hasExpression("DiagnosticReport.code.memberOf('" + MY_VALUE_SET_URL + "')")
  //       .isSingular()
  //       .hasFhirType(FHIRDefinedType.BOOLEAN)
  //       .isElementPath(BooleanCollection.class)
  //       .selectOrderedResult()
  //       .hasRows(expectedResult);
  //
  //   verify(terminologyService, atLeastOnce()).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding1));
  //   verify(terminologyService, atLeastOnce()).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding2));
  //   verify(terminologyService, atLeastOnce()).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding3));
  //   verify(terminologyService, atLeastOnce()).validateCode(eq(MY_VALUE_SET_URL), deepEq(coding4));
  //   verifyNoMoreInteractions(terminologyService);
  // }
  //
  // @Test
  // void throwsErrorIfInputTypeIsUnsupported() {
  //   final Collection mockContext = new ElementPathBuilder(spark).build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .expression("name.given")
  //       .build();
  //   final Collection argument = StringCollection.fromLiteral(MY_VALUE_SET_URL, mockContext);
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(mock(TerminologyServiceFactory.class))
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, input,
  //       Collections.singletonList(argument));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new MemberOfFunction().invoke(memberOfInput));
  //   assertEquals("Input to memberOf function is of unsupported type: name.given",
  //       error.getMessage());
  // }
  //
  // @Test
  // void throwsErrorIfArgumentIsNotString() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODEABLECONCEPT)
  //       .build();
  //   final IntegerLiteralPath argument = IntegerLiteralPath.fromString("4", input);
  //
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(mock(TerminologyServiceFactory.class))
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
  //       Collections.singletonList(argument));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new MemberOfFunction().invoke(memberOfInput));
  //   assertEquals("memberOf function accepts one argument of type String literal",
  //       error.getMessage());
  // }
  //
  // @Test
  // void throwsErrorIfMoreThanOneArgument() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODEABLECONCEPT)
  //       .build();
  //   final StringLiteralPath argument1 = StringCollection.fromLiteral("'foo'", input),
  //       argument2 = StringCollection.fromLiteral("'bar'", input);
  //
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .terminologyClientFactory(mock(TerminologyServiceFactory.class))
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
  //       Arrays.asList(argument1, argument2));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new MemberOfFunction().invoke(memberOfInput));
  //   assertEquals("memberOf function accepts one argument of type String",
  //       error.getMessage());
  // }
  //
  // @Test
  // void throwsErrorIfTerminologyServiceNotConfigured() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODEABLECONCEPT)
  //       .build();
  //   final Collection argument = StringCollection.fromLiteral("some string", input);
  //
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .build();
  //
  //   final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
  //       Collections.singletonList(argument));
  //
  //   final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
  //       () -> new MemberOfFunction().invoke(memberOfInput));
  //   assertEquals(
  //       "Attempt to call terminology function memberOf when terminology service has not been configured",
  //       error.getMessage());
  // }

}
