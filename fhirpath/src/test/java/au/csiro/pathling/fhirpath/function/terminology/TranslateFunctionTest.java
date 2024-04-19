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

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.SparkHelpers.codeableConceptStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCodeableConcept;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.NARROWER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class TranslateFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  static final String SOURCE_SYSTEM_URI = "uuid:source";
  static final String DEST_SYSTEM_URI = "uuid:dest";

  static final String CONCEPT_MAP1_URI = "http://snomed.info/sct?fhir_cm=100";
  static final String CONCEPT_MAP2_URI = "http://snomed.info/sct?fhir_cm=200";


  static final Coding CODING_1 = new Coding(SOURCE_SYSTEM_URI, "AMB", "ambulatory");
  static final Coding CODING_2 = new Coding(SOURCE_SYSTEM_URI, "EMER", null);
  static final Coding CODING_3 = new Coding(SOURCE_SYSTEM_URI, "IMP",
      "inpatient encounter");
  static final Coding CODING_4 = new Coding(SOURCE_SYSTEM_URI, "OTHER", null);
  static final Coding CODING_5 = new Coding(SOURCE_SYSTEM_URI, "ACUTE", "inpatient acute");

  static final Coding TRANSLATED_1 = new Coding(DEST_SYSTEM_URI, "TEST1", "Test1");
  static final Coding TRANSLATED_2 = new Coding(DEST_SYSTEM_URI, "TEST2", "Test2");


  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    reset(terminologyService);
  }

  @Test
  void translateCodingWithDefaultArguments() {

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    // The translations are
    // {
    //    coding1 -> [translated1],
    //    coding2 -> [translated1, translated2]
    // }

    // Use cases:
    // 1. [ C2,C3,C1 ] -> [ [T1, T2],[], [T1]]
    // 2. [ C3, C5 ] -> [[],[]]
    // 3. [ C2 ] -> [ [T1, T2] ]
    // 4. [] -> []
    // 5. null -> null

    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        // TC-1
        .withRow("encounter-1", makeEid(0), rowFromCoding(CODING_2))
        .withRow("encounter-1", makeEid(1), rowFromCoding(CODING_3))
        .withRow("encounter-1", makeEid(2), rowFromCoding(CODING_1))
        // TC-2
        .withRow("encounter-2", makeEid(0), rowFromCoding(CODING_3))
        .withRow("encounter-2", makeEid(1), rowFromCoding(CODING_5))
        // TC-3
        .withRow("encounter-3", makeEid(0), rowFromCoding(CODING_2))
        // TC-4
        .withRow("encounter-4", makeEid(0), null)
        // TC-5
        .withRow("encounter-5", null, null)
        .buildWithStructValue();

    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder(spark)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.class")
        .singular(false)
        .definition(definition)
        .buildDefined();

    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(CODING_1, CONCEPT_MAP1_URI,
            Translation.of(EQUIVALENT, TRANSLATED_1))
        .withTranslations(CODING_2, CONCEPT_MAP1_URI,
            Translation.of(EQUIVALENT, TRANSLATED_1),
            Translation.of(EQUIVALENT, TRANSLATED_2)
        );

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputExpression.getIdColumn())
        .terminologyClientFactory(terminologyServiceFactory)
        .build();

    final StringLiteralPath conceptMapUrlArgument = StringLiteralPath
        .fromString("'" + CONCEPT_MAP1_URI + "'", inputExpression);

    final NamedFunctionInput translateInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(conceptMapUrlArgument));
    // Invoke the function.
    final FhirPath result = new TranslateFunction().invoke(translateInput);
    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        // TC-1
        .withRow("encounter-1", makeEid(0, 0), rowFromCoding(TRANSLATED_1))
        .withRow("encounter-1", makeEid(0, 1), rowFromCoding(TRANSLATED_2))
        .withRow("encounter-1", makeEid(1, 0), null)
        .withRow("encounter-1", makeEid(2, 0), rowFromCoding(TRANSLATED_1))
        // TC-2
        .withRow("encounter-2", makeEid(0, 0), null)
        .withRow("encounter-2", makeEid(1, 0), null)
        // TC-3
        .withRow("encounter-3", makeEid(0, 0), rowFromCoding(TRANSLATED_1))
        .withRow("encounter-3", makeEid(0, 1), rowFromCoding(TRANSLATED_2))
        // TC-4
        .withRow("encounter-4", makeEid(0, 0), null)
        // TC-5
        .withRow("encounter-5", null, null)
        .buildWithStructValue();

    // Check the result.
    assertThat(result)
        .hasExpression(
            "Encounter.class.translate('" + CONCEPT_MAP1_URI + "')")
        .isElementPath(CodingPath.class)
        .hasFhirType(FHIRDefinedType.CODING)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);

    // Verify mocks
    Stream.of(CODING_1, CODING_2, CODING_3, CODING_5).forEach(coding ->
        verify(terminologyService, atLeastOnce())
            .translate(deepEq(coding), eq(CONCEPT_MAP1_URI), eq(false), isNull())
    );
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void translateCodeableConceptWithNonDefaultArguments() {

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "type");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    // The translations are
    // {
    //    coding1 -> [translated1],
    //    coding2 -> [translated1, translated2]
    //    coding4 -> [translated2]
    // }

    // Use cases:
    // 1. [ {C2,C3,C1}, {C3}, {C4} ] -> [ [T1, T2],[], [T2]]
    // 2. [ {C3, C5}, {C3} ] -> [ [], [] ]
    // 3. [ {C2} ] -> [[T1, T2]]
    // 4. [ {C3}] -> [[]]
    // 5. [ ]-> []
    // 6. null -> null

    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codeableConceptStructType())
        // TC-1
        .withRow("encounter-1", makeEid(0),
            rowFromCodeableConcept(
                new CodeableConcept(CODING_2).addCoding(CODING_3).addCoding(CODING_1)))
        .withRow("encounter-1", makeEid(1),
            rowFromCodeableConcept(
                new CodeableConcept(CODING_3).addCoding(CODING_5)))
        .withRow("encounter-1", makeEid(2),
            rowFromCodeableConcept(
                new CodeableConcept(CODING_4)))
        // TC-2
        .withRow("encounter-2", makeEid(0),
            rowFromCodeableConcept(new CodeableConcept(CODING_3).addCoding(CODING_5)))
        .withRow("encounter-2", makeEid(1),
            rowFromCodeableConcept(new CodeableConcept(CODING_3)))
        // TC-3
        .withRow("encounter-3", makeEid(0), rowFromCodeableConcept(new CodeableConcept(CODING_2)))
        // TC-4
        .withRow("encounter-4", makeEid(0), rowFromCodeableConcept(new CodeableConcept(CODING_3)))
        // TC-5
        .withRow("encounter-5", makeEid(0), null)
        .withRow("encounter-6", null, null)
        .buildWithStructValue();

    final ElementPath inputExpression = new ElementPathBuilder(spark)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.type")
        .singular(false)
        .definition(definition)
        .buildDefined();

    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(CODING_1, CONCEPT_MAP2_URI, true,
            Translation.of(EQUIVALENT, TRANSLATED_1))
        .withTranslations(CODING_2, CONCEPT_MAP2_URI, true,
            Translation.of(EQUIVALENT, TRANSLATED_1),
            Translation.of(EQUIVALENT, TRANSLATED_2)
        ).withTranslations(CODING_4, CONCEPT_MAP2_URI, true,
            Translation.of(NARROWER, TRANSLATED_2)
        );

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputExpression.getIdColumn())
        .terminologyClientFactory(terminologyServiceFactory)
        .build();

    final StringLiteralPath conceptMapUrlArgument = StringLiteralPath
        .fromString("'" + CONCEPT_MAP2_URI + "'", inputExpression);

    final BooleanLiteralPath reverseArgument = BooleanLiteralPath
        .fromString("true", inputExpression);

    final StringLiteralPath equivalenceArgument = StringLiteralPath
        .fromString("narrower,equivalent", inputExpression);

    final NamedFunctionInput translateInput = new NamedFunctionInput(parserContext, inputExpression,
        Arrays.asList(conceptMapUrlArgument, reverseArgument, equivalenceArgument));
    // Invoke the function.
    final FhirPath result = new TranslateFunction().invoke(translateInput);

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        // TC-1
        .withRow("encounter-1", makeEid(0, 0), rowFromCoding(TRANSLATED_1))
        .withRow("encounter-1", makeEid(0, 1), rowFromCoding(TRANSLATED_2))
        .withRow("encounter-1", makeEid(1, 0), null)
        .withRow("encounter-1", makeEid(2, 0), rowFromCoding(TRANSLATED_2))
        // TC-2
        .withRow("encounter-2", makeEid(0, 0), null)
        .withRow("encounter-2", makeEid(1, 0), null)
        // TC-3
        .withRow("encounter-3", makeEid(0, 0), rowFromCoding(TRANSLATED_1))
        .withRow("encounter-3", makeEid(0, 1), rowFromCoding(TRANSLATED_2))
        // TC-4
        .withRow("encounter-4", makeEid(0, 0), null)
        // TC-5
        .withRow("encounter-5", makeEid(0, 0), null)
        .withRow("encounter-6", null, null)
        .buildWithStructValue();

    // Check the result.
    assertThat(result)
        .hasExpression(
            "Encounter.type.translate('" + CONCEPT_MAP2_URI + "', true, 'narrower,equivalent')")
        .isElementPath(CodingPath.class)
        .hasFhirType(FHIRDefinedType.CODING)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);

    // Verify mocks
    Stream.of(CODING_1, CODING_2, CODING_3, CODING_4, CODING_5).forEach(coding ->
        verify(terminologyService, atLeastOnce())
            .translate(deepEq(coding), eq(CONCEPT_MAP2_URI), eq(true), isNull())
    );
    verifyNoMoreInteractions(terminologyService);
  }


  @Test
  void throwsErrorIfInputTypeIsUnsupported() {
    final FhirPath mockContext = new ElementPathBuilder(spark).build();
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .expression("name.given")
        .build();
    final FhirPath argument = StringLiteralPath.fromString(SOURCE_SYSTEM_URI, mockContext);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("Input to translate function is of unsupported type: name.given",
        error.getMessage());
  }


  void assertThrowsErrorForArguments(@Nonnull final String expectedError,
      @Nonnull final Function<ElementPath, List<FhirPath>> argsFactory) {

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .definition(definition)
        .buildDefined();

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        argsFactory.apply(input));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals(expectedError,
        error.getMessage());

  }

  @Test
  void throwsErrorIfNoArguments() {
    assertThrowsErrorForArguments(
        "translate function accepts one required and two optional arguments",
        input -> Collections.emptyList());
  }

  @Test
  void throwsErrorIfFirstArgumentIsNotString() {
    assertThrowsErrorForArguments("Function `translate` expects `String literal` as argument 1",
        input -> Collections.singletonList(
            IntegerLiteralPath.fromString("4", input)));
  }

  @Test
  void throwsErrorIfSecondArgumentIsNotBoolean() {
    assertThrowsErrorForArguments("Function `translate` expects `Boolean literal` as argument 2",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            StringLiteralPath.fromString("'bar'", input)));
  }


  @Test
  void throwsErrorIfThirdArgumentIsNotString() {
    assertThrowsErrorForArguments("Function `translate` expects `String literal` as argument 3",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            BooleanLiteralPath.fromString("true", input),
            BooleanLiteralPath.fromString("false", input)));
  }


  @Test
  void throwsErrorIfTooManyArguments() {
    assertThrowsErrorForArguments(
        "translate function accepts one required and two optional arguments",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            BooleanLiteralPath.fromString("true", input),
            StringLiteralPath.fromString("'false'", input),
            StringLiteralPath.fromString("'false'", input)
        ));
  }

  @Test
  void throwsErrorIfCannotParseEquivalences() {
    assertThrowsErrorForArguments(
        "Unknown ConceptMapEquivalence code 'not-an-equivalence'",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            BooleanLiteralPath.fromString("true", input),
            StringLiteralPath.fromString("'not-an-equivalence'", input)
        ));
  }

  @Test
  void throwsErrorIfTerminologyServiceNotConfigured() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final FhirPath argument = StringLiteralPath.fromString("some string", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals(
        "Attempt to call terminology function translate when terminology service has not been configured",
        error.getMessage());
  }

}
