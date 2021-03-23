/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.translate;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codeableConceptStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCodeableConcept;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.terminology.ConceptTranslator;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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
class TranslateFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  private static final String SOURCE_SYSTEM_URI = "uuid:source";
  private static final String DEST_SYSTEM_URI = "uuid:dest";

  private static final String CONCEPT_MAP1_URI = "http://snomed.info/sct?fhir_cm=100";
  private static final String CONCEPT_MAP2_URI = "http://snomed.info/sct?fhir_cm=200";


  private static final Coding CODING_1 = new Coding(SOURCE_SYSTEM_URI, "AMB", "ambulatory");
  private static final Coding CODING_2 = new Coding(SOURCE_SYSTEM_URI, "EMER", null);
  private static final Coding CODING_3 = new Coding(SOURCE_SYSTEM_URI, "IMP",
      "inpatient encounter");
  private static final Coding CODING_4 = new Coding(SOURCE_SYSTEM_URI, "OTHER", null);
  private static final Coding CODING_5 = new Coding(SOURCE_SYSTEM_URI, "ACUTE", "inpatient acute");

  private static final Coding TRANSLATED_1 = new Coding(DEST_SYSTEM_URI, "TEST1", "Test1");
  private static final Coding TRANSLATED_2 = new Coding(DEST_SYSTEM_URI, "TEST2", "Test2");


  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  TerminologyService terminologyService;

  @BeforeEach
  public void setUp() {
    reset(terminologyService);
  }

  @Test
  public void translateCodingWithDefaultArguments() {

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

    final ConceptTranslator returnedConceptTranslator = ConceptTranslatorBuilder
        .toSystem(DEST_SYSTEM_URI)
        .put(new SimpleCoding(CODING_1), TRANSLATED_1)
        .put(new SimpleCoding(CODING_2), TRANSLATED_1, TRANSLATED_2)
        .build();

    // Create a mock terminology client.
    when(terminologyService.translate(any(), any(), anyBoolean(), any()))
        .thenReturn(returnedConceptTranslator);

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
    final Set<SimpleCoding> expectedSourceCodings = ImmutableSet
        .of(new SimpleCoding(CODING_1), new SimpleCoding(CODING_2), new SimpleCoding(CODING_3),
            new SimpleCoding(CODING_5));

    final List<ConceptMapEquivalence> expectedEquivalences = Collections
        .singletonList(ConceptMapEquivalence.EQUIVALENT);

    verify(terminologyService)
        .translate(eq(expectedSourceCodings), eq(CONCEPT_MAP1_URI), eq(false),
            eq(expectedEquivalences));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  public void translateCodeableConceptWithNonDefaultArguments() {

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

    final ConceptTranslator returnedConceptTranslator = ConceptTranslatorBuilder
        .toSystem(DEST_SYSTEM_URI)
        .put(new SimpleCoding(CODING_1), TRANSLATED_1)
        .put(new SimpleCoding(CODING_2), TRANSLATED_1, TRANSLATED_2)
        .put(new SimpleCoding(CODING_4), TRANSLATED_2)
        .build();

    // Create a mock terminology client.
    when(terminologyService.translate(any(), any(), anyBoolean(), any()))
        .thenReturn(returnedConceptTranslator);

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
    final Set<SimpleCoding> expectedSourceCodings = ImmutableSet
        .of(new SimpleCoding(CODING_1), new SimpleCoding(CODING_2), new SimpleCoding(CODING_3),
            new SimpleCoding(CODING_4), new SimpleCoding(CODING_5));

    final List<ConceptMapEquivalence> expectedEquivalences = Arrays
        .asList(ConceptMapEquivalence.NARROWER, ConceptMapEquivalence.EQUIVALENT);

    verify(terminologyService)
        .translate(eq(expectedSourceCodings), eq(CONCEPT_MAP2_URI), eq(true),
            eq(expectedEquivalences));
    verifyNoMoreInteractions(terminologyService);
  }


  @Test
  public void throwsErrorIfInputTypeIsUnsupported() {
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


  private void assertThrowsErrorForArguments(@Nonnull final String expectedError,
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
  public void throwsErrorIfNoArguments() {
    assertThrowsErrorForArguments(
        "translate function accepts one required and two optional arguments",
        input -> Collections.emptyList());
  }

  @Test
  public void throwsErrorIfFirstArgumentIsNotString() {
    assertThrowsErrorForArguments("Function `translate` expects `String literal` as argument 1",
        input -> Collections.singletonList(
            IntegerLiteralPath.fromString("4", input)));
  }

  @Test
  public void throwsErrorIfSecondArgumentIsNotBoolean() {
    assertThrowsErrorForArguments("Function `translate` expects `Boolean literal` as argument 2",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            StringLiteralPath.fromString("'bar'", input)));
  }


  @Test
  public void throwsErrorIfThirdArgumentIsNotString() {
    assertThrowsErrorForArguments("Function `translate` expects `String literal` as argument 3",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            BooleanLiteralPath.fromString("true", input),
            BooleanLiteralPath.fromString("false", input)));
  }


  @Test
  public void throwsErrorIfTooManyArguments() {
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
  public void throwsErrorIfCannotParseEquivalences() {
    assertThrowsErrorForArguments(
        "Unknown ConceptMapEquivalence code 'not-an-equivalence'",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            BooleanLiteralPath.fromString("true", input),
            StringLiteralPath.fromString("'not-an-equivalence'", input)
        ));
  }

  @Test
  public void throwsErrorIfTerminologyServiceNotConfigured() {
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