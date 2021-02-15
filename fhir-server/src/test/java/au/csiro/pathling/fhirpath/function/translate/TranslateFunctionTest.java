/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.translate;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
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
import au.csiro.pathling.terminology.ConceptMapper;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
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

  private static final String CONCEPT_MAP_URI = "http://snomed.info/sct?fhir_cm=100";


  // TODO: This need rethinking
  // The problem is that the mock that are actually used in SparkTasks are ser/de copies
  // of the original mocks. This works for stubbing but not for verification
  // as the original mocks are not being called at all.
  private final static TerminologyService terminologyService = mock(TerminologyService.class);

  @Nullable
  private TerminologyClientFactory terminologyClientFactory;

  static class TestFactory extends TerminologyClientFactory {

    /**
     * @param fhirContext the {@link FhirContext} used to build the client
     */
    public TestFactory(@Nonnull FhirContext fhirContext, TerminologyService terminologyService) {
      super(fhirContext, "", 0, false);
    }

    @Nonnull
    @Override
    public TerminologyService buildService(@Nonnull Logger logger) {
      return TranslateFunctionTest.terminologyService;
    }
  }

  @BeforeEach
  public void setUp() {
    reset(terminologyService);
    terminologyClientFactory = new TestFactory(fhirContext, terminologyService);
  }

  @Test
  public void translateCoding() {
    final Coding coding1 = new Coding(SOURCE_SYSTEM_URI, "AMB", "ambulatory");
    final Coding coding2 = new Coding(SOURCE_SYSTEM_URI, "EMER", null);
    final Coding coding3 = new Coding(SOURCE_SYSTEM_URI, "IMP", "inpatient encounter");
    final Coding coding4 = new Coding(SOURCE_SYSTEM_URI, "IMP", null);
    final Coding coding5 = new Coding(SOURCE_SYSTEM_URI, "ACUTE", "inpatient acute");

    final Coding translated1 = new Coding(DEST_SYSTEM_URI, "TEST1", "Test");
    final Coding translated2 = new Coding(DEST_SYSTEM_URI, "TEST2", "Test");

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("encounter-1", makeEid(2), rowFromCoding(coding2))
        .withRow("encounter-1", makeEid(1), rowFromCoding(coding3))
        .withRow("encounter-1", makeEid(0), rowFromCoding(coding1))
        .withRow("encounter-2", makeEid(0), rowFromCoding(coding3))
        .withRow("encounter-3", makeEid(0), rowFromCoding(coding1))
        .withRow("encounter-4", makeEid(0), rowFromCoding(coding2))
        .withRow("encounter-5", makeEid(0), null)
        .withRow("encounter-6", null, null)
        .buildWithStructValue();

    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder(spark)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.class")
        .singular(false)
        .definition(definition)
        .buildDefined();

    // The translations are
    // {
    //    coding1 -> [translated1],
    //    coding2 -> [translated1, translated2]
    // }

    final Map<SimpleCoding, List<Coding>> translationMap = ImmutableMap.<SimpleCoding, List<Coding>>builder()
        .put(new SimpleCoding(coding1), Collections.singletonList(translated1))
        .put(new SimpleCoding(coding2), Arrays.asList(translated1, translated2))
        .build();
    final ConceptMapper returnedConceptMapper = new ConceptMapper(translationMap);

    // Create a mock terminology client.
    when(terminologyService.translate(any(), any(), anyBoolean(), any()))
        .thenReturn(returnedConceptMapper);

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputExpression.getIdColumn())
        .terminologyClientFactory(terminologyClientFactory)
        .build();

    final StringLiteralPath conceptMapUrlArgument = StringLiteralPath
        .fromString("'" + CONCEPT_MAP_URI + "'", inputExpression);

    final BooleanLiteralPath reverseArgument = BooleanLiteralPath
        .fromString("false", inputExpression);

    final StringLiteralPath equivalenceArgument = StringLiteralPath
        .fromString("wider,equal", inputExpression);

    final NamedFunctionInput translateInput = new NamedFunctionInput(parserContext, inputExpression,
        Arrays.asList(conceptMapUrlArgument, reverseArgument, equivalenceArgument));
    // Invoke the function.
    final FhirPath result = new TranslateFunction().invoke(translateInput);

    // The outcome is somehow random with regard to the sequence passed to MemberOfMapperAnswerer.
    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("encounter-1", makeEid(0), rowFromCoding(translated1))
        .withRow("encounter-1", makeEid(1), null)
        .withRow("encounter-1", makeEid(2), rowFromCoding(translated1))
        .withRow("encounter-1", makeEid(2), rowFromCoding(translated2))
        .withRow("encounter-2", makeEid(0), null)
        .withRow("encounter-3", makeEid(0), rowFromCoding(translated1))
        .withRow("encounter-4", makeEid(0), rowFromCoding(translated1))
        .withRow("encounter-4", makeEid(0), rowFromCoding(translated2))
        .withRow("encounter-5", makeEid(0), null)
        .withRow("encounter-6", null, null)
        .buildWithStructValue();

    // Check the result.
    assertThat(result)
        .hasExpression(
            "Encounter.class.translate('" + CONCEPT_MAP_URI + "', false, 'wider,equal')")
        .isElementPath(CodingPath.class)
        .hasFhirType(FHIRDefinedType.CODING)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .debugAllRows()
        .hasRows(expectedResult);

    // Verify mocks
    final Set<SimpleCoding> expectedSourceCodings = ImmutableSet
        .of(new SimpleCoding(coding1), new SimpleCoding(coding2), new SimpleCoding(coding3));

    final List<ConceptMapEquivalence> expectedEquivalences = Arrays
        .asList(ConceptMapEquivalence.WIDER, ConceptMapEquivalence.EQUAL);

    verify(terminologyService)
        .translate(eq(expectedSourceCodings), eq(CONCEPT_MAP_URI), eq(false),
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
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("Input to translate function is of unsupported type: name.given",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentIsNotString() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final IntegerLiteralPath argument = IntegerLiteralPath.fromString("4", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Arrays.asList(argument, argument, argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("Function `translate` expects `String literal` as argument 1",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfLessThanThreeArguments() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final StringLiteralPath argument1 = StringLiteralPath.fromString("'foo'", input),
        argument2 = StringLiteralPath.fromString("'bar'", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Arrays.asList(argument1, argument2));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals("translate function accepts 3 arguments",
        error.getMessage());
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