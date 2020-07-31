/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import static au.csiro.pathling.test.assertions.Assertions.assertEquals;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.assertions.Assertions.assertThrows;
import static au.csiro.pathling.test.assertions.Assertions.assertTrue;
import static au.csiro.pathling.test.helpers.SparkHelpers.codeableConceptStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCodeableConcept;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static au.csiro.pathling.test.helpers.TestHelpers.LOINC_URL;
import static au.csiro.pathling.test.helpers.TestHelpers.SNOMED_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.ElementPathBuilder;
import au.csiro.pathling.test.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.FhirHelpers.ValidateCodeMapperAnswerer;
import au.csiro.pathling.test.helpers.FhirHelpers.ValidateCodeableConceptTxAnswerer;
import au.csiro.pathling.test.helpers.FhirHelpers.ValidateCodingTxAnswerer;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class MemberOfFunctionTest {

  private static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";
  private static final String TERMINOLOGY_SERVICE_URL = "https://r4.ontoserver.csiro.au/fhir";

  @Test
  public void memberOfCoding() {
    final Coding coding1 = new Coding(MY_VALUE_SET_URL, "AMB", "ambulatory");
    final Coding coding2 = new Coding(MY_VALUE_SET_URL, "EMER", null);
    final Coding coding3 = new Coding(MY_VALUE_SET_URL, "IMP", "inpatient encounter");
    final Coding coding4 = new Coding(MY_VALUE_SET_URL, "IMP", null);
    final Coding coding5 = new Coding(MY_VALUE_SET_URL, "ACUTE", "inpatient acute");

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("Encounter/1", rowFromCoding(coding1))
        .withRow("Encounter/2", rowFromCoding(coding2))
        .withRow("Encounter/3", rowFromCoding(coding3))
        .withRow("Encounter/4", rowFromCoding(coding4))
        .withRow("Encounter/5", rowFromCoding(coding5))
        .buildWithStructValue();
    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder()
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Encounter.class")
        .singular(true)
        .definition(definition)
        .buildDefined();

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + MY_VALUE_SET_URL + "'", inputExpression);

    // Create a mock terminology client.
    final TerminologyClient terminologyClient = mock(TerminologyClient.class);
    final Answer<Bundle> validateCodingTxAnswerer = new ValidateCodingTxAnswerer(coding2, coding5);
    when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
    when(terminologyClient.batch(any(Bundle.class))).thenAnswer(validateCodingTxAnswerer);

    // Create a mock TerminologyClientFactory, and make it return the mock terminology client.
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Create a mock ValidateCodeMapper.
    final ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
    final Answer<Iterator<ValidateCodeResult>> validateCodeMapperAnswerer =
        new ValidateCodeMapperAnswerer(false, true, false, false, true);
    //noinspection unchecked
    when(mockCodeMapper.call(any(Iterator.class))).thenAnswer(validateCodeMapperAnswerer);

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder()
        .idColumn(inputExpression.getIdColumn())
        .terminologyClient(terminologyClient)
        .terminologyClientFactory(terminologyClientFactory)
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentExpression));

    // Invoke the function.
    final FhirPath result = new MemberOfFunction(mockCodeMapper).invoke(memberOfInput);

    // Check the result.
    assertTrue(result instanceof BooleanPath);
    assertThat((BooleanPath) result)
        .hasExpression("Encounter.class.memberOf('" + MY_VALUE_SET_URL + "')")
        .isSingular()
        .hasFhirType(FHIRDefinedType.BOOLEAN);

    // Test the mapper.
    final ValidateCodeMapper validateCodingMapper = new ValidateCodeMapper("xyz",
        terminologyClientFactory,
        MY_VALUE_SET_URL, FHIRDefinedType.CODING);
    final Row inputCodingRow1 = RowFactory.create(1, rowFromCoding(coding1));
    final Row inputCodingRow2 = RowFactory.create(2, rowFromCoding(coding2));
    final Row inputCodingRow3 = RowFactory.create(3, rowFromCoding(coding3));
    final Row inputCodingRow4 = RowFactory.create(4, rowFromCoding(coding4));
    final Row inputCodingRow5 = RowFactory.create(5, rowFromCoding(coding5));
    final List<Row> inputCodingRows = Arrays
        .asList(inputCodingRow1, inputCodingRow2, inputCodingRow3, inputCodingRow4,
            inputCodingRow5);
    final List<ValidateCodeResult> results = new ArrayList<>();
    validateCodingMapper.call(inputCodingRows.iterator()).forEachRemaining(results::add);

    // Check the result dataset.
    final List<ValidateCodeResult> expectedResults = Arrays.asList(
        new ValidateCodeResult(1, false),
        new ValidateCodeResult(2, true),
        new ValidateCodeResult(3, false),
        new ValidateCodeResult(4, false),
        new ValidateCodeResult(5, true)
    );
    assertEquals(expectedResults, results);
  }

  @Test
  public void memberOfCodeableConcept() {
    final Coding coding1 = new Coding(LOINC_URL, "10337-4",
        "Procollagen type I [Mass/volume] in Serum");
    final Coding coding2 = new Coding(LOINC_URL, "10428-1",
        "Varicella zoster virus immune globulin given [Volume]");
    final Coding coding3 = new Coding(LOINC_URL, "10555-1", null);
    final Coding coding4 = new Coding(LOINC_URL, "10665-8",
        "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
    final Coding coding5 = new Coding(SNOMED_URL, "416399002",
        "Procollagen type I amino-terminal propeptide level");

    final CodeableConcept codeableConcept1 = new CodeableConcept(coding1);
    codeableConcept1.addCoding(coding5);
    final CodeableConcept codeableConcept2 = new CodeableConcept(coding2);
    final CodeableConcept codeableConcept3 = new CodeableConcept(coding3);
    final CodeableConcept codeableConcept4 = new CodeableConcept(coding3);
    final CodeableConcept codeableConcept5 = new CodeableConcept(coding4);
    final CodeableConcept codeableConcept6 = new CodeableConcept(coding1);

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("DiagnosticReport", "code");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(codeableConceptStructType())
        .withRow("DiagnosticReport/1", rowFromCodeableConcept(codeableConcept1))
        .withRow("DiagnosticReport/2", rowFromCodeableConcept(codeableConcept2))
        .withRow("DiagnosticReport/3", rowFromCodeableConcept(codeableConcept3))
        .withRow("DiagnosticReport/4", rowFromCodeableConcept(codeableConcept4))
        .withRow("DiagnosticReport/5", rowFromCodeableConcept(codeableConcept5))
        .withRow("DiagnosticReport/6", rowFromCodeableConcept(codeableConcept6))
        .buildWithStructValue();
    final ElementPath inputExpression = new ElementPathBuilder()
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("DiagnosticReport.code")
        .singular(true)
        .definition(definition)
        .buildDefined();

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + MY_VALUE_SET_URL + "'", inputExpression);

    // Create a mock terminology client.
    final TerminologyClient terminologyClient = mock(TerminologyClient.class);
    final Answer<Bundle> validateCodeableConceptTxAnswerer = new ValidateCodeableConceptTxAnswerer(
        codeableConcept1, codeableConcept3, codeableConcept4);
    when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
    when(terminologyClient.batch(any(Bundle.class))).thenAnswer(validateCodeableConceptTxAnswerer);

    // Create a mock TerminologyClientFactory, and make it return the mock terminology client.
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Create a mock ValidateCodeMapper.
    final ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
    final Answer<Iterator<ValidateCodeResult>> validateCodeMapperAnswerer = new ValidateCodeMapperAnswerer(
        true,
        false, true, true, false, false);
    //noinspection unchecked
    when(mockCodeMapper.call(any(Iterator.class))).thenAnswer(validateCodeMapperAnswerer);

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder()
        .terminologyClient(terminologyClient)
        .terminologyClientFactory(terminologyClientFactory)
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentExpression));

    // Invoke the function.
    final FhirPath result = new MemberOfFunction(mockCodeMapper).invoke(memberOfInput);

    // Check the result.
    assertTrue(result instanceof BooleanPath);
    assertThat((BooleanPath) result)
        .hasExpression("DiagnosticReport.code.memberOf('" + MY_VALUE_SET_URL + "')")
        .isSingular()
        .hasFhirType(FHIRDefinedType.BOOLEAN);

    // Test the mapper.
    final ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper("xyz",
        terminologyClientFactory,
        MY_VALUE_SET_URL, FHIRDefinedType.CODEABLECONCEPT);
    final Row inputCodeableConceptRow1 = RowFactory
        .create(1, rowFromCodeableConcept(codeableConcept1));
    final Row inputCodeableConceptRow2 = RowFactory
        .create(2, rowFromCodeableConcept(codeableConcept2));
    final Row inputCodeableConceptRow3 = RowFactory
        .create(3, rowFromCodeableConcept(codeableConcept3));
    final Row inputCodeableConceptRow4 = RowFactory
        .create(4, rowFromCodeableConcept(codeableConcept4));
    final Row inputCodeableConceptRow5 = RowFactory
        .create(5, rowFromCodeableConcept(codeableConcept5));
    final Row inputCodeableConceptRow6 = RowFactory
        .create(6, rowFromCodeableConcept(codeableConcept6));
    final List<Row> inputCodeableConceptRows = Arrays
        .asList(inputCodeableConceptRow1, inputCodeableConceptRow2, inputCodeableConceptRow3,
            inputCodeableConceptRow4, inputCodeableConceptRow5, inputCodeableConceptRow6);
    final List<ValidateCodeResult> results = new ArrayList<>();
    validateCodeMapper.call(inputCodeableConceptRows.iterator()).forEachRemaining(results::add);

    // Check the result dataset.
    final List<ValidateCodeResult> expectedResults = Arrays.asList(
        new ValidateCodeResult(1, true),
        new ValidateCodeResult(2, false),
        new ValidateCodeResult(3, true),
        new ValidateCodeResult(4, true),
        new ValidateCodeResult(5, false),
        new ValidateCodeResult(6, true)
    );
    assertEquals(expectedResults, results);
  }


  @Test
  public void throwsErrorIfInputTypeIsUnsupported() {
    final FhirPath mockContext = mock(FhirPath.class);
    final FhirPath input = StringLiteralPath.fromString("some string", mockContext);
    final FhirPath argument = StringLiteralPath.fromString(MY_VALUE_SET_URL, mockContext);

    final ParserContext parserContext = new ParserContextBuilder()
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    assertThrows(InvalidUserInputError.class,
        () -> new MemberOfFunction().invoke(memberOfInput),
        "Input to memberOf function is of unsupported type: onsetString");
  }

  @Test
  public void throwsErrorIfArgumentIsNotString() {
    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final IntegerLiteralPath argument = IntegerLiteralPath
        .fromString("4", mock(FhirPath.class));

    final ParserContext context = new ParserContextBuilder().build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    assertThrows(InvalidUserInputError.class,
        () -> new MemberOfFunction().invoke(memberOfInput),
        "memberOf function accepts one argument of type String: memberOf(4)");
  }

  // @Test
  // public void throwsErrorIfMoreThanOneArgument() {
  //   ParsedExpression input = new ComplexNonLiteralPathBuilder(FHIRDefinedType.CODEABLECONCEPT)
  //       .build();
  //   ParsedExpression argument1 = PrimitiveNonLiteralPathBuilder.literalString("foo"),
  //       argument2 = PrimitiveNonLiteralPathBuilder.literalString("bar");
  //
  //   ParserContext parserContext = new ParserContext();
  //   parserContext.setTerminologyClientFactory(mock(TerminologyClientFactory.class));
  //
  //   FunctionInput memberOfInput = new FunctionInput();
  //   memberOfInput.setContext(parserContext);
  //   memberOfInput.setInput(input);
  //   memberOfInput.getArguments().add(argument1);
  //   memberOfInput.getArguments().add(argument2);
  //   memberOfInput.setExpression("memberOf('foo', 'bar')");
  //
  //   MemberOfFunction memberOfFunction = new MemberOfFunction();
  //   assertThatExceptionOfType(InvalidRequestException.class)
  //       .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
  //       .withMessage(
  //           "memberOf function accepts one argument of type String: memberOf('foo', 'bar')");
  // }
  //
  // @Test
  // public void throwsErrorIfTerminologyServiceNotConfigured() {
  //   ParsedExpression input = new ComplexNonLiteralPathBuilder(FHIRDefinedType.CODEABLECONCEPT)
  //       .build();
  //   ParsedExpression argument = PrimitiveNonLiteralPathBuilder.literalString("foo");
  //
  //   ParserContext parserContext = new ParserContext();
  //
  //   FunctionInput memberOfInput = new FunctionInput();
  //   memberOfInput.setContext(parserContext);
  //   memberOfInput.setInput(input);
  //   memberOfInput.getArguments().add(argument);
  //   memberOfInput.setExpression("memberOf('foo')");
  //
  //   MemberOfFunction memberOfFunction = new MemberOfFunction();
  //   assertThatExceptionOfType(InvalidRequestException.class)
  //       .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
  //       .withMessage(
  //           "Attempt to call terminology function memberOf when no terminology service is configured");
  // }

}