/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import static au.csiro.pathling.test.assertions.Assertions.assertEquals;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.assertions.Assertions.assertTrue;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.TestElementPath;
import au.csiro.pathling.test.TestParserContext;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.FhirHelpers.ValidateCodeMapperAnswerer;
import au.csiro.pathling.test.helpers.FhirHelpers.ValidateCodingTxAnswerer;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
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
    final CodingPath inputExpression = (CodingPath) TestElementPath
        .build("Encounter.class", inputDataset, true, definition);

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
    final ParserContext parserContext = TestParserContext.builder()
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
    assertEquals(results, expectedResults);
  }

  // @Test
  // public void memberOfCodeableConcept() {
  //   Coding coding1 = new Coding(LOINC_URL, "10337-4", "Procollagen type I [Mass/volume] in Serum");
  //   Coding coding2 = new Coding(LOINC_URL, "10428-1",
  //       "Varicella zoster virus immune globulin given [Volume]");
  //   Coding coding3 = new Coding(LOINC_URL, "10555-1", null);
  //   Coding coding4 = new Coding(LOINC_URL, "10665-8",
  //       "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
  //   Coding coding5 = new Coding(SNOMED_URL, "416399002",
  //       "Procollagen type I amino-terminal propeptide level");
  //
  //   CodeableConcept codeableConcept1 = new CodeableConcept(coding1);
  //   codeableConcept1.addCoding(coding5);
  //   CodeableConcept codeableConcept2 = new CodeableConcept(coding2);
  //   CodeableConcept codeableConcept3 = new CodeableConcept(coding3);
  //   CodeableConcept codeableConcept4 = new CodeableConcept(coding3);
  //   CodeableConcept codeableConcept5 = new CodeableConcept(coding4);
  //   CodeableConcept codeableConcept6 = new CodeableConcept(coding1);
  //
  //   ParsedExpression inputExpression = new ComplexNonLiteralPathBuilder(
  //       FHIRDefinedType.CODEABLECONCEPT)
  //       .withColumn("789wxyz_id", DataTypes.StringType)
  //       .withStructTypeColumns(SparkHelpers.codeableConceptStructType())
  //       .withRow("DiagnosticReport/xyz1", SparkHelpers.rowFromCodeableConcept(codeableConcept1))
  //       .withRow("DiagnosticReport/xyz2", SparkHelpers.rowFromCodeableConcept(codeableConcept2))
  //       .withRow("DiagnosticReport/xyz3", SparkHelpers.rowFromCodeableConcept(codeableConcept3))
  //       .withRow("DiagnosticReport/xyz4", SparkHelpers.rowFromCodeableConcept(codeableConcept4))
  //       .withRow("DiagnosticReport/xyz5", SparkHelpers.rowFromCodeableConcept(codeableConcept5))
  //       .withRow("DiagnosticReport/xyz6", SparkHelpers.rowFromCodeableConcept(codeableConcept6))
  //       .buildWithStructValue("789wxyz");
  //   inputExpression.setSingular(true);
  //   BaseRuntimeChildDefinition definition = FhirHelpers.getFhirContext()
  //       .getResourceDefinition("DiagnosticReport")
  //       .getChildByName("code");
  //   inputExpression.setDefinition(definition, "code");
  //   ParsedExpression argumentExpression = PrimitiveNonLiteralPathBuilder
  //       .literalString(MY_VALUE_SET_URL);
  //
  //   // Create a mock terminology client.
  //   TerminologyClient terminologyClient = mock(TerminologyClient.class);
  //   ValidateCodeableConceptTxAnswerer validateCodeableConceptTxAnswerer = new ValidateCodeableConceptTxAnswerer(
  //       codeableConcept1, codeableConcept3, codeableConcept4);
  //   when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
  //   when(terminologyClient.batch(any(Bundle.class))).thenAnswer(validateCodeableConceptTxAnswerer);
  //
  //   // Create a mock TerminologyClientFactory, and make it return the mock terminology client.
  //   TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
  //   when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);
  //
  //   // Create a mock ValidateCodeMapper.
  //   ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
  //   ValidateCodeMapperAnswerer validateCodeMapperAnswerer = new ValidateCodeMapperAnswerer(true,
  //       false, true, true, false, false);
  //   //noinspection unchecked
  //   when(mockCodeMapper.call(any(Iterator.class))).thenAnswer(validateCodeMapperAnswerer);
  //
  //   // Prepare the inputs to the function.
  //   ParserContext parserContext = new ParserContext();
  //   parserContext.setTerminologyClient(terminologyClient);
  //   parserContext.setTerminologyClientFactory(terminologyClientFactory);
  //
  //   FunctionInput memberOfInput = new FunctionInput();
  //   String inputFhirPath = "memberOf('" + MY_VALUE_SET_URL + "')";
  //   memberOfInput.setExpression(inputFhirPath);
  //   memberOfInput.setContext(parserContext);
  //   memberOfInput.setInput(inputExpression);
  //   memberOfInput.getArguments().add(argumentExpression);
  //
  //   // Invoke the function.
  //   MemberOfFunction memberOfFunction = new MemberOfFunction(mockCodeMapper);
  //   ParsedExpression result = memberOfFunction.invoke(memberOfInput);
  //
  //   // Check the result.
  //   au.csiro.pathling.test.assertions.Assertions.assertThat(result).hasFhirPath(inputFhirPath);
  //   au.csiro.pathling.test.assertions.Assertions
  //       .assertThat(result).isOfType(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN);
  //   au.csiro.pathling.test.assertions.Assertions.assertThat(result).isPrimitive();
  //   au.csiro.pathling.test.assertions.Assertions.assertThat(result).isSingular();
  //   au.csiro.pathling.test.assertions.Assertions.assertThat(result).isSelection();
  //
  //   // Test the mapper.
  //   ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper("xyz",
  //       terminologyClientFactory,
  //       MY_VALUE_SET_URL, FHIRDefinedType.CODEABLECONCEPT);
  //   Row inputCodeableConceptRow1 = RowFactory
  //       .create(1, SparkHelpers.rowFromCodeableConcept(codeableConcept1));
  //   Row inputCodeableConceptRow2 = RowFactory
  //       .create(2, SparkHelpers.rowFromCodeableConcept(codeableConcept2));
  //   Row inputCodeableConceptRow3 = RowFactory
  //       .create(3, SparkHelpers.rowFromCodeableConcept(codeableConcept3));
  //   Row inputCodeableConceptRow4 = RowFactory
  //       .create(4, SparkHelpers.rowFromCodeableConcept(codeableConcept4));
  //   Row inputCodeableConceptRow5 = RowFactory
  //       .create(5, SparkHelpers.rowFromCodeableConcept(codeableConcept5));
  //   Row inputCodeableConceptRow6 = RowFactory
  //       .create(5, SparkHelpers.rowFromCodeableConcept(codeableConcept6));
  //   List<Row> inputCodeableConceptRows = Arrays
  //       .asList(inputCodeableConceptRow1, inputCodeableConceptRow2, inputCodeableConceptRow3,
  //           inputCodeableConceptRow4, inputCodeableConceptRow5, inputCodeableConceptRow6);
  //   List<ValidateCodeResult> results = new ArrayList<>();
  //   validateCodeMapper.call(inputCodeableConceptRows.iterator()).forEachRemaining(results::add);
  //
  //   // Check the result dataset.
  //   Assertions.assertThat(results.size()).isEqualTo(6);
  //   ValidateCodeResult validateResult = results.get(0);
  //   Assertions.assertThat(validateResult.getHash()).isEqualTo(1);
  //   Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
  //   validateResult = results.get(1);
  //   Assertions.assertThat(validateResult.getHash()).isEqualTo(2);
  //   Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
  //   validateResult = results.get(2);
  //   Assertions.assertThat(validateResult.getHash()).isEqualTo(3);
  //   Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
  //   validateResult = results.get(3);
  //   Assertions.assertThat(validateResult.getHash()).isEqualTo(4);
  //   Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
  //   validateResult = results.get(4);
  //   Assertions.assertThat(validateResult.getHash()).isEqualTo(5);
  //   Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
  //   validateResult = results.get(4);
  //   Assertions.assertThat(validateResult.getHash()).isEqualTo(5);
  //   Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
  // }
  //
  // @Test
  // public void throwsErrorIfInputTypeIsUnsupported() {
  //   ParsedExpression input = new PrimitiveNonLiteralPathBuilder(FHIRDefinedType.STRING,
  //       FhirPathType.STRING)
  //       .build();
  //   input.setFhirPath("onsetString");
  //   ParsedExpression argument = PrimitiveNonLiteralPathBuilder.literalString(MY_VALUE_SET_URL);
  //
  //   ParserContext parserContext = new ParserContext();
  //   parserContext.setTerminologyClientFactory(mock(TerminologyClientFactory.class));
  //
  //   FunctionInput memberOfInput = new FunctionInput();
  //   memberOfInput.setContext(parserContext);
  //   memberOfInput.setInput(input);
  //   memberOfInput.getArguments().add(argument);
  //
  //   MemberOfFunction memberOfFunction = new MemberOfFunction();
  //   assertThatExceptionOfType(InvalidRequestException.class)
  //       .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
  //       .withMessage("Input to memberOf function is of unsupported type: onsetString");
  // }
  //
  // @Test
  // public void throwsErrorIfArgumentIsNotString() {
  //   ParsedExpression input = new ComplexNonLiteralPathBuilder(FHIRDefinedType.CODEABLECONCEPT)
  //       .build();
  //   ParsedExpression argument = PrimitiveNonLiteralPathBuilder.literalInteger(4);
  //
  //   ParserContext parserContext = new ParserContext();
  //   parserContext.setTerminologyClientFactory(mock(TerminologyClientFactory.class));
  //
  //   FunctionInput memberOfInput = new FunctionInput();
  //   memberOfInput.setContext(parserContext);
  //   memberOfInput.setInput(input);
  //   memberOfInput.getArguments().add(argument);
  //   memberOfInput.setExpression("memberOf(4)");
  //
  //   MemberOfFunction memberOfFunction = new MemberOfFunction();
  //   assertThatExceptionOfType(InvalidRequestException.class)
  //       .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
  //       .withMessage("memberOf function accepts one argument of type String: memberOf(4)");
  // }
  //
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