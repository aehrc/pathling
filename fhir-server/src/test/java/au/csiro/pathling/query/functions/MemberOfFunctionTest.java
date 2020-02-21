/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.*;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.TestUtilities.*;
import au.csiro.pathling.encoding.ValidateCodeResult;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.functions.MemberOfFunction.ValidateCodeMapper;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.ComplexExpressionBuilder;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.assertj.core.api.Assertions;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class MemberOfFunctionTest {

  private static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";
  private static final String TERMINOLOGY_SERVICE_URL = "https://r4.ontoserver.csiro.au/fhir";

  @Test
  public void memberOfCoding() throws Exception {
    Coding coding1 = new Coding(MY_VALUE_SET_URL, "AMB", "ambulatory");
    Coding coding2 = new Coding(MY_VALUE_SET_URL, "EMER", null);
    Coding coding3 = new Coding(MY_VALUE_SET_URL, "IMP", "inpatient encounter");
    Coding coding4 = new Coding(MY_VALUE_SET_URL, "IMP", null);
    Coding coding5 = new Coding(MY_VALUE_SET_URL, "ACUTE", "inpatient acute");

    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODING)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructTypeColumns(codingStructType())
        .withRow("Encounter/xyz1", rowFromCoding(coding1))
        .withRow("Encounter/xyz2", rowFromCoding(coding2))
        .withRow("Encounter/xyz3", rowFromCoding(coding3))
        .withRow("Encounter/xyz4", rowFromCoding(coding4))
        .withRow("Encounter/xyz5", rowFromCoding(coding5))
        .buildWithStructValue("789wxyz");
    inputExpression.setSingular(true);
    ParsedExpression argumentExpression = PrimitiveExpressionBuilder
        .literalString(MY_VALUE_SET_URL);

    // Create a mock terminology client.
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    ValidateCodingTxAnswerer validateCodingTxAnswerer = new ValidateCodingTxAnswerer(
        coding2, coding5);
    when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
    when(terminologyClient.batch(any(Bundle.class))).thenAnswer(validateCodingTxAnswerer);

    // Create a mock TerminologyClientFactory, and make it return the mock terminology client.
    TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Create a mock ValidateCodeMapper.
    ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
    ValidateCodeMapperAnswerer validateCodeMapperAnswerer = new ValidateCodeMapperAnswerer(false,
        true, false, false, true);
    //noinspection unchecked
    when(mockCodeMapper.call(any(Iterator.class))).thenAnswer(validateCodeMapperAnswerer);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setTerminologyClient(terminologyClient);
    parserContext.setTerminologyClientFactory(terminologyClientFactory);

    FunctionInput memberOfInput = new FunctionInput();
    String inputFhirPath = "memberOf('" + MY_VALUE_SET_URL + "')";
    memberOfInput.setExpression(inputFhirPath);
    memberOfInput.setContext(parserContext);
    memberOfInput.setInput(inputExpression);
    memberOfInput.getArguments().add(argumentExpression);

    // Invoke the function.
    MemberOfFunction memberOfFunction = new MemberOfFunction(mockCodeMapper);
    ParsedExpression result = memberOfFunction.invoke(memberOfInput);

    // Check the result.
    assertThat(result).hasFhirPath(inputFhirPath);
    assertThat(result).isOfType(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN);
    assertThat(result).isPrimitive();
    assertThat(result).isSingular();
    assertThat(result).isSelection();

    // Test the mapper.
    ValidateCodeMapper validateCodingMapper = new ValidateCodeMapper("xyz",
        terminologyClientFactory,
        MY_VALUE_SET_URL, FHIRDefinedType.CODING);
    Row inputCodingRow1 = RowFactory.create(1, rowFromCoding(coding1));
    Row inputCodingRow2 = RowFactory.create(2, rowFromCoding(coding2));
    Row inputCodingRow3 = RowFactory.create(3, rowFromCoding(coding3));
    Row inputCodingRow4 = RowFactory.create(4, rowFromCoding(coding4));
    Row inputCodingRow5 = RowFactory.create(5, rowFromCoding(coding5));
    List<Row> inputCodingRows = Arrays
        .asList(inputCodingRow1, inputCodingRow2, inputCodingRow3, inputCodingRow4,
            inputCodingRow5);
    List<ValidateCodeResult> results = new ArrayList<>();
    validateCodingMapper.call(inputCodingRows.iterator()).forEachRemaining(results::add);

    // Check the result dataset.
    Assertions.assertThat(results.size()).isEqualTo(5);
    ValidateCodeResult validateResult = results.get(0);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(1);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(1);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(2);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(2);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(3);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(3);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(4);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(4);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(5);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
  }

  @Test
  public void memberOfCodeableConcept() {
    Coding coding1 = new Coding(LOINC_URL, "10337-4", "Procollagen type I [Mass/volume] in Serum");
    Coding coding2 = new Coding(LOINC_URL, "10428-1",
        "Varicella zoster virus immune globulin given [Volume]");
    Coding coding3 = new Coding(LOINC_URL, "10555-1", null);
    Coding coding4 = new Coding(LOINC_URL, "10665-8",
        "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
    Coding coding5 = new Coding(SNOMED_URL, "416399002",
        "Procollagen type I amino-terminal propeptide level");

    CodeableConcept codeableConcept1 = new CodeableConcept(coding1);
    codeableConcept1.addCoding(coding5);
    CodeableConcept codeableConcept2 = new CodeableConcept(coding2);
    CodeableConcept codeableConcept3 = new CodeableConcept(coding3);
    CodeableConcept codeableConcept4 = new CodeableConcept(coding3);
    CodeableConcept codeableConcept5 = new CodeableConcept(coding4);
    CodeableConcept codeableConcept6 = new CodeableConcept(coding1);

    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructTypeColumns(codeableConceptStructType())
        .withRow("DiagnosticReport/xyz1", rowFromCodeableConcept(codeableConcept1))
        .withRow("DiagnosticReport/xyz2", rowFromCodeableConcept(codeableConcept2))
        .withRow("DiagnosticReport/xyz3", rowFromCodeableConcept(codeableConcept3))
        .withRow("DiagnosticReport/xyz4", rowFromCodeableConcept(codeableConcept4))
        .withRow("DiagnosticReport/xyz5", rowFromCodeableConcept(codeableConcept5))
        .withRow("DiagnosticReport/xyz6", rowFromCodeableConcept(codeableConcept6))
        .buildWithStructValue("789wxyz");
    inputExpression.setSingular(true);
    BaseRuntimeChildDefinition definition = TestUtilities.getFhirContext()
        .getResourceDefinition("DiagnosticReport")
        .getChildByName("code");
    inputExpression.setDefinition(definition, "code");
    ParsedExpression argumentExpression = PrimitiveExpressionBuilder
        .literalString(MY_VALUE_SET_URL);

    // Create a mock terminology client.
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    ValidateCodeableConceptTxAnswerer validateCodeableConceptTxAnswerer = new ValidateCodeableConceptTxAnswerer(
        codeableConcept1, codeableConcept3, codeableConcept4);
    when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
    when(terminologyClient.batch(any(Bundle.class))).thenAnswer(validateCodeableConceptTxAnswerer);

    // Create a mock TerminologyClientFactory, and make it return the mock terminology client.
    TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Create a mock ValidateCodeMapper.
    ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
    ValidateCodeMapperAnswerer validateCodeMapperAnswerer = new ValidateCodeMapperAnswerer(true,
        false, true, true, false, false);
    //noinspection unchecked
    when(mockCodeMapper.call(any(Iterator.class))).thenAnswer(validateCodeMapperAnswerer);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setTerminologyClient(terminologyClient);
    parserContext.setTerminologyClientFactory(terminologyClientFactory);

    FunctionInput memberOfInput = new FunctionInput();
    String inputFhirPath = "memberOf('" + MY_VALUE_SET_URL + "')";
    memberOfInput.setExpression(inputFhirPath);
    memberOfInput.setContext(parserContext);
    memberOfInput.setInput(inputExpression);
    memberOfInput.getArguments().add(argumentExpression);

    // Invoke the function.
    MemberOfFunction memberOfFunction = new MemberOfFunction(mockCodeMapper);
    ParsedExpression result = memberOfFunction.invoke(memberOfInput);

    // Check the result.
    assertThat(result).hasFhirPath(inputFhirPath);
    assertThat(result).isOfType(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN);
    assertThat(result).isPrimitive();
    assertThat(result).isSingular();
    assertThat(result).isSelection();

    // Test the mapper.
    ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper("xyz",
        terminologyClientFactory,
        MY_VALUE_SET_URL, FHIRDefinedType.CODEABLECONCEPT);
    Row inputCodeableConceptRow1 = RowFactory.create(1, rowFromCodeableConcept(codeableConcept1));
    Row inputCodeableConceptRow2 = RowFactory.create(2, rowFromCodeableConcept(codeableConcept2));
    Row inputCodeableConceptRow3 = RowFactory.create(3, rowFromCodeableConcept(codeableConcept3));
    Row inputCodeableConceptRow4 = RowFactory.create(4, rowFromCodeableConcept(codeableConcept4));
    Row inputCodeableConceptRow5 = RowFactory.create(5, rowFromCodeableConcept(codeableConcept5));
    Row inputCodeableConceptRow6 = RowFactory.create(5, rowFromCodeableConcept(codeableConcept6));
    List<Row> inputCodeableConceptRows = Arrays
        .asList(inputCodeableConceptRow1, inputCodeableConceptRow2, inputCodeableConceptRow3,
            inputCodeableConceptRow4, inputCodeableConceptRow5, inputCodeableConceptRow6);
    List<ValidateCodeResult> results = new ArrayList<>();
    validateCodeMapper.call(inputCodeableConceptRows.iterator()).forEachRemaining(results::add);

    // Check the result dataset.
    Assertions.assertThat(results.size()).isEqualTo(6);
    ValidateCodeResult validateResult = results.get(0);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(1);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(1);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(2);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(2);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(3);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(3);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(4);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(4);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(5);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(4);
    Assertions.assertThat(validateResult.getHash()).isEqualTo(5);
    Assertions.assertThat(validateResult.isResult()).isEqualTo(false);
  }

  @Test
  public void throwsErrorIfInputTypeIsUnsupported() {
    ParsedExpression input = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .build();
    input.setFhirPath("onsetString");
    ParsedExpression argument = PrimitiveExpressionBuilder.literalString(MY_VALUE_SET_URL);

    FunctionInput memberOfInput = new FunctionInput();
    memberOfInput.setInput(input);
    memberOfInput.getArguments().add(argument);

    MemberOfFunction memberOfFunction = new MemberOfFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
        .withMessage("Input to memberOf function is of unsupported type: onsetString");
  }

  @Test
  public void throwsErrorIfArgumentIsNotString() {
    ParsedExpression input = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    ParsedExpression argument = PrimitiveExpressionBuilder.literalInteger(4);

    FunctionInput memberOfInput = new FunctionInput();
    memberOfInput.setInput(input);
    memberOfInput.getArguments().add(argument);
    memberOfInput.setExpression("memberOf(4)");

    MemberOfFunction memberOfFunction = new MemberOfFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
        .withMessage("memberOf function accepts one argument of type String: memberOf(4)");
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    ParsedExpression input = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    ParsedExpression argument1 = PrimitiveExpressionBuilder.literalString("foo"),
        argument2 = PrimitiveExpressionBuilder.literalString("bar");

    FunctionInput memberOfInput = new FunctionInput();
    memberOfInput.setInput(input);
    memberOfInput.getArguments().add(argument1);
    memberOfInput.getArguments().add(argument2);
    memberOfInput.setExpression("memberOf('foo', 'bar')");

    MemberOfFunction memberOfFunction = new MemberOfFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
        .withMessage(
            "memberOf function accepts one argument of type String: memberOf('foo', 'bar')");
  }

}