/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.TestUtilities.*;
import au.csiro.pathling.encoding.ValidateCodeResult;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.functions.MemberOfFunction.ValidateCodeMapper;
import au.csiro.pathling.query.parsing.ExpressionParserContext;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.parser.IParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class MemberOfFunctionTest {

  private SparkSession spark;
  private final String loincUrl = "http://loinc.org";
  private final String snomedUrl = "http://snomed.info/sct";
  private final String myValueSetUrl = "https://csiro.au/fhir/ValueSet/my-value-set";
  private final String terminologyServiceUrl = "https://r4.ontoserver.csiro.au/fhir";

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .appName("pathling-test")
        .config("spark.master", "local")
        .config("spark.driver.host", "localhost")
        .getOrCreate();
    IParser jsonParser = getJsonParser();
  }

  @Test
  public void memberOfCoding() throws Exception {
    // Build a dataset which represents the input to the function.
    Metadata metadata = new MetadataBuilder().build();
    StructField inputId = new StructField("789wxyz_id", DataTypes.StringType, false,
        metadata);
    StructField inputValue = new StructField("789wxyz", codingStructType(), true,
        metadata);
    StructType inputStruct = new StructType(new StructField[]{inputId, inputValue});

    Coding coding1 = new Coding(myValueSetUrl, "AMB", "ambulatory");
    Coding coding2 = new Coding(myValueSetUrl, "EMER", null);
    Coding coding3 = new Coding(myValueSetUrl, "IMP", "inpatient encounter");
    Coding coding4 = new Coding(myValueSetUrl, "IMP", null);
    Coding coding5 = new Coding(myValueSetUrl, "ACUTE", "inpatient acute");

    Row inputRow1 = RowFactory.create("Encounter/xyz1", rowFromCoding(coding1));
    Row inputRow2 = RowFactory.create("Encounter/xyz2", rowFromCoding(coding2));
    Row inputRow3 = RowFactory.create("Encounter/xyz3", rowFromCoding(coding3));
    Row inputRow4 = RowFactory.create("Encounter/xyz4", rowFromCoding(coding4));
    Row inputRow5 = RowFactory.create("Encounter/xyz5", rowFromCoding(coding5));
    Dataset<Row> inputDataset = spark
        .createDataFrame(Arrays.asList(inputRow1, inputRow2, inputRow3, inputRow4, inputRow5),
            inputStruct);
    Column idColumn = inputDataset.col(inputDataset.columns()[0]);
    Column valueColumn = inputDataset.col(inputDataset.columns()[1]);

    // Create a mock terminology client.
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    ValidateCodingTxAnswerer validateCodingTxAnswerer = new ValidateCodingTxAnswerer(
        coding2, coding5);
    when(terminologyClient.getServerBase()).thenReturn(terminologyServiceUrl);
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

    // Prepare the input expression.
    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("class");
    inputExpression.setFhirPathType(FhirPathType.CODING);
    inputExpression.setSingular(true);
    inputExpression.setDataset(inputDataset);
    inputExpression.setIdColumn(idColumn);
    inputExpression.setValueColumn(valueColumn);

    // Prepare the argument expression.
    ParsedExpression argumentExpression = new ParsedExpression();
    argumentExpression.setFhirPath("'" + myValueSetUrl + "'");
    argumentExpression.setFhirPathType(FhirPathType.STRING);
    argumentExpression.setLiteralValue(new org.hl7.fhir.r4.model.StringType(myValueSetUrl));
    argumentExpression.setPrimitive(true);
    argumentExpression.setSingular(true);

    FunctionInput memberOfInput = new FunctionInput();
    String inputFhirPath = "memberOf('" + myValueSetUrl + "')";
    memberOfInput.setExpression(inputFhirPath);
    memberOfInput.setContext(parserContext);
    memberOfInput.setInput(inputExpression);
    memberOfInput.getArguments().add(argumentExpression);

    // Invoke the function.
    MemberOfFunction memberOfFunction = new MemberOfFunction(mockCodeMapper);
    ParsedExpression result = memberOfFunction.invoke(memberOfInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo(inputFhirPath);
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.BOOLEAN);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.BOOLEAN);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();
    assertThat(result.getIdColumn()).isInstanceOf(Column.class);
    assertThat(result.getValueColumn()).isInstanceOf(Column.class);

    // Test the mapper.
    ValidateCodeMapper validateCodingMapper = new ValidateCodeMapper("xyz",
        terminologyClientFactory,
        myValueSetUrl, FHIRDefinedType.CODING);
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
    assertThat(results.size()).isEqualTo(5);
    ValidateCodeResult validateResult = results.get(0);
    assertThat(validateResult.getHash()).isEqualTo(1);
    assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(1);
    assertThat(validateResult.getHash()).isEqualTo(2);
    assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(2);
    assertThat(validateResult.getHash()).isEqualTo(3);
    assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(3);
    assertThat(validateResult.getHash()).isEqualTo(4);
    assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(4);
    assertThat(validateResult.getHash()).isEqualTo(5);
    assertThat(validateResult.isResult()).isEqualTo(true);
  }

  @Test
  public void memberOfCodeableConcept() {
    // Build a dataset which represents the input to the function.
    Metadata metadata = new MetadataBuilder().build();
    StructField inputId = new StructField("789wxyz_id", DataTypes.StringType, false,
        metadata);
    StructField inputValue = new StructField("789wxyz", codeableConceptStructType(), true,
        metadata);
    StructType inputStruct = new StructType(new StructField[]{inputId, inputValue});

    Coding coding1 = new Coding(loincUrl, "10337-4", "Procollagen type I [Mass/volume] in Serum");
    Coding coding2 = new Coding(loincUrl, "10428-1",
        "Varicella zoster virus immune globulin given [Volume]");
    Coding coding3 = new Coding(loincUrl, "10555-1", null);
    Coding coding4 = new Coding(loincUrl, "10665-8",
        "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
    Coding coding5 = new Coding(snomedUrl, "416399002",
        "Procollagen type I amino-terminal propeptide level");

    CodeableConcept codeableConcept1 = new CodeableConcept(coding1);
    codeableConcept1.addCoding(coding5);
    CodeableConcept codeableConcept2 = new CodeableConcept(coding2);
    CodeableConcept codeableConcept3 = new CodeableConcept(coding3);
    CodeableConcept codeableConcept4 = new CodeableConcept(coding3);
    CodeableConcept codeableConcept5 = new CodeableConcept(coding4);
    CodeableConcept codeableConcept6 = new CodeableConcept(coding1);

    Row inputRow1 = RowFactory
        .create("DiagnosticReport/xyz1", rowFromCodeableConcept(codeableConcept1));
    Row inputRow2 = RowFactory
        .create("DiagnosticReport/xyz2", rowFromCodeableConcept(codeableConcept2));
    Row inputRow3 = RowFactory
        .create("DiagnosticReport/xyz3", rowFromCodeableConcept(codeableConcept3));
    Row inputRow4 = RowFactory
        .create("DiagnosticReport/xyz4", rowFromCodeableConcept(codeableConcept4));
    Row inputRow5 = RowFactory
        .create("DiagnosticReport/xyz5", rowFromCodeableConcept(codeableConcept5));
    Row inputRow6 = RowFactory
        .create("DiagnosticReport/xyz6", rowFromCodeableConcept(codeableConcept6));
    Dataset<Row> inputDataset = spark.createDataFrame(
        Arrays.asList(inputRow1, inputRow2, inputRow3, inputRow4, inputRow5, inputRow6),
        inputStruct);
    Column idColumn = inputDataset.col(inputDataset.columns()[0]);
    Column valueColumn = inputDataset.col(inputDataset.columns()[1]);

    // Create a mock terminology client.
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    ValidateCodeableConceptTxAnswerer validateCodeableConceptTxAnswerer = new ValidateCodeableConceptTxAnswerer(
        codeableConcept1, codeableConcept3, codeableConcept4);
    when(terminologyClient.getServerBase()).thenReturn(terminologyServiceUrl);
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

    // Prepare the input expression.
    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("code");
    inputExpression.setFhirType(FHIRDefinedType.CODEABLECONCEPT);
    inputExpression.setSingular(true);
    inputExpression.setDataset(inputDataset);
    inputExpression.setIdColumn(idColumn);
    inputExpression.setValueColumn(valueColumn);
    BaseRuntimeChildDefinition definition = TestUtilities.getFhirContext()
        .getResourceDefinition("DiagnosticReport")
        .getChildByName("code");
    inputExpression.setDefinition(definition, "code");

    // Prepare the argument expression.
    ParsedExpression argumentExpression = new ParsedExpression();
    argumentExpression.setFhirPath("'" + myValueSetUrl + "'");
    argumentExpression.setFhirPathType(FhirPathType.STRING);
    argumentExpression.setLiteralValue(new StringType(myValueSetUrl));
    argumentExpression.setPrimitive(true);
    argumentExpression.setSingular(true);

    FunctionInput memberOfInput = new FunctionInput();
    String inputFhirPath = "memberOf('" + myValueSetUrl + "')";
    memberOfInput.setExpression(inputFhirPath);
    memberOfInput.setContext(parserContext);
    memberOfInput.setInput(inputExpression);
    memberOfInput.getArguments().add(argumentExpression);

    // Invoke the function.
    MemberOfFunction memberOfFunction = new MemberOfFunction(mockCodeMapper);
    ParsedExpression result = memberOfFunction.invoke(memberOfInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo(inputFhirPath);
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.BOOLEAN);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.BOOLEAN);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();
    assertThat(result.getIdColumn()).isInstanceOf(Column.class);
    assertThat(result.getValueColumn()).isInstanceOf(Column.class);

    // Test the mapper.
    ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper("xyz",
        terminologyClientFactory,
        myValueSetUrl, FHIRDefinedType.CODEABLECONCEPT);
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
    assertThat(results.size()).isEqualTo(6);
    ValidateCodeResult validateResult = results.get(0);
    assertThat(validateResult.getHash()).isEqualTo(1);
    assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(1);
    assertThat(validateResult.getHash()).isEqualTo(2);
    assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(2);
    assertThat(validateResult.getHash()).isEqualTo(3);
    assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(3);
    assertThat(validateResult.getHash()).isEqualTo(4);
    assertThat(validateResult.isResult()).isEqualTo(true);
    validateResult = results.get(4);
    assertThat(validateResult.getHash()).isEqualTo(5);
    assertThat(validateResult.isResult()).isEqualTo(false);
    validateResult = results.get(4);
    assertThat(validateResult.getHash()).isEqualTo(5);
    assertThat(validateResult.isResult()).isEqualTo(false);
  }

}