package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.TestUtilities.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.FhirContextFactory;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.IdAndBoolean;
import au.csiro.clinsight.query.functions.MemberOfFunction.ValidateCodeMapper;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class MemberOfFunctionTest {

  private SparkSession spark;
  private Parameters positiveResponse, negativeResponse;
  private final String loincUrl = "http://loinc.org";
  private final String snomedUrl = "http://snomed.info/sct";
  private final String myValueSetUrl = "https://csiro.au/fhir/ValueSet/my-value-set";
  private final String terminologyServiceUrl = "https://r4.ontoserver.csiro.au/fhir";

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .config("spark.driver.host", "localhost")
        .getOrCreate();
    IParser jsonParser = getJsonParser();
    positiveResponse = (Parameters) jsonParser.parseResource(getResourceAsStream(
        "txResponses/MemberOfFunctionTest-memberOfCoding-validate-code-positive.Parameters.json"));
    negativeResponse = (Parameters) jsonParser.parseResource(getResourceAsStream(
        "txResponses/MemberOfFunctionTest-memberOfCoding-validate-code-negative.Parameters.json"));
  }

  @Test
  public void memberOfCoding() {
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

    // Create a mock terminology client which returns validate code responses.
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCoding(coding1)))
        .thenReturn(negativeResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCoding(coding2)))
        .thenReturn(positiveResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCoding(coding3)))
        .thenReturn(negativeResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCoding(coding4)))
        .thenReturn(negativeResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCoding(coding5)))
        .thenReturn(positiveResponse);
    when(terminologyClient.getServerBase()).thenReturn(terminologyServiceUrl);

    // Create a mock FhirContextFactory, and make it return the mock terminology client.
    FhirContext fhirContext = mock(FhirContext.class);
    when(fhirContext
        .newRestfulClient(TerminologyClient.class, terminologyServiceUrl))
        .thenReturn(terminologyClient);
    FhirContextFactory fhirContextFactory = mock(FhirContextFactory.class);
    when(fhirContextFactory.getFhirContext(FhirVersionEnum.R4)).thenReturn(fhirContext);

    // Create a mock ValidateCodeMapper.
    ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
    IdAndBoolean individualMapResult = new IdAndBoolean();
    individualMapResult.setId("foo");
    when(mockCodeMapper.call(any(Row.class))).thenReturn(individualMapResult);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setTerminologyClient(terminologyClient);
    parserContext.setFhirContextFactory(fhirContextFactory);

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
    ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper(fhirContextFactory,
        terminologyServiceUrl, FhirPathType.CODING, myValueSetUrl);
    List<IdAndBoolean> results = inputDataset.collectAsList().stream()
        .map(validateCodeMapper::call)
        .collect(Collectors.toList());

    // Check the result dataset.
    assertThat(results.size()).isEqualTo(5);
    IdAndBoolean idAndBoolean = results.get(0);
    assertThat(idAndBoolean.getId()).isEqualTo("Encounter/xyz1");
    assertThat(idAndBoolean.isValue()).isEqualTo(false);
    idAndBoolean = results.get(1);
    assertThat(idAndBoolean.getId()).isEqualTo("Encounter/xyz2");
    assertThat(idAndBoolean.isValue()).isEqualTo(true);
    idAndBoolean = results.get(2);
    assertThat(idAndBoolean.getId()).isEqualTo("Encounter/xyz3");
    assertThat(idAndBoolean.isValue()).isEqualTo(false);
    idAndBoolean = results.get(3);
    assertThat(idAndBoolean.getId()).isEqualTo("Encounter/xyz4");
    assertThat(idAndBoolean.isValue()).isEqualTo(false);
    idAndBoolean = results.get(4);
    assertThat(idAndBoolean.getId()).isEqualTo("Encounter/xyz5");
    assertThat(idAndBoolean.isValue()).isEqualTo(true);
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

    // Create a mock terminology client which returns validate code responses.
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCodeableConcept(codeableConcept1)))
        .thenReturn(positiveResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCodeableConcept(codeableConcept2)))
        .thenReturn(negativeResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCodeableConcept(codeableConcept3)))
        .thenReturn(positiveResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCodeableConcept(codeableConcept4)))
        .thenReturn(positiveResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCodeableConcept(codeableConcept5)))
        .thenReturn(negativeResponse);
    when(terminologyClient
        .validateCode(matchesUri(myValueSetUrl), matchesCodeableConcept(codeableConcept6)))
        .thenReturn(negativeResponse);
    when(terminologyClient.getServerBase()).thenReturn(terminologyServiceUrl);

    // Create a mock FhirContextFactory, and make it return the mock terminology client.
    FhirContext fhirContext = mock(FhirContext.class);
    when(fhirContext
        .newRestfulClient(TerminologyClient.class, terminologyServiceUrl))
        .thenReturn(terminologyClient);
    FhirContextFactory fhirContextFactory = mock(FhirContextFactory.class);
    when(fhirContextFactory.getFhirContext(FhirVersionEnum.R4)).thenReturn(fhirContext);

    // Create a mock ValidateCodeMapper.
    ValidateCodeMapper mockCodeMapper = mock(ValidateCodeMapper.class);
    IdAndBoolean individualMapResult = new IdAndBoolean();
    individualMapResult.setId("foo");
    when(mockCodeMapper.call(any(Row.class))).thenReturn(individualMapResult);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setTerminologyClient(terminologyClient);
    parserContext.setFhirContextFactory(fhirContextFactory);

    // Prepare the input expression.
    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("code");
    inputExpression.setSingular(true);
    inputExpression.setDataset(inputDataset);
    inputExpression.setIdColumn(idColumn);
    inputExpression.setValueColumn(valueColumn);
    BaseRuntimeChildDefinition definition = TestUtilities.getFhirContext()
        .getResourceDefinition("DiagnosticReport")
        .getChildByName("code");
    inputExpression.setDefinition(definition);

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
    ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper(fhirContextFactory,
        terminologyServiceUrl, null, myValueSetUrl);
    List<IdAndBoolean> results = inputDataset.collectAsList().stream()
        .map(validateCodeMapper::call)
        .collect(Collectors.toList());

    // Check the result dataset.
    assertThat(results.size()).isEqualTo(6);
    IdAndBoolean idAndBoolean = results.get(0);
    assertThat(idAndBoolean.getId()).isEqualTo("DiagnosticReport/xyz1");
    assertThat(idAndBoolean.isValue()).isEqualTo(true);
    idAndBoolean = results.get(1);
    assertThat(idAndBoolean.getId()).isEqualTo("DiagnosticReport/xyz2");
    assertThat(idAndBoolean.isValue()).isEqualTo(false);
    idAndBoolean = results.get(2);
    assertThat(idAndBoolean.getId()).isEqualTo("DiagnosticReport/xyz3");
    assertThat(idAndBoolean.isValue()).isEqualTo(true);
    idAndBoolean = results.get(3);
    assertThat(idAndBoolean.getId()).isEqualTo("DiagnosticReport/xyz4");
    assertThat(idAndBoolean.isValue()).isEqualTo(true);
    idAndBoolean = results.get(4);
    assertThat(idAndBoolean.getId()).isEqualTo("DiagnosticReport/xyz5");
    assertThat(idAndBoolean.isValue()).isEqualTo(false);
    idAndBoolean = results.get(5);
    assertThat(idAndBoolean.getId()).isEqualTo("DiagnosticReport/xyz6");
    assertThat(idAndBoolean.isValue()).isEqualTo(false);
  }

  @Test
  public void mapperCanHandleNullValue() {
    // Create a mock FhirContextFactory, and make it return the mock terminology client.
    FhirContext fhirContext = mock(FhirContext.class);
    when(fhirContext
        .newRestfulClient(TerminologyClient.class, terminologyServiceUrl))
        .thenReturn(mock(TerminologyClient.class));
    FhirContextFactory fhirContextFactory = mock(FhirContextFactory.class);
    when(fhirContextFactory.getFhirContext(FhirVersionEnum.R4)).thenReturn(fhirContext);

    // Test the mapper.
    ValidateCodeMapper validateCodeMapper = new ValidateCodeMapper(fhirContextFactory,
        terminologyServiceUrl, null, myValueSetUrl);
    String id = "Patient/abc123";
    Row inputRow = RowFactory.create(id, null);
    IdAndBoolean result = validateCodeMapper.call(inputRow);

    assertThat(result.getId()).isEqualTo(id);
    assertThat(result.isValue()).isFalse();
  }
}