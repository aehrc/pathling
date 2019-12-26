/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.parsing;

import static au.csiro.pathling.query.parsing.PatientListBuilder.*;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.fhir.FreshFhirContextFactory;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.ResourceReader;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.ParsedExpressionAssert;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
public class ExpressionParserTest {

  private SparkSession spark;
  private ResourceReader mockReader;
  private TerminologyClient terminologyClient;
  private String terminologyServiceUrl = "https://r4.ontoserver.csiro.au/fhir";
  private ExpressionParserContext parserContext;
  private ExpressionParser expressionParser;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder().appName("pathling-test").config("spark.master", "local")
        .config("spark.driver.host", "localhost").config("spark.sql.shuffle.partitions", "1")
        .getOrCreate();

    terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());
    TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    mockReader = mock(ResourceReader.class);

    // Gather dependencies for the execution of the expression parser.
    parserContext = new ExpressionParserContext();
    parserContext.setFhirContext(TestUtilities.getFhirContext());
    parserContext.setTerminologyClientFactory(terminologyClientFactory);
    parserContext.setTerminologyClient(terminologyClient);
    parserContext.setSparkSession(spark);
    parserContext.setResourceReader(mockReader);
    parserContext.setSubjectContext(null);
    expressionParser = new ExpressionParser(parserContext);
    mockResourceReader(ResourceType.PATIENT);

    ResourceType resourceType = ResourceType.PATIENT;
    Dataset<Row> subject = mockReader.read(resourceType);
    String firstColumn = subject.columns()[0];
    String[] remainingColumns = Arrays.copyOfRange(subject.columns(), 1, subject.columns().length);
    Column idColumn = subject.col("id");
    subject = subject.withColumn("resource",
        org.apache.spark.sql.functions.struct(firstColumn, remainingColumns));
    Column valueColumn = subject.col("resource");
    subject = subject.select(idColumn, valueColumn);

    // Build up an input for the function.
    ParsedExpression subjectResource = new ParsedExpression();
    subjectResource.setFhirPath("%resource");
    subjectResource.setResource(true);
    subjectResource.setResourceType(ResourceType.PATIENT);
    subjectResource.setOrigin(subjectResource);
    subjectResource.setDataset(subject);
    subjectResource.setIdColumn(idColumn);
    subjectResource.setSingular(true);
    subjectResource.setValueColumn(valueColumn);

    parserContext.setSubjectContext(subjectResource);
  }


  private void mockResourceReader(ResourceType... resourceTypes) throws MalformedURLException {
    for (ResourceType resourceType : resourceTypes) {
      File parquetFile = new File(
          "src/test/resources/test-data/parquet/" + resourceType.toCode() + ".parquet");
      URL parquetUrl = parquetFile.getAbsoluteFile().toURI().toURL();
      assertThat(parquetUrl).isNotNull();
      Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
      when(mockReader.read(resourceType)).thenReturn(dataset);
      when(mockReader.getAvailableResourceTypes())
          .thenReturn(new HashSet<>(Arrays.asList(resourceTypes)));
    }
  }

  private ParsedExpressionAssert assertThatResultOf(String expression) {
    return assertThat(expressionParser.parse(expression));
  }

  @Test
  public void testContainsOperator() {
    assertThatResultOf("name.family contains 'Wuckert783'").isOfBooleanType().isSelection()
        .selectResult().hasRows(allPatientsWithValue(false).withRow(PATIENT_ID_9360820c, true));

    assertThatResultOf("name.suffix contains 'MD'").isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(false).withRow(PATIENT_ID_8ee183e2, true));
  }

  @Test
  public void testInOperator() {
    assertThatResultOf("'Wuckert783' in name.family").isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(false).withRow(PATIENT_ID_9360820c, true));

    assertThatResultOf("'MD' in name.suffix").isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(false).withRow(PATIENT_ID_8ee183e2, true));
  }

  @Test
  public void testCodingOperations() {

    // test unversioned
    assertThatResultOf(
        "maritalStatus.coding contains http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(true).withRow(PATIENT_ID_8ee183e2, false)
            .withRow(PATIENT_ID_9360820c, false).withRow(PATIENT_ID_beff242e, false));

    // test versioned
    assertThatResultOf(
        "http://terminology.hl7.org/CodeSystem/v2-0203|v2.0.3|PPN in identifier.type.coding")
        .isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(true).withRow(PATIENT_ID_bbd33563, false));
  }

  @Test
  public void testDateTimeLiterals() {
    // Full DateTime.
    ParsedExpression result = expressionParser.parse("@2015-02-04T14:34:28Z");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.DATE_TIME);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.DATETIME);
    assertThat(result.getLiteralValue()).isInstanceOf(StringType.class);
    assertThat(result.getLiteralValue().toString()).isEqualTo("2015-02-04T14:34:28Z");
    assertThat(result.getJavaLiteralValue()).isEqualTo("2015-02-04T14:34:28Z");
    assertThat(result.isSingular()).isTrue();

    // Date with no time component.
    result = expressionParser.parse("@2015-02-04");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.DATE_TIME);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.DATETIME);
    assertThat(result.getLiteralValue()).isInstanceOf(StringType.class);
    assertThat(result.getLiteralValue().toString()).isEqualTo("2015-02-04");
    assertThat(result.getJavaLiteralValue()).isEqualTo("2015-02-04");
    assertThat(result.isSingular()).isTrue();
  }


  @Test
  public void testTimeLiterals() {
    // Full Time.
    ParsedExpression result = expressionParser.parse("@T14:34:28Z");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.TIME);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.TIME);
    assertThat(result.getLiteralValue()).isInstanceOf(StringType.class);
    assertThat(result.getLiteralValue().toString()).isEqualTo("14:34:28Z");
    assertThat(result.getJavaLiteralValue()).isEqualTo("14:34:28Z");
    assertThat(result.isSingular()).isTrue();

    // Hour only.
    result = expressionParser.parse("@T14");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.TIME);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.TIME);
    assertThat(result.getLiteralValue()).isInstanceOf(StringType.class);
    assertThat(result.getLiteralValue().toString()).isEqualTo("14");
    assertThat(result.getJavaLiteralValue()).isEqualTo("14");
    assertThat(result.isSingular()).isTrue();
  }

  @After
  public void tearDown() {
    spark.close();
  }
}
