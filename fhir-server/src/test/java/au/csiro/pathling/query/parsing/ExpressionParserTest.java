/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.parsing;

import static au.csiro.pathling.TestUtilities.getSparkSession;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.encoding.SystemAndCode;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.ResourceReader;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.query.parsing.parser.ExpressionParser;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.ParsedExpressionAssert;
import java.beans.BeanInfo;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
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
  private ExpressionParser expressionParser;

  @Before
  public void setUp() throws IOException {
    spark = getSparkSession();

    TerminologyClient terminologyClient =
        mock(TerminologyClient.class, Mockito.withSettings().serializable());
    TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class);
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    mockReader = mock(ResourceReader.class);

    // Gather dependencies for the execution of the expression parser.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setFhirContext(TestUtilities.getFhirContext());
    parserContext.setTerminologyClientFactory(terminologyClientFactory);
    parserContext.setTerminologyClient(terminologyClient);
    parserContext.setSparkSession(spark);
    parserContext.setResourceReader(mockReader);
    parserContext.setSubjectContext(null);
    expressionParser = new ExpressionParser(parserContext);
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
        ResourceType.PROCEDURE);

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
      File parquetFile =
          new File("src/test/resources/test-data/parquet/" + resourceType.toCode() + ".parquet");
      URL parquetUrl = parquetFile.getAbsoluteFile().toURI().toURL();
      Assertions.assertThat(parquetUrl).isNotNull();
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
        .selectResult().hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_9360820c, true));

    assertThatResultOf("name.suffix contains 'MD'").isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_8ee183e2, true));
  }

  @Test
  public void testInOperator() {
    assertThatResultOf("'Wuckert783' in name.family").isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_9360820c, true));

    assertThatResultOf("'MD' in name.suffix").isOfBooleanType().isSelection().selectResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_8ee183e2, true));
  }

  @Test
  public void testCodingOperations() {
    // test unversioned
    assertThatResultOf(
        "maritalStatus.coding contains http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
            .isOfBooleanType().isSelection().selectResult()
            .hasRows(allPatientsWithValue(true).changeValue(PATIENT_ID_8ee183e2, false)
                .changeValue(PATIENT_ID_9360820c, false).changeValue(PATIENT_ID_beff242e, false));

    // test versioned
    assertThatResultOf(
        "http://terminology.hl7.org/CodeSystem/v2-0203|v2.0.3|PPN in identifier.type.coding")
            .isOfBooleanType().isSelection().selectResult()
            .hasRows(allPatientsWithValue(true).changeValue(PATIENT_ID_bbd33563, false));
  }

  @Test
  public void testDateTimeLiterals() {
    // Full DateTime.
    ParsedExpression result = expressionParser.parse("@2015-02-04T14:34:28Z");
    assertThat(result).isOfType(FHIRDefinedType.DATETIME, FhirPathType.DATE_TIME);
    assertThat(result).isStringLiteral("2015-02-04T14:34:28Z");
    assertThat(result).isSingular();

    // Date with no time component.
    result = expressionParser.parse("@2015-02-04");
    assertThat(result).isOfType(FHIRDefinedType.DATETIME, FhirPathType.DATE_TIME);
    assertThat(result).isStringLiteral("2015-02-04");
    assertThat(result).isSingular();
  }


  @Test
  public void testTimeLiterals() {
    // Full Time.
    ParsedExpression result = expressionParser.parse("@T14:34:28Z");
    assertThat(result).isOfType(FHIRDefinedType.TIME, FhirPathType.TIME);
    assertThat(result).isStringLiteral("14:34:28Z");
    assertThat(result).isSingular();

    // Hour only.
    result = expressionParser.parse("@T14");
    assertThat(result).isOfType(FHIRDefinedType.TIME, FhirPathType.TIME);
    assertThat(result).isStringLiteral("14");
    assertThat(result).isSingular();
  }


  @Test
  public void testCodingLiterals() {

    // Coding literal form [system]|[code]
    final Coding expectedCoding =
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "S", null);
    assertThatResultOf("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .isOfType(FHIRDefinedType.CODING, FhirPathType.CODING).isSingular()
        .isTypeLiteral(expectedCoding);

    // Coding literal form [system]|[version]|[code]

    final Coding expectedCodingWithVersion =
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "S", null);
    expectedCodingWithVersion.setVersion("v1");
    assertThatResultOf("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|v1|S")
        .isOfType(FHIRDefinedType.CODING, FhirPathType.CODING).isSingular()
        .isTypeLiteral(expectedCodingWithVersion);

  }


  @Test
  public void testCountWithReverseResolve() {

    assertThatResultOf("reverseResolve(Condition.subject).code.coding.count()").isSelection()
        .isPrimitive().isSingular().selectResult()
        .hasRows(allPatientsWithValue(8L).changeValue(PATIENT_ID_121503c8, 10L)
            .changeValue(PATIENT_ID_2b36c1e2, 3L).changeValue(PATIENT_ID_7001ad9c, 5L)
            .changeValue(PATIENT_ID_9360820c, 16L).changeValue(PATIENT_ID_beff242e, 3L)
            .changeValue(PATIENT_ID_bbd33563, 10L));

  }

  @Test
  public void testCount() {
    DatasetBuilder expectedCountResult =
        allPatientsWithValue(1L).changeValue(PATIENT_ID_9360820c, 2L);
    assertThatResultOf("name.count()").isSelection().selectResult().hasRows(expectedCountResult);

    assertThatResultOf("name.family.count()").isSelection().selectResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.family.count()").isAggregation().aggByIdResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.given.count()").isSelection().selectResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.prefix.count()").isSelection().selectResult()
        .hasRows(expectedCountResult.changeValue(PATIENT_ID_bbd33563, 0L));
  }

  @Test
  public void testSubsumes() throws Exception {
    
    BeanInfo bi = java.beans.Introspector.getBeanInfo(SystemAndCode.class);
    Stream.of(bi.getPropertyDescriptors()).forEach(System.out::println);
    System.out.println();
    
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|444814009)")
            .selectResult().debugAllRows();

    //assertThatResultOf("reverseResolve(Condition.subject).code.coding.subsumes(reverseResolve(Condition.subject).code)").selectResult().debugSchema().debugAllRows();
    // //
    // assertThatResultOf("reverseResolve(Condition.subject).code.coding.subsumes(http://snomed.info/sct|444814009)").selectResult().debugAllRows();
    // assertThatResultOf("reverseResolve(Condition.subject).code.coding").selectResult().debugSchema().debugAllRows();
    // assertThatResultOf("Patient.reverseResolve(Encounter.subject).reverseResolve(Procedure.encounter).reasonCode.coding").selectResult().debugSchema().debugAllRows();
    // assertThatResultOf("http://snomed.info/sct|444814009").selectResult().debugSchema().debugAllRows();
  }

}
