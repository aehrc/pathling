/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.FHIR_SERVER_URL;
import static au.csiro.clinsight.TestConfiguration.createMockDataset;
import static au.csiro.clinsight.TestConfiguration.jsonParser;
import static au.csiro.clinsight.TestConfiguration.mockDefinitionRetrieval;
import static au.csiro.clinsight.TestConfiguration.postFhirResource;
import static au.csiro.clinsight.TestConfiguration.startFhirServer;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.FhirServerConfiguration;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQuery.AggregationComponent;
import au.csiro.clinsight.resources.AggregateQuery.GroupingComponent;
import au.csiro.clinsight.resources.AggregateQueryResult;
import au.csiro.clinsight.resources.AggregateQueryResult.LabelComponent;
import au.csiro.clinsight.resources.AggregateQueryResult.ResultComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jetty.server.Server;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.OperationOutcome;
import org.hl7.fhir.dstu3.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.dstu3.model.OperationOutcome.IssueType;
import org.hl7.fhir.dstu3.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.UnsignedIntType;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class QueryTest {

  private static final String QUERY_URL = FHIR_SERVER_URL + "/$query";
  private Server server;
  private SparkSession mockSpark;
  private CloseableHttpClient httpClient;

  @Before
  public void setUp() throws Exception {
    TerminologyClient mockTerminologyClient = mock(TerminologyClient.class);
    mockSpark = mock(SparkSession.class);
    mockDefinitionRetrieval(mockTerminologyClient);

    FhirServerConfiguration configuration = new FhirServerConfiguration();
    configuration.setTerminologyClient(mockTerminologyClient);
    configuration.setSparkSession(mockSpark);

    server = startFhirServer(configuration);
    httpClient = HttpClients.createDefault();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void simpleQuery() throws IOException, JSONException {
    String expectedSql =
        "SELECT patient.gender AS `Gender`, count(patient.id) AS `Number of patients` "
            + "FROM patient "
            + "GROUP BY patient.gender "
            + "ORDER BY patient.gender";
    StructField[] fields = {
        new StructField("Gender", DataTypes.StringType, true, null),
        new StructField("Number of patients", DataTypes.LongType, true, null)
    };
    StructType structType = new StructType(fields);
    List<Row> fakeResult = new ArrayList<>(Arrays.asList(
        new GenericRowWithSchema(new Object[]{"female", 70070L}, structType),
        new GenericRowWithSchema(new Object[]{"male", 73646L}, structType)
    ));

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(fakeResult);

    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of patients"));
    aggregation.setExpression(new StringType("count(Patient.id)"));
    query.setAggregation(singletonList(aggregation));

    GroupingComponent grouping = new GroupingComponent();
    grouping.setLabel(new StringType("Gender"));
    grouping.setExpression(new StringType("Patient.gender"));
    query.setGrouping(singletonList(grouping));

    AggregateQueryResult expectedResult = new AggregateQueryResult();
    expectedResult.setQuery(new Reference(query));
    AggregateQueryResult.GroupingComponent femaleGrouping = new AggregateQueryResult.GroupingComponent();
    femaleGrouping.setLabel(singletonList(new LabelComponent(new CodeType("female"))));
    femaleGrouping
        .setResult(singletonList(new ResultComponent(new UnsignedIntType(70070))));
    AggregateQueryResult.GroupingComponent maleGrouping = new AggregateQueryResult.GroupingComponent();
    maleGrouping.setLabel(singletonList(new LabelComponent(new CodeType("male"))));
    maleGrouping
        .setResult(singletonList(new ResultComponent(new UnsignedIntType(73646))));
    expectedResult.setGrouping(Arrays.asList(femaleGrouping, maleGrouping));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      AggregateQueryResult queryResult = (AggregateQueryResult) jsonParser
          .parseResource(response.getEntity().getContent());
      String queryResultJson = jsonParser.encodeResourceToString(queryResult);
      String expectedResultJson = jsonParser.encodeResourceToString(expectedResult);
      JSONAssert.assertEquals(queryResultJson, expectedResultJson, true);
    }

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
    verifyNoMoreInteractions(mockSpark);
  }

  @Test
  public void invalidAggregationFunction() throws IOException {
    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setExpression(new StringType("foo(Patient.id)"));
    query.setAggregation(singletonList(aggregation));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      OperationOutcome opOutcome = (OperationOutcome) jsonParser
          .parseResource(response.getEntity().getContent());
      assertThat(opOutcome.getIssue()).hasSize(1);
      OperationOutcomeIssueComponent issue = opOutcome.getIssueFirstRep();
      assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.ERROR);
      assertThat(issue.getCode()).isEqualTo(IssueType.PROCESSING);
      assertThat(issue.getDiagnostics()).isEqualTo("Unrecognised function: foo");
    }
  }

  @Test
  public void invalidResourceNameInGrouping() throws IOException {
    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of patients"));
    aggregation.setExpression(new StringType("count(Patient.id)"));
    query.setAggregation(Collections.singletonList(aggregation));

    GroupingComponent grouping = new GroupingComponent();
    grouping.setLabel(new StringType("Gender"));
    grouping.setExpression(new StringType("Foo.gender"));
    query.setGrouping(Collections.singletonList(grouping));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      OperationOutcome opOutcome = (OperationOutcome) jsonParser
          .parseResource(response.getEntity().getContent());
      assertThat(opOutcome.getIssue()).hasSize(1);
      OperationOutcomeIssueComponent issue = opOutcome.getIssueFirstRep();
      assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.ERROR);
      assertThat(issue.getCode()).isEqualTo(IssueType.PROCESSING);
      assertThat(issue.getDiagnostics()).isEqualTo("Resource identifier not known: Foo");
    }
  }

  @Test
  public void invalidElementNameInAggregation() throws IOException {
    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setExpression(new StringType("count(Patient.foo)"));
    query.setAggregation(Collections.singletonList(aggregation));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      OperationOutcome opOutcome = (OperationOutcome) jsonParser
          .parseResource(response.getEntity().getContent());
      assertThat(opOutcome.getIssue()).hasSize(1);
      OperationOutcomeIssueComponent issue = opOutcome.getIssueFirstRep();
      assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.ERROR);
      assertThat(issue.getCode()).isEqualTo(IssueType.PROCESSING);
      assertThat(issue.getDiagnostics()).isEqualTo("Element not known: Patient.foo");
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void aggregationFunctionWithMultipleArguments() throws IOException {
    String expectedSql = "SELECT count(patient.id, patient.gender) AS `Number of patients` "
        + "FROM patient";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of patients"));
    aggregation.setExpression(new StringType("count(Patient.id, Patient.gender)"));
    query.setAggregation(Collections.singletonList(aggregation));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void nonPrimitiveElementInAggregation() throws IOException {
    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setExpression(new StringType("count(Patient.identifier)"));
    query.setAggregation(Collections.singletonList(aggregation));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      OperationOutcome opOutcome = (OperationOutcome) jsonParser
          .parseResource(response.getEntity().getContent());
      assertThat(opOutcome.getIssue()).hasSize(1);
      OperationOutcomeIssueComponent issue = opOutcome.getIssueFirstRep();
      assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.ERROR);
      assertThat(issue.getCode()).isEqualTo(IssueType.PROCESSING);
      assertThat(issue.getDiagnostics()).isEqualTo(
          "Argument to aggregate function is not a primitive type: Patient.identifier (Identifier)");
    }
  }

  @Test
  public void nonPrimitiveElementInGrouping() throws IOException {
    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of patients"));
    aggregation.setExpression(new StringType("count(Patient.id)"));
    query.setAggregation(Collections.singletonList(aggregation));

    GroupingComponent grouping = new GroupingComponent();
    grouping.setLabel(new StringType("Photo"));
    grouping.setExpression(new StringType("Patient.photo"));
    query.setGrouping(Collections.singletonList(grouping));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      OperationOutcome opOutcome = (OperationOutcome) jsonParser
          .parseResource(response.getEntity().getContent());
      assertThat(opOutcome.getIssue()).hasSize(1);
      OperationOutcomeIssueComponent issue = opOutcome.getIssueFirstRep();
      assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.ERROR);
      assertThat(issue.getCode()).isEqualTo(IssueType.PROCESSING);
      assertThat(issue.getDiagnostics()).isEqualTo(
          "Grouping expression is not a primitive type: Patient.photo (Attachment)");
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTraversalInAggregation() throws IOException {
    String expectedSql = "SELECT count(identifierTypeCoding.code) AS `Number of patients` "
        + "FROM patient "
        + "LATERAL VIEW OUTER inline(patient.identifier) identifier AS id, use, type, system, value, period, assigner "
        + "LATERAL VIEW OUTER inline(identifier.type.coding) identifierTypeCoding AS id, system, version, code, display, userSelected";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of patients"));
    aggregation.setExpression(new StringType("count(Patient.identifier.type.coding.code)"));
    query.setAggregation(Collections.singletonList(aggregation));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTraversalInGrouping() throws IOException {
    String expectedSql = "SELECT communicationLanguageCoding.code AS `Language`, "
        + "count(patient.id) AS `Number of patients` "
        + "FROM patient "
        + "LATERAL VIEW OUTER inline(patient.communication) communication AS id, language, preferred "
        + "LATERAL VIEW OUTER inline(communication.language.coding) communicationLanguageCoding AS id, system, version, code, display, userSelected "
        + "GROUP BY communicationLanguageCoding.code "
        + "ORDER BY communicationLanguageCoding.code";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of patients"));
    aggregation.setExpression(new StringType("count(Patient.id)"));
    query.setAggregation(Collections.singletonList(aggregation));

    GroupingComponent grouping = new GroupingComponent();
    grouping.setLabel(new StringType("Language"));
    grouping.setExpression(new StringType("Patient.communication.language.coding.code"));
    query.setGrouping(Collections.singletonList(grouping));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValuePrimitive() throws IOException {
    String expectedSql = "SELECT category.category AS `Allergy category`, "
        + "count(allergyintolerance.id) AS `Number of allergies` "
        + "FROM allergyintolerance LATERAL VIEW OUTER explode(allergyintolerance.category) category AS category "
        + "GROUP BY category.category "
        + "ORDER BY category.category";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    AggregateQuery query = new AggregateQuery();

    AggregationComponent aggregation = new AggregationComponent();
    aggregation.setLabel(new StringType("Number of allergies"));
    aggregation.setExpression(new StringType("count(AllergyIntolerance.id)"));
    query.setAggregation(Collections.singletonList(aggregation));

    GroupingComponent grouping = new GroupingComponent();
    grouping.setLabel(new StringType("Allergy category"));
    grouping.setExpression(new StringType("AllergyIntolerance.category"));
    query.setGrouping(Collections.singletonList(grouping));

    HttpPost httpPost = postFhirResource(query, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }
}
