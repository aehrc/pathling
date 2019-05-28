/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class FilterTest {

  private static final String QUERY_URL = FHIR_SERVER_URL + "/$aggregate-query";
  private Server server;
  private SparkSession mockSpark;
  private CloseableHttpClient httpClient;

  @Before
  public void setUp() throws Exception {
    TerminologyClient mockTerminologyClient = mock(TerminologyClient.class);
    mockSpark = mock(SparkSession.class);
    mockDefinitionRetrieval(mockTerminologyClient);

    AnalyticsServerConfiguration configuration = new AnalyticsServerConfiguration();
    configuration.setTerminologyClient(mockTerminologyClient);
    configuration.setSparkSession(mockSpark);
    configuration.setExplainQueries(false);

    server = startFhirServer(configuration);
    httpClient = HttpClients.createDefault();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void simpleCodeFilter() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of encounters\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Encounter.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Encounter.class.code = 'emergency'\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql = "SELECT COUNT(DISTINCT encounter.id) AS `Number of encounters` "
        + "FROM encounter "
        + "WHERE encounter.class.code = 'emergency'";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void filterMissingValue() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        { \"name\": \"expression\", \"valueString\": \"AllergyIntolerance.count()\" },\n"
        + "        { \"name\": \"label\", \"valueString\": \"AllergyIntolerance.count()\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"AllergyIntolerance.clinicalStatus\"\n"
        + "        },\n"
        + "        { \"name\": \"label\", \"valueString\": \"AllergyIntolerance.clinicalStatus\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    { \"name\": \"filter\" }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Filter parameter must have value\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, Charset.forName("UTF-8"));
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }
  }

  @Test
  public void filterNotBoolean() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        { \"name\": \"expression\", \"valueString\": \"AllergyIntolerance.count()\" },\n"
        + "        { \"name\": \"label\", \"valueString\": \"AllergyIntolerance.count()\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"AllergyIntolerance.clinicalStatus\"\n"
        + "        },\n"
        + "        { \"name\": \"label\", \"valueString\": \"AllergyIntolerance.clinicalStatus\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    { \"name\": \"filter\", \"valueString\": \"AllergyIntolerance.clinicalStatus\" }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Filter expression is not of boolean type: AllergyIntolerance.clinicalStatus\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, Charset.forName("UTF-8"));
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void andOperator() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        { \"name\": \"expression\", \"valueString\": \"Encounter.count()\" },\n"
        + "        { \"name\": \"label\", \"valueString\": \"Number of encounters\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Encounter.class.code = 'emergency' and Encounter.type.coding.code = '183478001'\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql = "SELECT COUNT(DISTINCT encounter.id) AS `Number of encounters` "
        + "FROM encounter "
        + "LATERAL VIEW OUTER explode(encounter.type) encounterType AS encounterType "
        + "LATERAL VIEW OUTER explode(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
        + "WHERE encounter.class.code = 'emergency' AND encounterTypeCoding.code = '183478001'";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void orOperator() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        { \"name\": \"expression\", \"valueString\": \"Encounter.count()\" },\n"
        + "        { \"name\": \"label\", \"valueString\": \"Number of encounters\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Encounter.class.code = 'emergency' or Encounter.type.coding.code = '183478001'\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql = "SELECT COUNT(DISTINCT encounter.id) AS `Number of encounters` "
        + "FROM encounter "
        + "LATERAL VIEW OUTER explode(encounter.type) encounterType AS encounterType "
        + "LATERAL VIEW OUTER explode(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
        + "WHERE encounter.class.code = 'emergency' OR encounterTypeCoding.code = '183478001'";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void parenthesisedExpression() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        { \"name\": \"expression\", \"valueString\": \"Encounter.count()\" },\n"
        + "        { \"name\": \"label\", \"valueString\": \"Number of encounters\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"(Encounter.class.code = 'emergency' and Encounter.type.coding.code = '183478001') or Encounter.class.code = 'inpatient'\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql = "SELECT COUNT(DISTINCT encounter.id) AS `Number of encounters` "
        + "FROM encounter "
        + "LATERAL VIEW OUTER explode(encounter.type) encounterType AS encounterType "
        + "LATERAL VIEW OUTER explode(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
        + "WHERE (encounter.class.code = 'emergency' "
        + "AND encounterTypeCoding.code = '183478001') "
        + "OR encounter.class.code = 'inpatient'";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
