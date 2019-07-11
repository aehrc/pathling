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
public class MembershipExpressionTest {

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
  public void literalStringValue() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"'40275004' in Patient.reverseResolve(Condition.subject).code.coding.code\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql =
        "SELECT COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(patientConditionAsSubjectCodeCoding.code = '40275004'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject "
            + "ON patient.id = patientConditionAsSubject.subject.reference "
            + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingMembership "
            + "ON patient.id = patientConditionAsSubjectCodeCodingMembership.id "
            + "WHERE patientConditionAsSubjectCodeCodingMembership.result";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void singularPrimitive() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Patient.gender in Patient.photo.title\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql =
        "SELECT patientPhotoTitleMembership.result AS `Gender in the title of a photo`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientPhoto.title) IS NULL THEN FALSE ELSE TRUE END AS result "
            + "FROM patient "
            + "LATERAL VIEW OUTER explode(patient.photo) patientPhoto AS patientPhoto "
            + "GROUP BY 1"
            + ") patientPhotoTitleMembership "
            + "ON patient.id = patientPhotoTitleMembership.id "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void coding() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"http://snomed.info/sct|44054006 in Patient.reverseResolve(Condition.subject).code\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Diagnosed with type 2 diabetes?\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql =
        "SELECT patient.gender AS `Gender`, "
            + "patientConditionAsSubjectCodeCodingMembership.result AS `Diagnosed with type 2 diabetes?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX((patientConditionAsSubjectCodeCoding.system = 'http://snomed.info/sct' AND patientConditionAsSubjectCodeCoding.code = '44054006')), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject "
            + "ON patient.id = patientConditionAsSubject.subject.reference "
            + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingMembership "
            + "ON patient.id = patientConditionAsSubjectCodeCodingMembership.id "
            + "GROUP BY 1, 2 "
            + "ORDER BY 1, 2, 3";

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
  public void contains() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Patient.reverseResolve(Condition.subject).code.coding.code contains '40275004'\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql =
        "SELECT COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX(patientConditionAsSubjectCodeCoding.code = '40275004'), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject "
            + "ON patient.id = patientConditionAsSubject.subject.reference "
            + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingMembership "
            + "ON patient.id = patientConditionAsSubjectCodeCodingMembership.id "
            + "WHERE patientConditionAsSubjectCodeCodingMembership.result";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void leftOperandIsNotSingular() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Patient.name.given in Patient.photo.title\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Maximum cardinality of left operand in membership expression must not be greater than one: Patient.name.given\"\n"
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

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
