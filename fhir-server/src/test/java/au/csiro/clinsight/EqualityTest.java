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
import java.nio.charset.StandardCharsets;
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
import org.junit.Ignore;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class EqualityTest {

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
  public void singularOperands() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Encounter\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of encounters\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.class.code = 'emergency'\"\n"
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
  @Ignore
  public void operandNotSingular() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Encounter\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of encounters\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.type.text = 'something'\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Equality operator does not support operand that is not singular: Encounter.type.text\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }
  }

  @Test
  @Ignore
  public void operandNotPrimitive() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Encounter\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of encounters\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.period = %resource.partOf.resolve().period\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Equality operator does not support operand that is not primitive: Encounter.period\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(400);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
