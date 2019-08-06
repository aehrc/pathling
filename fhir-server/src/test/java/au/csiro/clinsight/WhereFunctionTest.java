/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class WhereFunctionTest {

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
  public void simpleQuery() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
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
        + "          \"valueString\": \"Number of patients\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where($this.class.code = 'ambulatory').type.coding.display\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Encounter type, where ambulatory\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql =
        "SELECT d.display AS `Encounter type, where ambulatory`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN encounter a ON patient.id = a.subject.reference "
            + "LEFT JOIN encounter b ON patient.id = b.subject.reference AND b.class.code = 'ambulatory' "
            + "LATERAL VIEW OUTER EXPLODE(b.type) c AS c "
            + "LATERAL VIEW OUTER EXPLODE(c.coding) d AS d "
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

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
