/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.FHIR_SERVER_URL;
import static au.csiro.clinsight.TestConfiguration.mockDefinitionRetrieval;
import static au.csiro.clinsight.TestConfiguration.mockTableCaching;
import static au.csiro.clinsight.TestConfiguration.startFhirServer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import java.io.IOException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class AnalyticsServerTest {

  private Server server;

  @Before
  public void setUp() throws Exception {
    TerminologyClient mockTerminologyClient = mock(TerminologyClient.class);
    SparkSession mockSpark = mock(SparkSession.class);
    mockDefinitionRetrieval(mockTerminologyClient);
    mockTableCaching(mockSpark);

    AnalyticsServerConfiguration configuration = new AnalyticsServerConfiguration();
    configuration.setTerminologyClient(mockTerminologyClient);
    configuration.setSparkSession(mockSpark);

    server = startFhirServer(configuration);
  }


  @Test
  public void conformanceStatement() throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpGet httpGet = new HttpGet(FHIR_SERVER_URL + "/metadata");
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    }
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }
}
