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
import java.util.HashMap;
import java.util.Map;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class DateFunctionsTest {

  private static final String QUERY_URL = FHIR_SERVER_URL + "/$aggregate-query";
  private Server server;
  private TerminologyClient mockTerminologyClient;
  private SparkSession mockSpark;
  private Catalog mockCatalog;
  private CloseableHttpClient httpClient;

  @Before
  public void setUp() throws Exception {
    mockTerminologyClient = mock(TerminologyClient.class);
    mockSpark = mock(SparkSession.class);
    mockDefinitionRetrieval(mockTerminologyClient);

    mockCatalog = mock(Catalog.class);
    when(mockSpark.catalog()).thenReturn(mockCatalog);
    when(mockCatalog.tableExists(any(), any())).thenReturn(true);

    AnalyticsServerConfiguration configuration = new AnalyticsServerConfiguration();
    configuration.setTerminologyClient(mockTerminologyClient);
    configuration.setSparkSession(mockSpark);
    configuration.setExplainQueries(false);

    server = startFhirServer(configuration);
    httpClient = HttpClients.createDefault();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void dateFormat() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/DiagnosticReport\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of diagnostic reports\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Month issued\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.issued.dateFormat('MMM')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.issued >= @2016-01-01 and %resource.issued <= @2016-09-30\"\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT date_format(diagnosticreport.issued, 'MMM') AS `Month issued`, COUNT(DISTINCT diagnosticreport.id) AS `Number of diagnostic reports` "
            + "FROM diagnosticreport "
            + "WHERE diagnosticreport.issued >= '2016-01-01' "
            + "AND diagnosticreport.issued <= '2016-09-30' "
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


  @SuppressWarnings("unchecked")
  @Test
  public void dateComponentFunctions() throws IOException {
    Map<String, String> functionsMap = new HashMap<String, String>() {{
      put("toSeconds", "second");
      put("toMinutes", "minute");
      put("toHours", "hour");
      put("dayOfMonth", "dayofmonth");
      put("dayOfWeek", "dayofweek");
      put("weekOfYear", "weekofyear");
      put("toMonthNumber", "month");
      put("toQuarter", "quarter");
      put("toYear", "year");
    }};
    for (String functionName : functionsMap.keySet()) {
      httpClient = HttpClients.createDefault();
      String inParams = "{\n"
          + "  \"resourceType\": \"Parameters\",\n"
          + "  \"parameter\": [\n"
          + "    {\n"
          + "      \"name\": \"subjectResource\",\n"
          + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/DiagnosticReport\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"aggregation\",\n"
          + "      \"part\": [\n"
          + "        {\n"
          + "          \"name\": \"label\",\n"
          + "          \"valueString\": \"Number of diagnostic reports\"\n"
          + "        },\n"
          + "        {\n"
          + "          \"name\": \"expression\",\n"
          + "          \"valueString\": \"%resource.count()\"\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"grouping\",\n"
          + "      \"part\": [\n"
          + "        {\n"
          + "          \"name\": \"label\",\n"
          + "          \"valueString\": \"Issued date component\"\n"
          + "        },\n"
          + "        {\n"
          + "          \"name\": \"expression\",\n"
          + "          \"valueString\": \"%resource.issued." + functionName + "()\"\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"filter\",\n"
          + "      \"valueString\": \"%resource.issued >= @2016-01-01 and %resource.issued <= @2016-09-30\"\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";

      String expectedSql =
          "SELECT " + functionsMap.get(functionName)
              + "(diagnosticreport.issued) AS `Issued date component`, COUNT(DISTINCT diagnosticreport.id) AS `Number of diagnostic reports` "
              + "FROM diagnosticreport "
              + "WHERE diagnosticreport.issued >= '2016-01-01' "
              + "AND diagnosticreport.issued <= '2016-09-30' "
              + "GROUP BY 1 "
              + "ORDER BY 1, 2";

      Dataset mockDataset = createMockDataset();
      when(mockSpark.sql(any())).thenReturn(mockDataset);
      when(mockDataset.collectAsList()).thenReturn(new ArrayList());

      HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
      httpClient.execute(httpPost);

      verify(mockSpark).sql("USE clinsight");
      verify(mockSpark).sql(expectedSql);
      httpClient.close();
    }
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
