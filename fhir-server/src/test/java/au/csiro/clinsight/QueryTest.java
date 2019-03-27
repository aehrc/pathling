/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.FHIR_SERVER_URL;
import static au.csiro.clinsight.TestConfiguration.createMockDataset;
import static au.csiro.clinsight.TestConfiguration.mockDefinitionRetrieval;
import static au.csiro.clinsight.TestConfiguration.postFhirResource;
import static au.csiro.clinsight.TestConfiguration.startFhirServer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
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
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class QueryTest {

  private static final String QUERY_URL = FHIR_SERVER_URL + "/$aggregateQuery";
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

    server = startFhirServer(configuration);
    httpClient = HttpClients.createDefault();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void simpleQuery() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.distinct().count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueCode\": \"female\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"result\",\n"
        + "          \"valueUnsignedInt\": 70070\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueCode\": \"male\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"result\",\n"
        + "          \"valueUnsignedInt\": 73646\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT patient.gender AS `Gender`, count(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";
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

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, Charset.forName("UTF-8"));
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
    verifyNoMoreInteractions(mockSpark);
  }

  @Test
  public void invalidAggregationFunction() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.foo()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Unrecognised function: foo\"\n"
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
  public void invalidResourceNameInGrouping() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.distinct().count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Foo.gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Resource or data type not known: Foo\"\n"
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
  public void invalidElementNameInAggregation() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.foo.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Element not known: Patient.foo\"\n"
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
  public void nonPrimitiveElementInAggregation() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.identifier.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Input to count function must be of primitive type: Patient.identifier (Identifier)\"\n"
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
  public void nonPrimitiveElementInGrouping() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Photo\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.photo\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Grouping expression is not of primitive type: Patient.photo (Attachment)\"\n"
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
  public void multiValueTraversalInAggregation() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.identifier.type.coding.code.distinct().count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT count(DISTINCT patientIdentifierTypeCoding.code) AS `Number of patients` "
            + "FROM patient "
            + "LATERAL VIEW explode(patient.identifier) patientIdentifier AS patientIdentifier "
            + "LATERAL VIEW explode(patientIdentifier.type.coding) patientIdentifierTypeCoding AS patientIdentifierTypeCoding "
            + "ORDER BY 1";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValueTraversalInGrouping() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.distinct().count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Language\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.communication.language.coding.code\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql = "SELECT patientCommunicationLanguageCoding.code AS `Language`, "
        + "count(DISTINCT patient.id) AS `Number of patients` "
        + "FROM patient "
        + "LATERAL VIEW explode(patient.communication) patientCommunication AS patientCommunication "
        + "LATERAL VIEW explode(patientCommunication.language.coding) patientCommunicationLanguageCoding AS patientCommunicationLanguageCoding "
        + "GROUP BY 1 "
        + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multiValuePrimitive() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of allergies\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"AllergyIntolerance.id.distinct().count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Allergy category\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"AllergyIntolerance.category\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT allergyIntoleranceCategory AS `Allergy category`, count(DISTINCT allergyintolerance.id) AS `Number of allergies` "
            + "FROM allergyintolerance "
            + "LATERAL VIEW explode(allergyintolerance.category) allergyIntoleranceCategory AS allergyIntoleranceCategory "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void referenceTraversalInGrouping() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of allergies/intolerances\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"AllergyIntolerance.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Patient gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"AllergyIntolerance.patient.resolve().gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT allergyIntolerancePatient.gender AS `Patient gender`, count(allergyintolerance.id) AS `Number of allergies/intolerances` "
            + "FROM allergyintolerance "
            + "INNER JOIN patient allergyIntolerancePatient ON allergyintolerance.patient.reference = allergyIntolerancePatient.id "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void referenceWithDependencyOnLateralView() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of diagnostic reports\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"DiagnosticReport.id.distinct().count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Observation type\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"DiagnosticReport.result.resolve().code.coding.display\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT diagnosticReportResultCodeCoding.display AS `Observation type`, count(DISTINCT diagnosticreport.id) AS `Number of diagnostic reports` "
            + "FROM diagnosticreport "
            + "INNER JOIN ("
            + "SELECT id, diagnosticReportResult.reference "
            + "FROM diagnosticreport "
            + "LATERAL VIEW explode(diagnosticreport.result) diagnosticReportResult AS diagnosticReportResult"
            + ") diagnosticReportResultReference ON diagnosticreport.id = diagnosticReportResultReference.id "
            + "INNER JOIN observation diagnosticReportResult ON diagnosticReportResultReference.reference = diagnosticReportResult.id "
            + "LATERAL VIEW explode(diagnosticReportResult.code.coding) diagnosticReportResultCodeCoding AS diagnosticReportResultCodeCoding "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void polymorphicReferenceTraversal() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of conditions\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Context encounter type\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.context.resolve(Encounter).type.coding.display\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT conditionContextTypeCoding.display AS `Context encounter type`, count(condition.id) AS `Number of conditions` "
            + "FROM condition "
            + "INNER JOIN encounter conditionContext ON condition.context.reference = conditionContext.id "
            + "LATERAL VIEW explode(conditionContext.type) conditionContextType AS conditionContextType "
            + "LATERAL VIEW explode(conditionContextType.coding) conditionContextTypeCoding AS conditionContextTypeCoding "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void anyReferenceTraversal() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of conditions\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Associated diagnosis\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.evidence.detail.resolve(DiagnosticReport).codedDiagnosis.coding.display\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT conditionEvidenceDetailCodedDiagnosisCoding.display AS `Associated diagnosis`, count(condition.id) AS `Number of conditions` "
            + "FROM condition "
            + "INNER JOIN ("
            + "SELECT id, conditionEvidenceDetail.reference "
            + "FROM condition "
            + "LATERAL VIEW explode(condition.evidence) conditionEvidence AS conditionEvidence "
            + "LATERAL VIEW explode(conditionEvidence.detail) conditionEvidenceDetail AS conditionEvidenceDetail"
            + ") conditionEvidenceDetailReference ON condition.id = conditionEvidenceDetailReference.id "
            + "INNER JOIN diagnosticreport conditionEvidenceDetail ON conditionEvidenceDetailReference.reference = conditionEvidenceDetail.id "
            + "LATERAL VIEW explode(conditionEvidenceDetail.codedDiagnosis) conditionEvidenceDetailCodedDiagnosis AS conditionEvidenceDetailCodedDiagnosis "
            + "LATERAL VIEW explode(conditionEvidenceDetailCodedDiagnosis.coding) conditionEvidenceDetailCodedDiagnosisCoding AS conditionEvidenceDetailCodedDiagnosisCoding "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void nonReferenceInvokerForResolve() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.evidence.resolve().something\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Input to resolve function must be a Reference: Condition.evidence (BackboneElement)\"\n"
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
  public void noArgumentToPolymorphicResolve() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.evidence.detail.resolve().codedDiagnosis.coding.display\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Attempt to resolve polymorphic reference without providing an argument: Condition.evidence.detail\"\n"
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
  public void nonResourceArgumentToResolve() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.evidence.detail.resolve(DiagnosticReport.id).codedDiagnosis.coding.display\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Argument to resolve function must be a base resource type: DiagnosticReport.id (id)\"\n"
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
  public void reverseReferenceTraversalInGrouping() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Reason for encounter\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject).reason.coding.display\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT patientEncounterAsSubjectReasonCoding.display AS `Reason for encounter`, count(patient.id) AS `Number of patients` "
            + "FROM patient "
            + "INNER JOIN encounter patientEncounterAsSubject ON patient.id = patientEncounterAsSubject.subject.reference "
            + "LATERAL VIEW explode(patientEncounterAsSubject.reason) patientEncounterAsSubjectReason AS patientEncounterAsSubjectReason "
            + "LATERAL VIEW explode(patientEncounterAsSubjectReason.coding) patientEncounterAsSubjectReasonCoding AS patientEncounterAsSubjectReasonCoding "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(expectedSql)).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @Test
  public void nonResourceInvokerForReverseResolve() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.id.reverseResolve(Encounter.subject).diagnosis\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Input to reverseResolve function must be a Resource: Patient.id (id)\"\n"
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
  public void nonReferenceArgumentToReverseResolve() throws IOException, JSONException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.reverseResolve(Encounter).diagnosis\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedResponse = "{\n"
        + "  \"resourceType\": \"OperationOutcome\",\n"
        + "  \"issue\": [\n"
        + "    {\n"
        + "      \"severity\": \"error\",\n"
        + "      \"code\": \"processing\",\n"
        + "      \"diagnostics\": \"Argument to reverseResolve function must be a Reference: Encounter (Encounter)\"\n"
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
  }
}
