/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.TestConfiguration.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jetty.server.Server;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.dstu3.model.ValueSet;
import org.hl7.fhir.dstu3.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.dstu3.model.ValueSet.ValueSetExpansionContainsComponent;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class TerminologyQueryTest {

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
        + "          \"valueString\": \"Patient.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Diagnosis in value set?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject).reason.coding.inValueSet('https://clinsight.csiro.au/fhir/ValueSet/some-value-set-0')\"\n"
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
        + "          \"valueBoolean\": true\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"result\",\n"
        + "          \"valueUnsignedInt\": 145999\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueBoolean\": false\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"result\",\n"
        + "          \"valueUnsignedInt\": 12344\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT /* MAPJOIN(patientEncounterAsSubjectReasonCodingValueSet) */ "
            + "CASE WHEN patientEncounterAsSubjectReasonCodingValueSet.code IS NULL THEN FALSE ELSE TRUE END AS `Diagnosis in value set?`, "
            + "count(patient.id) AS `Number of patients` "
            + "FROM patient "
            + "INNER JOIN encounter patientEncounterAsSubject ON patient.id = patientEncounterAsSubject.subject.reference "
            + "INNER JOIN ("
            + "SELECT id, patientEncounterAsSubjectReasonCoding.system, patientEncounterAsSubjectReasonCoding.code "
            + "FROM encounter "
            + "LATERAL VIEW explode(encounter.reason) patientEncounterAsSubjectReason AS patientEncounterAsSubjectReason "
            + "LATERAL VIEW explode(patientEncounterAsSubjectReason.coding) patientEncounterAsSubjectReasonCoding AS patientEncounterAsSubjectReasonCoding"
            + ") patientEncounterAsSubjectReasonCodingExploded ON patientEncounterAsSubject.id = patientEncounterAsSubjectReasonCodingExploded.id "
            + "LEFT OUTER JOIN `valueSet_006902c7ba674e2a272362fced9ba9ee` patientEncounterAsSubjectReasonCodingValueSet "
            + "ON patientEncounterAsSubjectReasonCodingExploded.system = patientEncounterAsSubjectReasonCodingValueSet.system "
            + "AND patientEncounterAsSubjectReasonCodingExploded.code = patientEncounterAsSubjectReasonCodingValueSet.code "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    when(mockCatalog.tableExists(any(), any())).thenReturn(false);

    ValueSet fakeValueSet = new ValueSet();
    ValueSetExpansionComponent expansion = new ValueSetExpansionComponent();
    ValueSetExpansionContainsComponent contains1 = new ValueSetExpansionContainsComponent();
    contains1.setSystem("http://snomed.info/sct");
    contains1.setCode("18643000");
    expansion.getContains().add(contains1);
    ValueSetExpansionContainsComponent contains2 = new ValueSetExpansionContainsComponent();
    contains2.setSystem("http://snomed.info/sct");
    contains2.setCode("88850006");
    expansion.getContains().add(contains2);
    fakeValueSet.setExpansion(expansion);
    when(mockTerminologyClient.expandValueSet(any(UriType.class))).thenReturn(fakeValueSet);
    Dataset<Row> mockExpansionDataset = createMockDataset();
    when(mockSpark.createDataset(any(List.class), any(Encoder.class)))
        .thenReturn(mockExpansionDataset);

    StructField[] fields = {
        new StructField("Diagnosis in value set?", DataTypes.BooleanType, true, null),
        new StructField("Number of patients", DataTypes.LongType, true, null)
    };
    StructType structType = new StructType(fields);
    List<Row> fakeResult = new ArrayList<>(Arrays.asList(
        new GenericRowWithSchema(new Object[]{true, 145999L}, structType),
        new GenericRowWithSchema(new Object[]{false, 12344L}, structType)
    ));

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(fakeResult);

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, Charset.forName("UTF-8"));
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }

    verify(mockTerminologyClient).expandValueSet(
        argThat(uri -> uri.getValue()
            .equals("https://clinsight.csiro.au/fhir/ValueSet/some-value-set-0")));
    verify(mockSpark).createDataset(any(List.class), any(Encoder.class));
    verify(mockExpansionDataset).createOrReplaceTempView(
        "valueSet_006902c7ba674e2a272362fced9ba9ee");
    verify(mockSpark, atLeastOnce()).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void multipleSetsOfLateralViews() throws IOException {
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
        + "          \"valueString\": \"DiagnosticReport.id.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Globulin observation?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"DiagnosticReport.result.resolve().code.coding.inValueSet('http://loinc.org/vs/LP14885-5')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT /* MAPJOIN(diagnosticReportResultCodeCodingValueSet) */ "
            + "CASE WHEN diagnosticReportResultCodeCodingValueSet.code IS NULL THEN FALSE ELSE TRUE END AS `Globulin observation?`, "
            + "count(diagnosticreport.id) AS `Number of diagnostic reports` "
            + "FROM diagnosticreport "
            + "INNER JOIN ("
            + "SELECT id, diagnosticReportResult.reference "
            + "FROM diagnosticreport "
            + "LATERAL VIEW explode(diagnosticreport.result) diagnosticReportResult AS diagnosticReportResult"
            + ") diagnosticReportResultExploded ON diagnosticreport.id = diagnosticReportResultExploded.id "
            + "INNER JOIN observation diagnosticReportResult ON diagnosticReportResultExploded.reference = diagnosticReportResult.id "
            + "INNER JOIN ("
            + "SELECT id, diagnosticReportResultCodeCoding.system, diagnosticReportResultCodeCoding.code "
            + "FROM observation "
            + "LATERAL VIEW explode(observation.code.coding) diagnosticReportResultCodeCoding AS diagnosticReportResultCodeCoding"
            + ") diagnosticReportResultCodeCodingExploded ON diagnosticReportResult.id = diagnosticReportResultCodeCodingExploded.id "
            + "LEFT OUTER JOIN `valueSet_03775044c1767e1bcba62ef01cf1750d` diagnosticReportResultCodeCodingValueSet "
            + "ON diagnosticReportResultCodeCodingExploded.system = diagnosticReportResultCodeCodingValueSet.system "
            + "AND diagnosticReportResultCodeCodingExploded.code = diagnosticReportResultCodeCodingValueSet.code "
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
  }

}
