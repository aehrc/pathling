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
        + "          \"valueString\": \"Patient.count()\"\n"
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
        "SELECT CASE WHEN patientEncounterAsSubjectReasonCodingValueSetAggregated.code IS NULL THEN FALSE ELSE TRUE END AS `Diagnosis in value set?`, "
            + "COUNT(DISTINCT patientEncounterAsSubjectReasonCodingValueSetAggregated.`Number of patients`) AS `Number of patients` "
            + "FROM ("
            + "SELECT patient.id AS `Number of patients`, MAX(patientEncounterAsSubjectReasonCodingValueSet.code) AS code "
            + "FROM patient "
            + "LEFT JOIN encounter patientEncounterAsSubject ON patient.id = patientEncounterAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT id, patientEncounterAsSubjectReasonCoding.system, patientEncounterAsSubjectReasonCoding.code "
            + "FROM encounter "
            + "LATERAL VIEW OUTER explode(encounter.reason) patientEncounterAsSubjectReason AS patientEncounterAsSubjectReason "
            + "LATERAL VIEW OUTER explode(patientEncounterAsSubjectReason.coding) patientEncounterAsSubjectReasonCoding AS patientEncounterAsSubjectReasonCoding"
            + ") patientEncounterAsSubjectReasonCodingExploded ON patientEncounterAsSubject.id = patientEncounterAsSubjectReasonCodingExploded.id "
            + "LEFT JOIN `valueSet_006902c7ba674e2a272362fced9ba9ee` patientEncounterAsSubjectReasonCodingValueSet "
            + "ON patientEncounterAsSubjectReasonCodingExploded.system = patientEncounterAsSubjectReasonCodingValueSet.system "
            + "AND patientEncounterAsSubjectReasonCodingExploded.code = patientEncounterAsSubjectReasonCodingValueSet.code "
            + "GROUP BY 1"
            + ") patientEncounterAsSubjectReasonCodingValueSetAggregated "
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
        + "          \"valueString\": \"DiagnosticReport.count()\"\n"
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
        "SELECT CASE WHEN diagnosticReportResultCodeCodingValueSetAggregated.code IS NULL THEN FALSE ELSE TRUE END AS `Globulin observation?`, "
            + "COUNT(DISTINCT diagnosticReportResultCodeCodingValueSetAggregated.`Number of diagnostic reports`) AS `Number of diagnostic reports` "
            + "FROM ("
            + "SELECT diagnosticreport.id AS `Number of diagnostic reports`, MAX(diagnosticReportResultCodeCodingValueSet.code) AS code "
            + "FROM diagnosticreport "
            + "LEFT JOIN ("
            + "SELECT id, diagnosticReportResult.reference "
            + "FROM diagnosticreport "
            + "LATERAL VIEW OUTER explode(diagnosticreport.result) diagnosticReportResult AS diagnosticReportResult"
            + ") diagnosticReportResultExploded ON diagnosticreport.id = diagnosticReportResultExploded.id "
            + "LEFT JOIN observation diagnosticReportResult ON diagnosticReportResultExploded.reference = diagnosticReportResult.id "
            + "LEFT JOIN ("
            + "SELECT id, diagnosticReportResultCodeCoding.system, diagnosticReportResultCodeCoding.code "
            + "FROM observation "
            + "LATERAL VIEW OUTER explode(observation.code.coding) diagnosticReportResultCodeCoding AS diagnosticReportResultCodeCoding"
            + ") diagnosticReportResultCodeCodingExploded ON diagnosticReportResult.id = diagnosticReportResultCodeCodingExploded.id "
            + "LEFT JOIN `valueSet_03775044c1767e1bcba62ef01cf1750d` diagnosticReportResultCodeCodingValueSet ON diagnosticReportResultCodeCodingExploded.system = diagnosticReportResultCodeCodingValueSet.system AND diagnosticReportResultCodeCodingExploded.code = diagnosticReportResultCodeCodingValueSet.code "
            + "GROUP BY 1"
            + ") diagnosticReportResultCodeCodingValueSetAggregated "
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
  public void inValueSetMultipleAggregations() throws IOException {
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
        + "          \"valueString\": \"Patient.count()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Max multiple birth\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.multipleBirthInteger.max()\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Prescribed medication containing metoprolol tartrate?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.reverseResolve(MedicationRequest.subject).medicationCodeableConcept.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/((* : << 30364011000036101|has Australian BoSS| = << 2338011000036107|metoprolol tartrate|) OR ((^ 929360041000036105|Trade product pack reference set| OR ^ 929360051000036108|Containered trade product pack reference set|) : 30409011000036107|has TPUU| = (* : << 30364011000036101|has Australian BoSS| = << 2338011000036107|metoprolol tartrate|)) OR (^ 929360081000036101|Medicinal product pack reference set| : 30348011000036104|has MPUU| = (* : << 30364011000036101|has Australian BoSS| = << 2338011000036107|metoprolol tartrate|)))')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT CASE WHEN patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated.code IS NULL THEN FALSE ELSE TRUE END AS `Prescribed medication containing metoprolol tartrate?`, "
            + "COUNT(DISTINCT patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated.`Number of patients`) AS `Number of patients`, "
            + "MAX(patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated.`Max multiple birth`) AS `Max multiple birth` "
            + "FROM ("
            + "SELECT patient.id AS `Number of patients`, "
            + "patient.multipleBirthInteger AS `Max multiple birth`, "
            + "MAX(patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet.code) AS code "
            + "FROM patient "
            + "LEFT JOIN medicationrequest patientMedicationRequestAsSubject "
            + "ON patient.id = patientMedicationRequestAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT id, patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.system, patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.code "
            + "FROM medicationrequest "
            + "LATERAL VIEW OUTER explode(medicationrequest.medicationCodeableConcept.coding) patientMedicationRequestAsSubjectMedicationCodeableConceptCoding AS patientMedicationRequestAsSubjectMedicationCodeableConceptCoding"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded ON patientMedicationRequestAsSubject.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.id "
            + "LEFT JOIN `valueSet_59eb431a24c87670dc95b1e669477345` patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet "
            + "ON patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.system = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet.system "
            + "AND patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.code = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet.code "
            + "GROUP BY 1, 2"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated "
            + "GROUP BY 1 "
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
  public void inValueSetMultipleGroupings() throws IOException {
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
        + "          \"valueString\": \"Patient.count()\"\n"
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
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Prescribed TNF inhibitor?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.reverseResolve(MedicationRequest.subject).medicationCodeableConcept.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/(<< 416897008|Tumour necrosis factor alpha inhibitor product| OR 408154002|Adalimumab 40mg injection solution 0.8mL prefilled syringe|)')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated.`Gender` AS `Gender`, "
            + "CASE WHEN patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated.code IS NULL THEN FALSE ELSE TRUE END AS `Prescribed TNF inhibitor?`, "
            + "COUNT(DISTINCT patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated.`Number of patients`) AS `Number of patients` "
            + "FROM ("
            + "SELECT patient.gender AS `Gender`, "
            + "patient.id AS `Number of patients`, "
            + "MAX(patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet.code) AS code "
            + "FROM patient "
            + "LEFT JOIN medicationrequest patientMedicationRequestAsSubject ON patient.id = patientMedicationRequestAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT id, patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.system, patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.code "
            + "FROM medicationrequest "
            + "LATERAL VIEW OUTER explode(medicationrequest.medicationCodeableConcept.coding) patientMedicationRequestAsSubjectMedicationCodeableConceptCoding AS patientMedicationRequestAsSubjectMedicationCodeableConceptCoding"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded "
            + "ON patientMedicationRequestAsSubject.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.id "
            + "LEFT JOIN `valueSet_8017b8dad6884547ed255fcfc3e43950` patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet "
            + "ON patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.system = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet.system "
            + "AND patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.code = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet.code "
            + "GROUP BY 1, 2"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSetAggregated "
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

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

}
