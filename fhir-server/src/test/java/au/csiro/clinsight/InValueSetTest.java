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
public class InValueSetTest {

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
        "SELECT patientEncounterAsSubjectReasonCodingValueSet006902cAggregated.codeExists AS `Diagnosis in value set?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientEncounterAsSubjectReasonCodingValueSet006902c.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN encounter patientEncounterAsSubject ON patient.id = patientEncounterAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM encounter "
            + "LATERAL VIEW OUTER explode(encounter.reason) patientEncounterAsSubjectReason AS patientEncounterAsSubjectReason "
            + "LATERAL VIEW OUTER explode(patientEncounterAsSubjectReason.coding) patientEncounterAsSubjectReasonCoding AS patientEncounterAsSubjectReasonCoding"
            + ") patientEncounterAsSubjectReasonCodingExploded ON patientEncounterAsSubject.id = patientEncounterAsSubjectReasonCodingExploded.id "
            + "LEFT JOIN `valueSet_006902c` patientEncounterAsSubjectReasonCodingValueSet006902c ON patientEncounterAsSubjectReasonCodingExploded.patientEncounterAsSubjectReasonCoding.system = patientEncounterAsSubjectReasonCodingValueSet006902c.system "
            + "AND patientEncounterAsSubjectReasonCodingExploded.patientEncounterAsSubjectReasonCoding.code = patientEncounterAsSubjectReasonCodingValueSet006902c.code "
            + "GROUP BY 1) patientEncounterAsSubjectReasonCodingValueSet006902cAggregated ON patient.id = patientEncounterAsSubjectReasonCodingValueSet006902cAggregated.id "
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
        "valueSet_006902c");
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
        "SELECT diagnosticReportResultCodeCodingValueSet0377504Aggregated.codeExists AS `Globulin observation?`, "
            + "COUNT(DISTINCT diagnosticreport.id) AS `Number of diagnostic reports` "
            + "FROM diagnosticreport "
            + "LEFT JOIN ("
            + "SELECT diagnosticreport.id, "
            + "CASE WHEN MAX(diagnosticReportResultCodeCodingValueSet0377504.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM diagnosticreport "
            + "LEFT JOIN ("
            + "SELECT * FROM diagnosticreport "
            + "LATERAL VIEW OUTER explode(diagnosticreport.result) diagnosticReportResult AS diagnosticReportResult"
            + ") diagnosticReportResultExploded ON diagnosticreport.id = diagnosticReportResultExploded.id "
            + "LEFT JOIN observation diagnosticReportResult ON diagnosticReportResultExploded.diagnosticReportResult.reference = diagnosticReportResult.id "
            + "LEFT JOIN ("
            + "SELECT * FROM observation "
            + "LATERAL VIEW OUTER explode(observation.code.coding) diagnosticReportResultCodeCoding AS diagnosticReportResultCodeCoding"
            + ") diagnosticReportResultCodeCodingExploded ON diagnosticReportResult.id = diagnosticReportResultCodeCodingExploded.id "
            + "LEFT JOIN `valueSet_0377504` diagnosticReportResultCodeCodingValueSet0377504 ON diagnosticReportResultCodeCodingExploded.diagnosticReportResultCodeCoding.system = diagnosticReportResultCodeCodingValueSet0377504.system "
            + "AND diagnosticReportResultCodeCodingExploded.diagnosticReportResultCodeCoding.code = diagnosticReportResultCodeCodingValueSet0377504.code "
            + "GROUP BY 1"
            + ") diagnosticReportResultCodeCodingValueSet0377504Aggregated ON diagnosticreport.id = diagnosticReportResultCodeCodingValueSet0377504Aggregated.id "
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
        "SELECT patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431Aggregated.codeExists AS `Prescribed medication containing metoprolol tartrate?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients`, "
            + "MAX(patient.multipleBirthInteger) AS `Max multiple birth` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN medicationrequest patientMedicationRequestAsSubject "
            + "ON patient.id = patientMedicationRequestAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * FROM medicationrequest "
            + "LATERAL VIEW OUTER explode(medicationrequest.medicationCodeableConcept.coding) patientMedicationRequestAsSubjectMedicationCodeableConceptCoding AS patientMedicationRequestAsSubjectMedicationCodeableConceptCoding"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded ON patientMedicationRequestAsSubject.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.id "
            + "LEFT JOIN `valueSet_59eb431` patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431 "
            + "ON patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.system = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431.system "
            + "AND patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.code = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431.code "
            + "GROUP BY 1"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431Aggregated "
            + "ON patient.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet59eb431Aggregated.id "
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
        "SELECT patient.gender AS `Gender`, "
            + "patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8dAggregated.codeExists AS `Prescribed TNF inhibitor?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN medicationrequest patientMedicationRequestAsSubject "
            + "ON patient.id = patientMedicationRequestAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM medicationrequest "
            + "LATERAL VIEW OUTER explode(medicationrequest.medicationCodeableConcept.coding) patientMedicationRequestAsSubjectMedicationCodeableConceptCoding AS patientMedicationRequestAsSubjectMedicationCodeableConceptCoding"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded "
            + "ON patientMedicationRequestAsSubject.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.id "
            + "LEFT JOIN `valueSet_8017b8d` patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d "
            + "ON patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.system = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d.system "
            + "AND patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.code = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d.code "
            + "GROUP BY 1"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8dAggregated "
            + "ON patient.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8dAggregated.id "
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
  public void multipleGroupingsOnInValueSetInput() throws IOException {
    String inParams = "{\n"
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        { \"name\": \"expression\", \"valueString\": \"Condition.count()\" },\n"
        + "        { \"name\": \"label\", \"valueString\": \"Number of conditions\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.code.coding.display\"\n"
        + "        },\n"
        + "        { \"name\": \"label\", \"valueString\": \"Condition type\" }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Condition.code.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/<< 125605004')\"\n"
        + "        },\n"
        + "        { \"name\": \"label\", \"valueString\": \"Is it a type of fracture?\" }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql =
        "SELECT conditionCodeCoding.display AS `Condition type`, "
            + "conditionCodeCodingValueSetce36080Aggregated.codeExists AS `Is it a type of fracture?`, "
            + "COUNT(DISTINCT condition.id) AS `Number of conditions` "
            + "FROM condition "
            + "LEFT JOIN ("
            + "SELECT * FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) conditionCodeCoding AS conditionCodeCoding"
            + ") conditionCodeCodingExploded ON condition.id = conditionCodeCodingExploded.id "
            + "LEFT JOIN ("
            + "SELECT condition.id, "
            + "CASE WHEN MAX(conditionCodeCodingValueSetce36080.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM condition "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) conditionCodeCoding AS conditionCodeCoding"
            + ") conditionCodeCodingExploded "
            + "ON condition.id = conditionCodeCodingExploded.id "
            + "LEFT JOIN `valueSet_ce36080` conditionCodeCodingValueSetce36080 "
            + "ON conditionCodeCodingExploded.conditionCodeCoding.system = conditionCodeCodingValueSetce36080.system "
            + "AND conditionCodeCodingExploded.conditionCodeCoding.code = conditionCodeCodingValueSetce36080.code "
            + "GROUP BY 1"
            + ") conditionCodeCodingValueSetce36080Aggregated "
            + "ON condition.id = conditionCodeCodingValueSetce36080Aggregated.id "
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
  public void snomedCtExample() throws IOException {
    String inParams = "{\n"
        + "    \"resourceType\": \"Parameters\",\n"
        + "    \"parameter\": [\n"
        + "        {\n"
        + "            \"name\": \"aggregation\",\n"
        + "            \"part\": [\n"
        + "                {\n"
        + "                    \"name\": \"label\",\n"
        + "                    \"valueString\": \"Number of patients\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"name\": \"expression\",\n"
        + "                    \"valueString\": \"Patient.count()\"\n"
        + "                }\n"
        + "            ]\n"
        + "        },\n"
        + "        {\n"
        + "            \"name\": \"grouping\",\n"
        + "            \"part\": [\n"
        + "                {\n"
        + "                    \"name\": \"label\",\n"
        + "                    \"valueString\": \"Prescribed TNF inhibitor?\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"name\": \"expression\",\n"
        + "                    \"valueString\": \"Patient.reverseResolve(MedicationRequest.subject).medicationCodeableConcept.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/(<< 416897008|Tumour necrosis factor alpha inhibitor product| OR 408154002|Adalimumab 40mg injection solution 0.8mL prefilled syringe|)')\"\n"
        + "                }\n"
        + "            ]\n"
        + "        },\n"
        + "        {\n"
        + "            \"name\": \"grouping\",\n"
        + "            \"part\": [\n"
        + "                {\n"
        + "                    \"name\": \"label\",\n"
        + "                    \"valueString\": \"Got lung infection?\"\n"
        + "                },\n"
        + "                {\n"
        + "                    \"name\": \"expression\",\n"
        + "                    \"valueString\": \"Patient.reverseResolve(Condition.subject).code.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/< 64572001|Disease (disorder)| : (363698007|Finding site| = << 39607008|Lung structure|, 370135005|Pathological process| = << 441862004|Infectious process|)')\"\n"
        + "                }\n"
        + "            ]\n"
        + "        },\n"
        + "        {\n"
        + "            \"name\": \"filter\",\n"
        + "            \"valueString\": \"Patient.reverseResolve(Condition.subject).code.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/< 64572001|Disease (disorder)| : (363698007|Finding site| = << 39352004|Joint structure|, 370135005|Pathological process| = << 263680009|Autoimmune process|)') and Patient.reverseResolve(Condition.subject).code.coding.inValueSet('http://snomed.info/sct?fhir_vs=ecl/< 64572001|Disease (disorder)| : (363698007|Finding site| = << 39607008|Lung structure|, 263502005|Clinical course| = << 90734009|Chronic|)')\"\n"
        + "        }\n"
        + "    ]\n"
        + "}";

    String expectedSql =
        "SELECT patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8dAggregated.codeExists AS `Prescribed TNF inhibitor?`, patientConditionAsSubjectCodeCodingValueSet269adeeAggregated.codeExists AS `Got lung infection?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN medicationrequest patientMedicationRequestAsSubject ON patient.id = patientMedicationRequestAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM medicationrequest "
            + "LATERAL VIEW OUTER explode(medicationrequest.medicationCodeableConcept.coding) patientMedicationRequestAsSubjectMedicationCodeableConceptCoding AS patientMedicationRequestAsSubjectMedicationCodeableConceptCoding"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded ON patientMedicationRequestAsSubject.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.id "
            + "LEFT JOIN `valueSet_8017b8d` patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d ON patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.system = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d.system "
            + "AND patientMedicationRequestAsSubjectMedicationCodeableConceptCodingExploded.patientMedicationRequestAsSubjectMedicationCodeableConceptCoding.code = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8d.code "
            + "GROUP BY 1"
            + ") patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8dAggregated ON patient.id = patientMedicationRequestAsSubjectMedicationCodeableConceptCodingValueSet8017b8dAggregated.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientConditionAsSubjectCodeCodingValueSet269adee.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject ON patient.id = patientConditionAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding"
            + ") patientConditionAsSubjectCodeCodingExploded ON patientConditionAsSubject.id = patientConditionAsSubjectCodeCodingExploded.id "
            + "LEFT JOIN `valueSet_269adee` patientConditionAsSubjectCodeCodingValueSet269adee ON patientConditionAsSubjectCodeCodingExploded.patientConditionAsSubjectCodeCoding.system = patientConditionAsSubjectCodeCodingValueSet269adee.system "
            + "AND patientConditionAsSubjectCodeCodingExploded.patientConditionAsSubjectCodeCoding.code = patientConditionAsSubjectCodeCodingValueSet269adee.code "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingValueSet269adeeAggregated ON patient.id = patientConditionAsSubjectCodeCodingValueSet269adeeAggregated.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientConditionAsSubjectCodeCodingValueSet04586e8.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject ON patient.id = patientConditionAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding"
            + ") patientConditionAsSubjectCodeCodingExploded ON patientConditionAsSubject.id = patientConditionAsSubjectCodeCodingExploded.id "
            + "LEFT JOIN `valueSet_04586e8` patientConditionAsSubjectCodeCodingValueSet04586e8 ON patientConditionAsSubjectCodeCodingExploded.patientConditionAsSubjectCodeCoding.system = patientConditionAsSubjectCodeCodingValueSet04586e8.system "
            + "AND patientConditionAsSubjectCodeCodingExploded.patientConditionAsSubjectCodeCoding.code = patientConditionAsSubjectCodeCodingValueSet04586e8.code "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingValueSet04586e8Aggregated ON patient.id = patientConditionAsSubjectCodeCodingValueSet04586e8Aggregated.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "CASE WHEN MAX(patientConditionAsSubjectCodeCodingValueSet0d8179c.code) IS NULL THEN FALSE ELSE TRUE END AS codeExists "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject ON patient.id = patientConditionAsSubject.subject.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding"
            + ") patientConditionAsSubjectCodeCodingExploded ON patientConditionAsSubject.id = patientConditionAsSubjectCodeCodingExploded.id "
            + "LEFT JOIN `valueSet_0d8179c` patientConditionAsSubjectCodeCodingValueSet0d8179c ON patientConditionAsSubjectCodeCodingExploded.patientConditionAsSubjectCodeCoding.system = patientConditionAsSubjectCodeCodingValueSet0d8179c.system "
            + "AND patientConditionAsSubjectCodeCodingExploded.patientConditionAsSubjectCodeCoding.code = patientConditionAsSubjectCodeCodingValueSet0d8179c.code "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingValueSet0d8179cAggregated ON patient.id = patientConditionAsSubjectCodeCodingValueSet0d8179cAggregated.id "
            + "WHERE patientConditionAsSubjectCodeCodingValueSet04586e8Aggregated.codeExists "
            + "AND patientConditionAsSubjectCodeCodingValueSet0d8179cAggregated.codeExists "
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
    httpClient.close();
  }

}
