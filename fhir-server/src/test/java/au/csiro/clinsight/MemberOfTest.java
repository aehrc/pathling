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
import java.nio.charset.StandardCharsets;
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
public class MemberOfTest {

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
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
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
        + "          \"valueString\": \"Diagnosis in value set?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).reason.memberOf('https://clinsight.csiro.au/fhir/ValueSet/some-value-set-0')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

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
        "SELECT d.result AS `Diagnosis in value set?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(c.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN encounter a ON patient.id = a.subject.reference "
            + "LEFT JOIN ("
            + "SELECT encounter.id, e.* "
            + "FROM encounter "
            + "LATERAL VIEW OUTER EXPLODE(encounter.reason) b AS b "
            + "LATERAL VIEW OUTER EXPLODE(b.coding) e AS e"
            + ") f ON a.id = f.id "
            + "LEFT JOIN valueSet_006902c c ON e.system = c.system AND e.code = c.code "
            + "GROUP BY 1"
            + ") d ON patient.id = d.id "
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
      IOUtils.copy(response.getEntity().getContent(), writer, StandardCharsets.UTF_8);
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
        + "          \"valueString\": \"Globulin observation?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.result.resolve().code.memberOf('http://loinc.org/vs/LP14885-5')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String expectedSql =
        "SELECT e.result AS `Globulin observation?`, "
            + "COUNT(DISTINCT diagnosticreport.id) AS `Number of diagnostic reports` "
            + "FROM diagnosticreport "
            + "LEFT JOIN ("
            + "SELECT diagnosticreport.id, MAX(d.code) IS NULL AS result "
            + "FROM diagnosticreport "
            + "LEFT JOIN ("
            + "SELECT diagnosticreport.id, a.* FROM diagnosticreport "
            + "LATERAL VIEW OUTER EXPLODE(diagnosticreport.result) a AS a"
            + ") c ON diagnosticreport.id = c.id "
            + "LEFT JOIN observation b ON c.a.reference = b.id "
            + "LEFT JOIN ("
            + "SELECT observation.id, f.* "
            + "FROM observation "
            + "LATERAL VIEW OUTER EXPLODE(observation.code.coding) f AS f"
            + ") g ON b.id = g.id "
            + "LEFT JOIN valueSet_0377504 d ON f.system = d.system AND f.code = d.code "
            + "GROUP BY 1"
            + ") e ON diagnosticreport.id = e.id "
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
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.count()\"\n"
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
        + "          \"valueString\": \"%resource.multipleBirthInteger.max()\"\n"
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
        + "          \"valueString\": \"%resource.reverseResolve(MedicationRequest.subject).medicationCodeableConcept.memberOf('http://snomed.info/sct?fhir_vs=ecl/((* : << 30364011000036101|has Australian BoSS| = << 2338011000036107|metoprolol tartrate|) OR ((^ 929360041000036105|Trade product pack reference set| OR ^ 929360051000036108|Containered trade product pack reference set|) : 30409011000036107|has TPUU| = (* : << 30364011000036101|has Australian BoSS| = << 2338011000036107|metoprolol tartrate|)) OR (^ 929360081000036101|Medicinal product pack reference set| : 30348011000036104|has MPUU| = (* : << 30364011000036101|has Australian BoSS| = << 2338011000036107|metoprolol tartrate|)))')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String expectedSql =
        "SELECT c.result AS `Prescribed medication containing metoprolol tartrate?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients`, "
            + "MAX(patient.multipleBirthInteger) AS `Max multiple birth` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(b.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN medicationrequest a ON patient.id = a.subject.reference "
            + "LEFT JOIN ("
            + "SELECT medicationrequest.id, d.* FROM medicationrequest "
            + "LATERAL VIEW OUTER EXPLODE(medicationrequest.medicationCodeableConcept.coding) d AS d"
            + ") e ON a.id = e.id "
            + "LEFT JOIN valueSet_59eb431 b ON d.system = b.system AND d.code = b.code "
            + "GROUP BY 1"
            + ") c ON patient.id = c.id "
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
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
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
        + "          \"valueString\": \"Gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.gender\"\n"
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
        + "          \"valueString\": \"%resource.reverseResolve(MedicationRequest.subject).medicationCodeableConcept.memberOf('http://snomed.info/sct?fhir_vs=ecl/(<< 416897008|Tumour necrosis factor alpha inhibitor product| OR 408154002|Adalimumab 40mg injection solution 0.8mL prefilled syringe|)')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String expectedSql =
        "SELECT patient.gender AS `Gender`, "
            + "c.result AS `Prescribed TNF inhibitor?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(b.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN medicationrequest a ON patient.id = a.subject.reference "
            + "LEFT JOIN ("
            + "SELECT medicationrequest.id, d.* FROM medicationrequest "
            + "LATERAL VIEW OUTER EXPLODE(medicationrequest.medicationCodeableConcept.coding) d AS d"
            + ") e ON a.id = e.id "
            + "LEFT JOIN valueSet_8017b8d b ON d.system = b.system AND d.code = b.code "
            + "GROUP BY 1"
            + ") c ON patient.id = c.id "
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
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Condition\"\n"
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
        + "          \"valueString\": \"Number of conditions\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.code.coding.display\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Condition type\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.code.memberOf('http://snomed.info/sct?fhir_vs=ecl/<< 125605004')\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Is it a type of fracture?\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String expectedSql =
        "SELECT a.display AS `Condition type`, "
            + "c.result AS `Is it a type of fracture?`, "
            + "COUNT(DISTINCT condition.id) AS `Number of conditions` "
            + "FROM condition "
            + "LEFT JOIN ("
            + "SELECT condition.id, MAX(b.code) IS NULL AS result "
            + "FROM condition "
            + "LEFT JOIN ("
            + "SELECT condition.id, d.* "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) d AS d"
            + ") e ON condition.id = e.id "
            + "LEFT JOIN valueSet_ce36080 b ON d.system = b.system AND d.code = b.code "
            + "GROUP BY 1"
            + ") c ON condition.id = c.id "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) a AS a "
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
        + "  \"resourceType\": \"Parameters\",\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"subjectResource\",\n"
        + "      \"valueUri\": \"http://hl7.org/fhir/StructureDefinition/Patient\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Number of patients\"\n"
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
        + "          \"valueString\": \"Prescribed TNF inhibitor?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(MedicationRequest.subject).medicationCodeableConcept.memberOf('http://snomed.info/sct?fhir_vs=ecl/(<< 416897008|Tumour necrosis factor alpha inhibitor product| OR 408154002|Adalimumab 40mg injection solution 0.8mL prefilled syringe|)')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Got lung infection?\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Condition.subject).code.memberOf('http://snomed.info/sct?fhir_vs=ecl/< 64572001|Disease (disorder)| : (363698007|Finding site| = << 39607008|Lung structure|, 370135005|Pathological process| = << 441862004|Infectious process|)')\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.reverseResolve(Condition.subject).code.memberOf('http://snomed.info/sct?fhir_vs=ecl/< 64572001|Disease (disorder)| : (363698007|Finding site| = << 39352004|Joint structure|, 370135005|Pathological process| = << 263680009|Autoimmune process|)') and Patient.reverseResolve(Condition.subject).code.memberOf('http://snomed.info/sct?fhir_vs=ecl/< 64572001|Disease (disorder)| : (363698007|Finding site| = << 39607008|Lung structure|, 263502005|Clinical course| = << 90734009|Chronic|)')\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    String expectedSql =
        "SELECT c.result AS `Prescribed TNF inhibitor?`, h.result AS `Got lung infection?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(b.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN medicationrequest a ON patient.id = a.subject.reference "
            + "LEFT JOIN ("
            + "SELECT medicationrequest.id, d.* FROM medicationrequest "
            + "LATERAL VIEW OUTER EXPLODE(medicationrequest.medicationCodeableConcept.coding) d AS d"
            + ") e ON a.id = e.id "
            + "LEFT JOIN valueSet_8017b8d b ON d.system = b.system AND d.code = b.code "
            + "GROUP BY 1"
            + ") c ON patient.id = c.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(g.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN condition f ON patient.id = f.subject.reference "
            + "LEFT JOIN ("
            + "SELECT condition.id, i.* FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) i AS i"
            + ") j ON f.id = j.id "
            + "LEFT JOIN valueSet_269adee g ON i.system = g.system AND i.code = g.code "
            + "GROUP BY 1"
            + ") h ON patient.id = h.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(l.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN condition k ON patient.id = k.subject.reference "
            + "LEFT JOIN ("
            + "SELECT condition.id, n.* FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) n AS n"
            + ") o ON k.id = o.id "
            + "LEFT JOIN valueSet_04586e8 l ON n.system = l.system AND n.code = l.code "
            + "GROUP BY 1"
            + ") m ON patient.id = m.id "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(q.code) IS NULL AS result "
            + "FROM patient "
            + "LEFT JOIN condition p ON patient.id = p.subject.reference "
            + "LEFT JOIN ("
            + "SELECT condition.id, s.* FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) s AS s"
            + ") t ON p.id = t.id "
            + "LEFT JOIN valueSet_0d8179c q ON s.system = q.system AND s.code = q.code "
            + "GROUP BY 1"
            + ") r ON patient.id = r.id "
            + "WHERE m.result AND r.result "
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
