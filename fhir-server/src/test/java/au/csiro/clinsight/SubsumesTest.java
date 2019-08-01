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
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.TargetElementComponent;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class SubsumesTest {

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
  public void subsumes() throws IOException, JSONException {
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
        + "          \"valueString\": \"%resource.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|9859006)\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

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
        + "          \"valueUnsignedInt\": 145\n"
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
        + "          \"valueUnsignedInt\": 49\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    String expectedSql1 = "SELECT b.system, b.code "
        + "FROM patient "
        + "LEFT JOIN condition a ON patient.id = a.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(a.code.coding) b AS b";

    String expectedSql2 =
        "SELECT patient.gender AS `Gender`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(c.equivalence IN ('subsumes', 'equal')) AS result "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, b.* FROM patient "
            + "LEFT JOIN condition a ON patient.id = a.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(a.code.coding) b AS b"
            + ") e ON patient.id = e.id "
            + "LEFT JOIN closure_a245083 c "
            + "ON e.b.system = c.targetSystem "
            + "AND e.b.code = c.targetCode "
            + "AND 'http://snomed.info/sct' = c.sourceSystem "
            + "AND '9859006' = c.sourceCode "
            + "GROUP BY 1"
            + ") d ON patient.id = d.id "
            + "WHERE d.result "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    StructField[] fields1 = {
        new StructField("system", DataTypes.StringType, true, null),
        new StructField("code", DataTypes.StringType, true, null)
    };
    StructType structType1 = new StructType(fields1);
    List<Row> fakeResult1 = new ArrayList<>(Arrays.asList(
        new GenericRowWithSchema(
            new Object[]{"https://csiro.au/fhir/CodeSystem/foods", "zucchini"}, structType1),
        new GenericRowWithSchema(
            new Object[]{"https://csiro.au/fhir/CodeSystem/foods", "fruit"}, structType1),
        new GenericRowWithSchema(
            new Object[]{"https://csiro.au/fhir/CodeSystem/foods", "vegetable"}, structType1)));

    Dataset mockDataset1 = createMockDataset();
    when(mockSpark.sql(expectedSql1)).thenReturn(mockDataset1);
    when(mockDataset1.collectAsList()).thenReturn(fakeResult1);

    StructField[] fields2 = {
        new StructField("Gender", DataTypes.StringType, true, null),
        new StructField("Number of patients", DataTypes.LongType, true, null)
    };
    StructType structType2 = new StructType(fields2);
    List<Row> fakeResult2 = new ArrayList<>(Arrays.asList(
        new GenericRowWithSchema(new Object[]{"female", 145L}, structType2),
        new GenericRowWithSchema(new Object[]{"male", 49L}, structType2)
    ));

    Dataset mockDataset2 = createMockDataset();
    when(mockSpark.createDataset(any(List.class), any(Encoder.class))).thenReturn(mockDataset2);

    Dataset mockDataset3 = createMockDataset();
    when(mockSpark.sql(expectedSql2)).thenReturn(mockDataset3);
    when(mockDataset3.collectAsList()).thenReturn(fakeResult2);

    ConceptMap fakeConceptMap = new ConceptMap();
    ConceptMapGroupComponent group = new ConceptMapGroupComponent();
    group.setSource("https://csiro.au/fhir/CodeSystem/foods");
    group.setTarget("https://csiro.au/fhir/CodeSystem/foods");
    fakeConceptMap.getGroup().add(group);

    SourceElementComponent element = new SourceElementComponent();
    element.setCode("zucchini");
    TargetElementComponent target = new TargetElementComponent();
    target.setCode("vegetable");
    element.getTarget().add(target);
    group.getElement().add(element);

    element = new SourceElementComponent();
    element.setCode("zucchini");
    target = new TargetElementComponent();
    target.setCode("fruit");
    element.getTarget().add(target);
    group.getElement().add(element);

    when(mockTerminologyClient.closure(any(), any(), any())).thenReturn(fakeConceptMap);

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, StandardCharsets.UTF_8);
      JSONAssert.assertEquals(expectedResponse, writer.toString(), true);
    }

    verify(mockDataset2).createOrReplaceTempView("closure_a245083");
    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql1);
    verify(mockSpark).sql(expectedSql2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void subsumedBy() throws IOException {
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
        + "          \"valueString\": \"%resource.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"%resource.reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|44054006)\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql1 = "SELECT b.system, b.code "
        + "FROM patient "
        + "LEFT JOIN condition a ON patient.id = a.subject.reference "
        + "LATERAL VIEW OUTER EXPLODE(a.code.coding) b AS b";

    String expectedSql2 =
        "SELECT patient.gender AS `Gender`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, MAX(c.equivalence IN ('subsumes', 'equal')) AS result "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, b.* FROM patient "
            + "LEFT JOIN condition a ON patient.id = a.subject.reference "
            + "LATERAL VIEW OUTER EXPLODE(a.code.coding) b AS b"
            + ") e ON patient.id = e.id "
            + "LEFT JOIN closure_a245083 c "
            + "ON 'http://snomed.info/sct' = c.sourceSystem "
            + "AND '9859006' = c.sourceCode "
            + "AND e.b.system = c.targetSystem "
            + "AND e.b.code = c.targetCode "
            + "GROUP BY 1"
            + ") d ON patient.id = d.id "
            + "WHERE d.result "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql1);
    verify(mockSpark).sql(expectedSql2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void kidgenExample() throws IOException {
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
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|referral in $this.type).reverseResolve(Condition.context).verificationStatus\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Referral verification status\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|pre-investigation in $this.type).reverseResolve(Condition.context).verificationStatus\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Pre-investigation verification status\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|pre-investigation in $this.type).reverseResolve(Condition.context).code.subsumes(%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|referral in $this.type).reverseResolve(Condition.context).code)\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Pre-investigation diagnosis more specific than referral diagnosis?\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|post-investigation in $this.type).reverseResolve(Condition.context).verificationStatus\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Post-investigation verification status\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"grouping\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|post-investigation in $this.type).reverseResolve(Condition.context).code.subsumedBy(%resource.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|pre-investigation in $this.type).reverseResolve(Condition.context).code)\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Post-investigation diagnosis more specific than pre-investigation diagnosis?\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql1 = "SELECT encounterConditionAsContextCodeCoding.system, "
        + "encounterConditionAsContextCodeCoding.version, "
        + "encounterConditionAsContextCodeCoding.code, "
        + "encounterConditionAsContextCodeCoding.display, "
        + "encounterConditionAsContextCodeCoding.userSelected "
        + "FROM patient "
        + "LEFT JOIN encounter patientEncounterAsSubject "
        + "ON patient.id = patientEncounterAsSubject.subject.reference "
        + "LEFT JOIN condition encounterConditionAsContext "
        + "ON patientEncounterAsSubject.id = encounterConditionAsContext.context.reference "
        + "LATERAL VIEW OUTER EXPLODE(encounterConditionAsContext.code.coding) encounterConditionAsContextCodeCoding AS encounterConditionAsContextCodeCoding "
        + "UNION "
        + "SELECT encounterProcedureRequestAsContextReasonCodeCoding.system, "
        + "encounterProcedureRequestAsContextReasonCodeCoding.version, "
        + "encounterProcedureRequestAsContextReasonCodeCoding.code, "
        + "encounterProcedureRequestAsContextReasonCodeCoding.display, "
        + "encounterProcedureRequestAsContextReasonCodeCoding.userSelected "
        + "FROM patient "
        + "LEFT JOIN encounter patientEncounterAsSubject "
        + "ON patient.id = patientEncounterAsSubject.subject.reference "
        + "LEFT JOIN procedurerequest encounterProcedureRequestAsContext "
        + "ON patientEncounterAsSubject.id = encounterProcedureRequestAsContext.context.reference "
        + "LATERAL VIEW OUTER EXPLODE(encounterProcedureRequestAsContext.reasonCode) encounterProcedureRequestAsContextReasonCode AS encounterProcedureRequestAsContextReasonCode "
        + "LATERAL VIEW OUTER EXPLODE(encounterProcedureRequestAsContextReasonCode.coding) encounterProcedureRequestAsContextReasonCodeCoding AS encounterProcedureRequestAsContextReasonCodeCoding";

    String expectedSql2 =
        "SELECT patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosureInResult.inResult AS `Pre-investigation diagnosis more specific than referral diagnosis?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "MAX(CASE WHEN patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure.equivalence IN ('subsumes', 'equal') THEN TRUE ELSE FALSE END) AS inResult "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM encounter "
            + "LATERAL VIEW OUTER EXPLODE(encounter.type) encounterType AS encounterType "
            + "LATERAL VIEW OUTER EXPLODE(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
            + "WHERE encounterTypeCoding.code = 'referral'"
            + ") patientEncounterAsSubjectReferral "
            + "ON patient.id = patientEncounterAsSubjectReferral.subject.reference "
            + "LEFT JOIN condition patientEncounterAsSubjectReferralConditionAsContext "
            + "ON patientEncounterAsSubjectReferral.id = patientEncounterAsSubjectReferralConditionAsContext.context.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) conditionCodeCoding AS conditionCodeCoding"
            + ") patientEncounterAsSubjectReferralConditionAsContextCodeCodingExploded "
            + "ON patientEncounterAsSubjectReferralConditionAsContext.id = patientEncounterAsSubjectReferralConditionAsContextCodeCodingExploded.id "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM encounter "
            + "LATERAL VIEW OUTER EXPLODE(encounter.type) encounterType AS encounterType "
            + "LATERAL VIEW OUTER EXPLODE(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
            + "WHERE encounterTypeCoding.code = 'pre-investigation'"
            + ") patientEncounterAsSubjectPreInvestigation "
            + "ON patient.id = patientEncounterAsSubjectPreInvestigation.subject.reference "
            + "LEFT JOIN condition patientEncounterAsSubjectPreInvestigationConditionAsContext "
            + "ON patientEncounterAsSubjectPreInvestigation.id = patientEncounterAsSubjectPreInvestigationConditionAsContext.context.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER EXPLODE(condition.code.coding) conditionCodeCoding AS conditionCodeCoding"
            + ") patientEncounterAsSubjectPreInvestigationConditionAsContextCodeCodingExploded "
            + "ON patientEncounterAsSubjectPreInvestigationConditionAsContext.id = patientEncounterAsSubjectPreInvestigationConditionAsContextCodeCodingExploded.id "
            + "LEFT JOIN `closure_simpleQuery` patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure "
            + "ON patientEncounterAsSubjectReferralConditionAsContextCodeCodingExploded.conditionCodeCoding.system = patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure.targetSystem "
            + "AND patientEncounterAsSubjectReferralConditionAsContextCodeCodingExploded.conditionCodeCoding.code = patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure.targetCode "
            + "AND patientEncounterAsSubjectPreInvestigationConditionAsContextCodeCodingExploded.conditionCodeCoding.system = patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure.sourceSystem "
            + "AND patientEncounterAsSubjectPreInvestigationConditionAsContextCodeCodingExploded.conditionCodeCoding.code = patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure.sourceCode "
            + "GROUP BY 1"
            + ") patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosureInResult "
            + "ON patient.id = patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosureInResult.id "
            + "GROUP BY 1 "
            + "ORDER BY 1, 2";

    Dataset mockDataset = createMockDataset();
    when(mockSpark.sql(any())).thenReturn(mockDataset);
    when(mockDataset.collectAsList()).thenReturn(new ArrayList());

    HttpPost httpPost = postFhirResource(inParams, QUERY_URL);
    httpClient.execute(httpPost);

    verify(mockSpark).sql("USE clinsight");
    verify(mockSpark).sql(expectedSql1);
    verify(mockSpark).sql(expectedSql2);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    httpClient.close();
  }

}
