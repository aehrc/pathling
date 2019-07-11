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
import org.apache.spark.sql.catalog.Catalog;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
  @Ignore
  public void subsumedBy() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
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
        + "          \"valueString\": \"Patient.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Patient.reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|44054006)\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql1 = "SELECT DISTINCT patientConditionAsSubjectCodeCoding.system, "
        + "patientConditionAsSubjectCodeCoding.version, "
        + "patientConditionAsSubjectCodeCoding.code, "
        + "patientConditionAsSubjectCodeCoding.display, "
        + "patientConditionAsSubjectCodeCoding.userSelected "
        + "FROM patient "
        + "LEFT JOIN condition patientConditionAsSubject "
        + "ON patient.id = patientConditionAsSubject.subject.reference "
        + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding";

    String expectedSql2 =
        "SELECT patient.gender AS `Gender`, "
            + "patientConditionAsSubjectCodeCodingMembership.result AS `Diagnosed with type 2 diabetes?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX((patientConditionAsSubjectCodeCoding.system = 'http://snomed.info/sct' AND patientConditionAsSubjectCodeCoding.code = '44054006') OR (patientConditionAsSubjectCodeCodingClosure.equivalence IN ('subsumes', 'equal'))), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject "
            + "ON patient.id = patientConditionAsSubject.subject.reference "
            + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding "
            + "LEFT JOIN patientConditionAsSubjectCodeCodingClosure "
            + "ON patientConditionAsSubjectCodeCodingClosure.sourceSystem = 'http://snomed.info/sct' "
            + "AND patientConditionAsSubjectCodeCodingClosure.sourceCode = '44054006' "
            + "AND patientConditionAsSubjectCodeCoding.system = patientConditionAsSubjectCodeCodingClosure.targetSystem "
            + "AND patientConditionAsSubjectCodeCoding.code = patientConditionAsSubjectCodeCodingClosure.targetCode "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingMembership "
            + "ON patient.id = patientConditionAsSubjectCodeCodingMembership.id "
            + "GROUP BY 1, 2 "
            + "ORDER BY 1, 2, 3";

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
  @Ignore
  public void subsumes() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
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
        + "          \"valueString\": \"Patient.gender\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Gender\"\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"filter\",\n"
        + "      \"valueString\": \"Patient.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|9859006)\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"resourceType\": \"Parameters\"\n"
        + "}";

    String expectedSql1 = "SELECT DISTINCT patientConditionAsSubjectCodeCoding.system, "
        + "patientConditionAsSubjectCodeCoding.version, "
        + "patientConditionAsSubjectCodeCoding.code, "
        + "patientConditionAsSubjectCodeCoding.display, "
        + "patientConditionAsSubjectCodeCoding.userSelected "
        + "FROM patient "
        + "LEFT JOIN condition patientConditionAsSubject "
        + "ON patient.id = patientConditionAsSubject.subject.reference "
        + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding";

    String expectedSql2 =
        "SELECT patient.gender AS `Gender`, "
            + "patientConditionAsSubjectCodeCodingMembership.result AS `Diagnosed with type 2 diabetes?`, "
            + "COUNT(DISTINCT patient.id) AS `Number of patients` "
            + "FROM patient "
            + "LEFT JOIN ("
            + "SELECT patient.id, "
            + "IFNULL(MAX((patientConditionAsSubjectCodeCoding.system = 'http://snomed.info/sct' AND patientConditionAsSubjectCodeCoding.code = '44054006') OR (patientConditionAsSubjectCodeCodingClosure.equivalence IN ('subsumes', 'equal'))), FALSE) AS result "
            + "FROM patient "
            + "LEFT JOIN condition patientConditionAsSubject "
            + "ON patient.id = patientConditionAsSubject.subject.reference "
            + "LATERAL VIEW OUTER explode(patientConditionAsSubject.code.coding) patientConditionAsSubjectCodeCoding AS patientConditionAsSubjectCodeCoding "
            + "LEFT JOIN patientConditionAsSubjectCodeCodingClosure "
            + "ON patientConditionAsSubjectCodeCoding.system = patientConditionAsSubjectCodeCodingClosure.sourceSystem "
            + "AND patientConditionAsSubjectCodeCoding.code = patientConditionAsSubjectCodeCodingClosure.sourceCode "
            + "AND patientConditionAsSubjectCodeCodingClosure.targetSystem = 'http://snomed.info/sct' "
            + "AND patientConditionAsSubjectCodeCodingClosure.targetCode = '44054006' "
            + "GROUP BY 1"
            + ") patientConditionAsSubjectCodeCodingMembership "
            + "ON patient.id = patientConditionAsSubjectCodeCodingMembership.id "
            + "GROUP BY 1, 2 "
            + "ORDER BY 1, 2, 3";

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
  @Ignore
  public void kidgenExample() throws IOException {
    String inParams = "{\n"
        + "  \"parameter\": [\n"
        + "    {\n"
        + "      \"name\": \"aggregation\",\n"
        + "      \"part\": [\n"
        + "        {\n"
        + "          \"name\": \"expression\",\n"
        + "          \"valueString\": \"Patient.count()\"\n"
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
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|referral in $this.type).reverseResolve(Condition.context).verificationStatus\"\n"
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
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|pre-investigation in $this.type).reverseResolve(Condition.context).verificationStatus\"\n"
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
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|pre-investigation in $this.type).reverseResolve(Condition.context).code.subsumes(Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|referral in $this.type).reverseResolve(Condition.context).code)\"\n"
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
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject)    .where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|post-investigation in $this.type).reverseResolve(Condition.context).verificationStatus\"\n"
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
        + "          \"valueString\": \"Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|post-investigation in $this.type).reverseResolve(Condition.context).code.subsumedBy(Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type|pre-investigation in $this.type).reverseResolve(Condition.context).code)\"\n"
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
        + "LATERAL VIEW OUTER explode(encounterConditionAsContext.code.coding) encounterConditionAsContextCodeCoding AS encounterConditionAsContextCodeCoding "
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
        + "LATERAL VIEW OUTER explode(encounterProcedureRequestAsContext.reasonCode) encounterProcedureRequestAsContextReasonCode AS encounterProcedureRequestAsContextReasonCode "
        + "LATERAL VIEW OUTER explode(encounterProcedureRequestAsContextReasonCode.coding) encounterProcedureRequestAsContextReasonCodeCoding AS encounterProcedureRequestAsContextReasonCodeCoding";

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
            + "LATERAL VIEW OUTER explode(encounter.type) encounterType AS encounterType "
            + "LATERAL VIEW OUTER explode(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
            + "WHERE encounterTypeCoding.code = 'referral'"
            + ") patientEncounterAsSubjectReferral "
            + "ON patient.id = patientEncounterAsSubjectReferral.subject.reference "
            + "LEFT JOIN condition patientEncounterAsSubjectReferralConditionAsContext "
            + "ON patientEncounterAsSubjectReferral.id = patientEncounterAsSubjectReferralConditionAsContext.context.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) conditionCodeCoding AS conditionCodeCoding"
            + ") patientEncounterAsSubjectReferralConditionAsContextCodeCodingExploded "
            + "ON patientEncounterAsSubjectReferralConditionAsContext.id = patientEncounterAsSubjectReferralConditionAsContextCodeCodingExploded.id "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM encounter "
            + "LATERAL VIEW OUTER explode(encounter.type) encounterType AS encounterType "
            + "LATERAL VIEW OUTER explode(encounterType.coding) encounterTypeCoding AS encounterTypeCoding "
            + "WHERE encounterTypeCoding.code = 'pre-investigation'"
            + ") patientEncounterAsSubjectPreInvestigation "
            + "ON patient.id = patientEncounterAsSubjectPreInvestigation.subject.reference "
            + "LEFT JOIN condition patientEncounterAsSubjectPreInvestigationConditionAsContext "
            + "ON patientEncounterAsSubjectPreInvestigation.id = patientEncounterAsSubjectPreInvestigationConditionAsContext.context.reference "
            + "LEFT JOIN ("
            + "SELECT * "
            + "FROM condition "
            + "LATERAL VIEW OUTER explode(condition.code.coding) conditionCodeCoding AS conditionCodeCoding"
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
