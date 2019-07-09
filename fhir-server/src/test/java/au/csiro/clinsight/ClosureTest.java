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
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class ClosureTest {

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
  public void simpleQuery() throws IOException, JSONException {
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
        + "          \"valueString\": \"'subsumes' in Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|pre-investigation in $this.type.coding).reverseResolve(Condition.context).code.coding.closure(Patient.reverseResolve(Encounter.subject).where(https://csiro.au/fhir/CodeSystem/kidgen-pilot-encounter-type-0|referral in $this.type.coding).reverseResolve(Condition.context).code.coding)\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"name\": \"label\",\n"
        + "          \"valueString\": \"Pre-investigation diagnosis more specific than referral diagnosis?\"\n"
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
            + "MAX(CASE WHEN patientEncounterAsSubjectConditionAsContextReferralPreInvestigationClosure.equivalence = 'subsumes' THEN TRUE ELSE FALSE END) AS inResult "
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
