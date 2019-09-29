/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirEncoders;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.servlet.http.HttpServletRequest;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class ImportProviderTest {

  private ImportProvider importProvider;
  private SparkSession spark;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .getOrCreate();
    Path warehouseDirectory = Files.createTempDirectory("clinsight-test");

    AnalyticsServerConfiguration config = new AnalyticsServerConfiguration();
    config.setWarehouseUrl(warehouseDirectory.toString());
    config.setDatabaseName("test");

    importProvider = new ImportProvider(config, spark, FhirContext.forR4(),
        FhirEncoders.forR4().getOrCreate());

    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    ResourceDefinitions.ensureInitialized(terminologyClient);
  }

  @Test
  public void simpleImport() throws IOException {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("Content-Type")).thenReturn("application/json");
    URL importUrl = Thread.currentThread().getContextClassLoader()
        .getResource("test-data/fhir/Patient.ndjson");
    assertThat(importUrl).isNotNull();
    String requestJson = "{\n"
        + "  \"inputFormat\": \"application/fhir+ndjson\",\n"
        + "  \"inputs\": [\n"
        + "    {\n"
        + "      \"type\": \"Patient\",\n"
        + "      \"url\": \"" + importUrl.toString() + "\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    BufferedReader requestReader = new BufferedReader(new StringReader(requestJson));
    when(request.getReader()).thenReturn(requestReader);

    OperationOutcome opOutcome = importProvider.importOperation(request);

    assertThat(opOutcome).isNotNull();
    OperationOutcomeIssueComponent issue = opOutcome.getIssueFirstRep();
    assertThat(opOutcome).isNotNull();
    assertThat(issue.getSeverity()).isEqualTo(IssueSeverity.INFORMATION);
    assertThat(issue.getCode()).isEqualTo(IssueType.INFORMATIONAL);
    assertThat(issue.getDiagnostics()).isEqualTo("Data import completed successfully");
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }
}
