/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirEncoders;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class ImportProviderTest {

  private ImportProvider importProvider;
  private SparkSession spark;
  private IParser jsonParser;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .config("spark.driver.bindAddress", "localhost")
        .getOrCreate();
    Path warehouseDirectory = Files.createTempDirectory("clinsight-test");

    jsonParser = FhirContext.forR4().newJsonParser();

    AnalyticsServerConfiguration config = new AnalyticsServerConfiguration();
    config.setWarehouseUrl(warehouseDirectory.toString());
    config.setDatabaseName("test");

    importProvider = new ImportProvider(config, spark, FhirEncoders.forR4().getOrCreate());

    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    ResourceDefinitions.ensureInitialized(terminologyClient);
  }

  @Test
  public void simpleImport() throws IOException {
    URL importUrl = Thread.currentThread().getContextClassLoader()
        .getResource("test-data/fhir/Patient.ndjson");
    assertThat(importUrl).isNotNull();

    // Build the request Parameters resource.
    Parameters requestParameters = new Parameters();
    ParametersParameterComponent source = new ParametersParameterComponent(
        new StringType("source"));
    ParametersParameterComponent resourceType = new ParametersParameterComponent(
        new StringType("resourceType"));
    resourceType.setValue(new CodeType("Patient"));
    source.getPart().add(resourceType);
    ParametersParameterComponent url = new ParametersParameterComponent(new StringType("url"));
    url.setValue(new UrlType(importUrl.toString()));
    source.getPart().add(url);
    requestParameters.getParameter().add(source);

    OperationOutcome opOutcome = importProvider.importOperation(requestParameters);

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
