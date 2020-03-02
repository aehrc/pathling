/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static au.csiro.pathling.TestUtilities.getSparkSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.AnalyticsServerConfiguration;
import au.csiro.pathling.fhir.FhirContextFactory;
import au.csiro.pathling.query.ResourceReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class ImportProviderTest {

  private ImportProvider importProvider;
  private SparkSession spark;
  private ResourceReader resourceReader;

  @Before
  public void setUp() throws IOException {
    spark = getSparkSession();
    Path warehouseDirectory = Files.createTempDirectory("pathling-test");

    AnalyticsServerConfiguration config = new AnalyticsServerConfiguration();
    config.setWarehouseUrl(warehouseDirectory.toString());
    config.setDatabaseName("test");

    resourceReader = mock(ResourceReader.class);
    ImportExecutor importExecutor = new ImportExecutor(config, spark, FhirEncoders.forR4().getOrCreate(),
        new FhirContextFactory(), resourceReader);
    importProvider = new ImportProvider(importExecutor);
  }

  @Test
  public void simpleImport() throws IOException, URISyntaxException {
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
    verify(resourceReader).updateAvailableResourceTypes();
  }

}
