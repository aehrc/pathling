/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import static au.csiro.pathling.TestUtilities.getFhirContext;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SearchProviderTest extends ExecutorTest {

  private ExecutorConfiguration configuration;
  private FhirContext fhirContext;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Create and configure a new AggregateExecutor.
    Path warehouseDirectory = Files.createTempDirectory("pathling-test-");
    configuration = new ExecutorConfiguration(spark,
        getFhirContext(), null, null, mockReader);
    configuration.setWarehouseUrl(warehouseDirectory.toString());
    configuration.setDatabaseName("test");
    configuration.setFhirEncoders(FhirEncoders.forR4().getOrCreate());

    fhirContext = getFhirContext();
  }

  @Test
  public void throwsInvalidRequestError() throws MalformedURLException {
    mockResourceReader(ResourceType.CAREPLAN);

    StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("status = 'completed"));

    ResourceType subjectResource = ResourceType.CAREPLAN;
    Class<? extends IBaseResource> resourceTypeClass = fhirContext
        .getResourceDefinition(subjectResource.name()).getImplementingClass();
    SearchProvider searchProvider = new SearchProvider(configuration, resourceTypeClass);

    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> searchProvider.search(filters))
        .withMessage(
            "Error parsing FHIRPath expression: mismatched input '<EOF>' expecting {'+', '-', 'is', 'as', 'contains', '(', '{', 'true', 'false', '%', '$this', CODING, DATETIME, TIME, IDENTIFIER, QUOTEDIDENTIFIER, STRING, NUMBER}");
  }

}