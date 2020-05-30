/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static au.csiro.pathling.test.helpers.FhirHelpers.getJsonParser;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.TestHelpers;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("IntegrationTest")
class AggregateExecutorTest extends QueryExecutorTest {

  @Autowired
  private AggregateExecutor executor;

  @Autowired
  private SparkSession spark;

  @MockBean
  private ResourceReader resourceReader;

  @MockBean
  private TerminologyClient terminologyClient;

  @MockBean
  private TerminologyClientFactory terminologyClientFactory;

  @Test
  void simpleQuery() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Gender", "gender")
        .build();

    assertResponse("responses/AggregateExecutorTest-simpleQuery.Parameters.json",
        executor.execute(request));
  }

  @Test
  void multipleGroupingsAndAggregations() {
    final ResourceType subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(subjectResource, ResourceType.ORGANIZATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of encounters", "count()")
        .withAggregation("Number of reasons", "reasonCode.count()")
        .withGrouping("Class", "class.code")
        .withGrouping("Reason", "reasonCode.coding.display")
        .withFilter("status = 'finished'")
        .withFilter("serviceProvider.resolve().name = 'ST ELIZABETH\\'S MEDICAL CENTER'")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-multipleGroupingsAndAggregations.Parameters.json",
        executor.execute(request));
  }

  private static void assertResponse(@Nonnull final String expectedPath,
      @Nonnull final AggregateResponse response) {
    final Parameters parameters = response.toParameters();
    final String actualJson = getJsonParser().encodeResourceToString(parameters);
    assertJson(expectedPath, actualJson);
  }

  private void mockResourceReader(final ResourceType... resourceTypes) {
    TestHelpers.mockResourceReader(resourceReader, spark, resourceTypes);
  }

}