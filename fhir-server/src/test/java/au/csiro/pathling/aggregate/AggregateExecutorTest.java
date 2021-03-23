/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateResponse.Grouping;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.search.SearchExecutor;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
public abstract class AggregateExecutorTest {

  @Autowired
  protected SparkSession spark;

  @Autowired
  protected TerminologyClient terminologyClient;

  @Autowired
  protected TerminologyService terminologyService;

  @Autowired
  protected TerminologyClientFactory terminologyClientFactory;

  @Autowired
  protected Configuration configuration;

  @Autowired
  protected FhirContext fhirContext;

  @Autowired
  protected IParser jsonParser;

  @Autowired
  protected FhirEncoders fhirEncoders;

  protected AggregateExecutor executor;
  protected ResourceType subjectResource;
  protected ResourceReader resourceReader;
  protected AggregateResponse response = null;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    resourceReader = mock(ResourceReader.class);
    executor = new FreshAggregateExecutor(configuration, fhirContext, spark, resourceReader,
        Optional.of(terminologyClient), Optional.of(terminologyClientFactory));
  }

  /**
   * Test that the drill down expression from the first grouping from each aggregate result can be
   * successfully executed using the FHIRPath search.
   */
  @AfterEach
  public void runFirstGroupingThroughSearch() {
    if (response != null) {
      final Optional<Grouping> firstGroupingOptional = response.getGroupings()
          .stream()
          .filter(grouping -> grouping.getDrillDown().isPresent())
          .findFirst();

      if (firstGroupingOptional.isPresent()) {
        final Grouping firstGrouping = firstGroupingOptional.get();
        assertTrue(firstGrouping.getDrillDown().isPresent());
        final String drillDown = firstGrouping.getDrillDown().get();
        final StringAndListParam filters = new StringAndListParam();
        filters.addAnd(new StringParam(drillDown));
        final IBundleProvider searchExecutor = new SearchExecutor(configuration, fhirContext, spark,
            resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory),
            fhirEncoders, subjectResource, Optional.of(filters));
        final List<IBaseResource> resources = searchExecutor.getResources(0, 100);
        assertTrue(resources.size() > 0);
      }
    }
  }

  protected void assertResponse(@Nonnull final String expectedPath,
      @Nonnull final AggregateResponse response) {
    final Parameters parameters = response.toParameters();
    final String actualJson = jsonParser.encodeResourceToString(parameters);
    assertJson("responses/" + expectedPath, actualJson);
  }

  protected void mockResourceReader(final ResourceType... resourceTypes) {
    TestHelpers.mockResourceReader(resourceReader, spark, resourceTypes);
  }

}
