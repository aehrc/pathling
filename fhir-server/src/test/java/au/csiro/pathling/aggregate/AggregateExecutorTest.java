/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.TestResources.assertJson;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.aggregate.AggregateResponse.Grouping;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.Database;
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
abstract class AggregateExecutorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  TerminologyService terminologyService;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  Configuration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  IParser jsonParser;

  @Autowired
  FhirEncoders fhirEncoders;

  AggregateExecutor executor;
  ResourceType subjectResource;
  Database database;
  AggregateResponse response = null;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    database = mock(Database.class);
    executor = new AggregateExecutor(configuration, fhirContext, spark, database,
        Optional.of(terminologyServiceFactory));
  }

  /**
   * Test that the drill-down expression from the first grouping from each aggregate result can be
   * successfully executed using the FHIRPath search.
   */
  @AfterEach
  void runFirstGroupingThroughSearch() {
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
            database, Optional.of(terminologyServiceFactory),
            fhirEncoders, subjectResource, Optional.of(filters));
        final List<IBaseResource> resources = searchExecutor.getResources(0, 100);
        assertTrue(resources.size() > 0);
      }
    }
  }

  void assertResponse(@Nonnull final String expectedPath,
      @Nonnull final AggregateResponse response) {
    final Parameters parameters = response.toParameters();
    final String actualJson = jsonParser.encodeResourceToString(parameters);
    assertJson("responses/" + expectedPath, actualJson);
  }

  void mockResource(final ResourceType... resourceTypes) {
    TestHelpers.mockResource(database, spark, resourceTypes);
  }

  void mockEmptyResource(final ResourceType... resourceType) {
    TestHelpers.mockEmptyResource(database, spark, fhirEncoders, resourceType);
  }

}
