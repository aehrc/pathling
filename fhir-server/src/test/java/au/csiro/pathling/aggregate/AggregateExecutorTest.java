/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.TestResources.assertJson;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.UnitTestDependencies;
import au.csiro.pathling.aggregate.AggregateResponse.Grouping;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.search.SearchExecutor;
import au.csiro.pathling.search.SearchExecutorTest.EncoderConfig;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@SpringBootTest(classes = {EncoderConfig.class, UnitTestDependencies.class})
abstract class AggregateExecutorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  TerminologyService terminologyService;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  QueryConfiguration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  IParser jsonParser;

  @Autowired
  FhirEncoders fhirEncoders;

  @MockBean
  CacheableDatabase database;

  AggregateExecutor executor;
  ResourceType subjectResource;
  AggregateResponse response = null;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
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
        assertFalse(resources.isEmpty());
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

}
