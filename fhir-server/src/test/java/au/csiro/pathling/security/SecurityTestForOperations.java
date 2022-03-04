/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.ResourceProviderFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import au.csiro.pathling.update.BatchProvider;
import au.csiro.pathling.update.ImportProvider;
import au.csiro.pathling.update.UpdateHelpers;
import au.csiro.pathling.update.UpdateProvider;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"core", "server", "unit-test"})
public abstract class SecurityTestForOperations extends SecurityTest {

  @Autowired
  ImportProvider importProvider;

  @Autowired
  ResourceProviderFactory resourceProviderFactory;

  @Autowired
  BatchProvider batchProvider;

  @MockBean
  ResourceReader resourceReader;

  @MockBean
  UpdateHelpers updateHelpers;

  @MockBean
  CacheInvalidator cacheInvalidator;

  @Autowired
  SparkSession sparkSession;

  @Autowired
  IParser jsonParser;

  @BeforeEach
  void setUp() {
    when(resourceReader.read(any()))
        .thenReturn(new ResourceDatasetBuilder(sparkSession).withIdColumn().build());
  }

  void assertImportSuccess() {
    try {
      importProvider.importOperation(new Parameters(), null, null, null);
    } catch (final InvalidUserInputError ex) {
      // pass
    }
  }

  void assertAggregateSuccess() {
    final AggregateProvider aggregateProvider = (AggregateProvider) resourceProviderFactory
        .createAggregateResourceProvider(ResourceType.Patient);
    try {
      aggregateProvider.aggregate(null, null, null, null, null, null);
    } catch (final InvalidUserInputError ex) {
      // pass
    }
  }

  void assertSearchSuccess() {
    final SearchProvider searchProvider = resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient);
    searchProvider.search(null);
  }

  void assertSearchWithFilterSuccess() {
    final SearchProvider searchProvider = resourceProviderFactory
        .createSearchResourceProvider(ResourceType.Patient);
    searchProvider.search(null);
  }

  void assertCreateSuccess() {
    final UpdateProvider updateProvider = resourceProviderFactory.createUpdateResourceProvider(
        ResourceType.Patient);
    try {
      updateProvider.create(null);
    } catch (final InvalidUserInputError e) {
      // pass
    }
  }

  void assertUpdateSuccess() {
    final UpdateProvider updateProvider = resourceProviderFactory.createUpdateResourceProvider(
        ResourceType.Patient);
    try {
      updateProvider.update(null, null);
    } catch (final InvalidUserInputError e) {
      // pass
    }
  }

  void assertBatchSuccess() {
    final String json = TestHelpers.getResourceAsString(
        "requests/BatchProviderTest/mixedCreateUpdateResourceType.Bundle.json");
    final Bundle bundle = (Bundle) jsonParser.parseResource(json);
    batchProvider.batch(bundle);
  }

}
