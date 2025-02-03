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

package au.csiro.pathling.security;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.ResourceProviderFactory;
import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.update.BatchProvider;
import au.csiro.pathling.update.ImportProvider;
import au.csiro.pathling.update.UpdateProvider;
import ca.uhn.fhir.parser.IParser;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({"core", "server", "unit-test"})
@TestPropertySource(properties = {"pathling.async.enabled=false"})
abstract class SecurityTestForOperations extends SecurityTest {

  @Autowired
  ImportProvider importProvider;

  @Autowired
  ResourceProviderFactory resourceProviderFactory;

  @Autowired
  BatchProvider batchProvider;

  @MockBean
  CacheableDatabase database;

  @Autowired
  SparkSession sparkSession;

  @Autowired
  IParser jsonParser;

  @BeforeEach
  void setUp() {
    when(database.read(any(Enumerations.ResourceType.class)))
        .thenReturn(new ResourceDatasetBuilder(sparkSession)
            .withIdColumn()
            .withColumn("id_versioned", DataTypes.StringType)
            .build());
  }

  void assertImportSuccess() {
    try {
      importProvider.importOperation(new Parameters(), null);
    } catch (final InvalidUserInputError ex) {
      // pass
    }
  }

  void assertAggregateSuccess() {
    final AggregateProvider aggregateProvider = (AggregateProvider) resourceProviderFactory
        .createAggregateResourceProvider(ResourceType.Patient);
    try {
      aggregateProvider.aggregate(null, null, null, null);
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
    final String json = getResourceAsString(
        "requests/BatchProviderTest/mixedResourceTypes.Bundle.json");
    final Bundle bundle = (Bundle) jsonParser.parseResource(json);
    batchProvider.batch(bundle);
  }

}
