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

package au.csiro.pathling.update;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.io.CacheableDatabase;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@Tag("UnitTest")
@ActiveProfiles("unit-test")
class BatchProviderTest {

  @Autowired
  IParser jsonParser;

  @Autowired
  ServerConfiguration configuration;

  @MockBean
  CacheableDatabase database;

  BatchProvider batchProvider;

  @BeforeEach
  void setUp() {
    batchProvider = new BatchProvider(database, configuration);
  }

  @Test
  void mixedResourceTypes() {
    batchProvider.batch(getBundle("mixedResourceTypes"));
    verify(database)
        .merge(eq(ResourceType.PATIENT),
            argThat(resourceListMatcher(ResourceType.PATIENT, 2)));
    verify(database)
        .merge(eq(ResourceType.PRACTITIONER),
            argThat(resourceListMatcher(ResourceType.PRACTITIONER, 1)));
    verify(database)
        .merge(eq(ResourceType.ORGANIZATION),
            argThat(resourceListMatcher(ResourceType.ORGANIZATION, 1)));
  }

  @Test
  void entryWithNoRequest() {
    batchProvider.batch(getBundle("entryWithNoRequest"));
    verifyNoInteractions(database);
  }

  @Test
  void entryWithNoResource() {
    batchProvider.batch(getBundle("entryWithNoResource"));
    verifyNoInteractions(database);
  }

  @Test
  void unsupportedOperation() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("unsupportedOperation")));
    assertEquals("Only update requests are supported for use within the batch operation",
        exception.getMessage());
  }

  @Test
  void updateWithUnsupportedResource() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("updateWithUnsupportedResource")));
    assertEquals(
        "The URL for an update request must refer to the code of a supported resource "
            + "type, and must look like this: [resource type]/[id]",
        exception.getMessage());
  }

  @Test
  void updateWithMismatchingResource() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("updateWithMismatchingResource")));
    assertEquals(
        "Resource in URL does not match resource type",
        exception.getMessage());
  }

  @Test
  void updateWithMismatchingIds() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("updateWithMismatchingIds")));
    assertEquals(
        "Resource ID missing or does not match supplied ID",
        exception.getMessage());
  }

  @Nonnull
  Bundle getBundle(@Nonnull final String name) {
    final String json = getResourceAsString(
        "requests/BatchProviderTest/" + name + ".Bundle.json");
    return (Bundle) jsonParser.parseResource(json);
  }

  @Nonnull
  ArgumentMatcher<List<IBaseResource>> resourceListMatcher(@Nonnull final ResourceType resourceType,
      final int size) {
    final String resourceCode = resourceType.toCode();
    return list -> {
      final boolean resourceTypesMatch = list.stream()
          .map(IBaseResource::fhirType)
          .anyMatch(resourceCode::equals);
      final boolean listSizeMatches = list.size() == size;
      return resourceTypesMatch && listSizeMatches;
    };
  }

}
