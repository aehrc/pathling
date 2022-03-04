/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.parser.IParser;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Tag("UnitTest")
class BatchProviderTest {

  @Autowired
  IParser jsonParser;

  @Autowired
  Configuration configuration;

  BatchProvider batchProvider;
  UpdateHelpers updateHelpers;
  CacheInvalidator cacheInvalidator;

  @BeforeEach
  void setUp() {
    updateHelpers = mock(UpdateHelpers.class);
    cacheInvalidator = mock(CacheInvalidator.class);
    batchProvider = new BatchProvider(updateHelpers, cacheInvalidator, configuration);
  }

  @Test
  void mixedCreateUpdateResourceType() {
    batchProvider.batch(getBundle("mixedCreateUpdateResourceType"));
    verify(updateHelpers)
        .appendDataset(eq(ResourceType.PATIENT),
            argThat(resourceListMatcher(ResourceType.PATIENT, 2)));
    verify(updateHelpers)
        .updateDataset(eq(ResourceType.PRACTITIONER),
            argThat(resourceListMatcher(ResourceType.PRACTITIONER, 1)));
    verify(updateHelpers)
        .appendDataset(eq(ResourceType.ORGANIZATION),
            argThat(resourceListMatcher(ResourceType.ORGANIZATION, 1)));
    verify(cacheInvalidator).invalidateAll();
  }

  @Test
  void entryWithNoResource() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("entryWithNoResource")));
    assertEquals("Each batch entry must have a resource element", exception.getMessage());
  }

  @Test
  void entryWithNoRequest() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("entryWithNoRequest")));
    assertEquals("Each batch entry must have a request element", exception.getMessage());
  }

  @Test
  void unsupportedOperation() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("unsupportedOperation")));
    assertEquals("Only create and update operations are supported via batch",
        exception.getMessage());
  }

  @Test
  void createWithUnsupportedResource() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("createWithUnsupportedResource")));
    assertEquals(
        "The URL for a create request must be equal to the code of a supported resource type",
        exception.getMessage());
  }

  @Test
  void createWithUnknownResource() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("createWithUnknownResource")));
    assertEquals(
        "The URL for a create request must be equal to the code of a supported resource type",
        exception.getMessage());
  }

  @Test
  void createWithMismatchingResource() {
    final InvalidUserInputError exception = assertThrows(InvalidUserInputError.class,
        () -> batchProvider.batch(getBundle("createWithMismatchingResource")));
    assertEquals(
        "Resource in URL does not match resource type",
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
    final String json = TestHelpers.getResourceAsString(
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