/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration.modification;

import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

public class BatchTest extends ModificationTest {

  static final List<ResourceType> RESOURCE_TYPES = List.of(ResourceType.PATIENT,
      ResourceType.PRACTITIONER, ResourceType.ORGANIZATION);

  static final Map<ResourceType, Integer> expectedCounts = new HashMap<>();

  static {
    expectedCounts.put(ResourceType.PATIENT, 9);
    expectedCounts.put(ResourceType.PRACTITIONER, 29);
    expectedCounts.put(ResourceType.ORGANIZATION, 23);
  }

  @Test
  void batch() throws URISyntaxException {
    // Check the counts for each resource type.
    for (final ResourceType resourceType : RESOURCE_TYPES) {
      assertResourceCount(resourceType, expectedCounts.get(resourceType));
    }

    // Send a batch request with a new Patient, Practitioner and Organization resource.
    final String request = getResourceAsString("requests/BatchTest/batch.Bundle.json");
    final Bundle requestBundle = (Bundle) jsonParser.parseResource(request);
    final String url = "http://localhost:" + port + "/fhir";
    final ResponseEntity<String> response = restTemplate
        .exchange(url, HttpMethod.POST, RequestEntity.put(new URI(url))
            .contentType(FHIR_MEDIA_TYPE)
            .accept(FHIR_MEDIA_TYPE)
            .body(request), String.class);
    assertEquals(200, response.getStatusCode().value());
    assertNotNull(response.getBody());

    // Check the response bundle is successful and matches the requests.
    final Bundle responseBundle = (Bundle) jsonParser.parseResource(response.getBody());
    assertEquals(requestBundle.getEntry().size(), responseBundle.getEntry().size());
    for (final BundleEntryComponent entry : responseBundle.getEntry()) {
      assertTrue(entry.getResponse().getStatus().startsWith("200"));
      assertNotNull(entry.getResource());
      final String resourceId = entry.getResource().getIdElement().getIdPart();
      final ResourceType resourceType = ResourceType.fromCode(
          entry.getResource().getResourceType().toString());
      getResourceResult(resourceType, resourceId);
    }

    // Check that the new resource counts are one greater than before the operation.
    for (final ResourceType resourceType : RESOURCE_TYPES) {
      assertResourceCount(resourceType, expectedCounts.get(resourceType) + 1);
    }
  }

}
