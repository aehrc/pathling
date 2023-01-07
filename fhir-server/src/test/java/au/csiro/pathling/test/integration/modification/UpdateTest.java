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

package au.csiro.pathling.test.integration.modification;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import au.csiro.pathling.test.helpers.TestHelpers;
import java.net.URI;
import java.net.URISyntaxException;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;

class UpdateTest extends ModificationTest {

  static final String EXISTING_PATIENT_ID = "8ee183e2-b3c0-4151-be94-b945d6aa8c6d";
  static final String NEW_PATIENT_ID = "foo";

  @Test
  void update() throws URISyntaxException {
    // Check the total Patient count.
    assertResourceCount(ResourceType.PATIENT, 9);

    // Send an update request with a modified Patient resource.
    final String request = getResourceAsString("requests/UpdateTest/update.Patient.json");
    final String url = "http://localhost:" + port + "/fhir/Patient/" + EXISTING_PATIENT_ID;
    final ResponseEntity<String> response = restTemplate
        .exchange(url, HttpMethod.PUT, RequestEntity.put(new URI(url))
            .contentType(TestHelpers.FHIR_MEDIA_TYPE)
            .accept(TestHelpers.FHIR_MEDIA_TYPE)
            .body(request), String.class);
    assertEquals(200, response.getStatusCode().value());
    assertNotNull(response.getBody());
    JSONAssert.assertEquals(request, response.getBody(), JSONCompareMode.LENIENT);

    // Get the new patient resource via search and verify its contents.
    final BundleEntryComponent bundleEntryComponent = getResourceResult(ResourceType.PATIENT,
        EXISTING_PATIENT_ID
    );

    // Verify that the Patient resource has been updated.
    final Patient searchResultPatient = (Patient) bundleEntryComponent.getResource();
    assertEquals("female", searchResultPatient.getGender().toCode());

    // Check that the new Patient count is the same as it was before the operation.
    assertResourceCount(ResourceType.PATIENT, 9);
  }

  @Test
  void createWithId() throws URISyntaxException {
    // Check the total Patient count.
    assertResourceCount(ResourceType.PATIENT, 9);

    // Send an update request with a modified Patient resource.
    final String request = getResourceAsString("requests/UpdateTest/createWithId.Patient.json");
    final String url = "http://localhost:" + port + "/fhir/Patient/" + NEW_PATIENT_ID;
    final ResponseEntity<String> response = restTemplate
        .exchange(url, HttpMethod.PUT, RequestEntity.put(new URI(url))
            .contentType(TestHelpers.FHIR_MEDIA_TYPE)
            .accept(TestHelpers.FHIR_MEDIA_TYPE)
            .body(request), String.class);
    assertEquals(200, response.getStatusCode().value());
    assertNotNull(response.getBody());
    JSONAssert.assertEquals(request, response.getBody(), JSONCompareMode.LENIENT);

    // Get the new patient resource via search and verify its contents.
    final Patient patient = (Patient) jsonParser.parseResource(response.getBody());
    final String patientId = patient.getIdElement().getIdPart().replace("Patient/", "");
    getResourceResult(ResourceType.PATIENT, patientId);

    // Check that the new Patient count is now one more than it was previously.
    assertResourceCount(ResourceType.PATIENT, 10);
  }

}
