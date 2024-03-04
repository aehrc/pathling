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

package au.csiro.pathling.export.ws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class BulkExportTemplateTest {

  @Test
  void testDefaultRequestUri() throws Exception {
    final URI baseUri = URI.create("http://example.com/fhir");
    assertEquals(URI.create("http://example.com/fhir?_outputFormat=ndjson"),
        BulkExportTemplate.toRequestURI(baseUri, BulkExportRequest.builder().build())
    );
  }

  @Test
  void testNonDefaultRequestUri() throws Exception {
    final URI baseUri = URI.create("http://test.com/fhir");
    final Instant testInstant = Instant.parse("2023-01-11T00:00:00.1234Z");
    assertEquals(URI.create(
            "http://test.com/fhir?_outputFormat=xml&_type=Patient%2CObservation"
                + "&_elements=Patient.id%2CCondition.status"
                + "&_typeFilter=Patient.active%3Dtrue%2CObservation.status%3Dfinal"
                + "&_since=2023-01-11T00%3A00%3A00.123Z"),
        BulkExportTemplate.toRequestURI(baseUri, BulkExportRequest.builder()
            ._outputFormat("xml")
            ._type(List.of("Patient", "Observation"))
            ._elements(List.of("Patient.id", "Condition.status"))
            ._typeFilter(List.of("Patient.active=true", "Observation.status=final"))
            ._since(testInstant)
            .build())
    );
  }
}
