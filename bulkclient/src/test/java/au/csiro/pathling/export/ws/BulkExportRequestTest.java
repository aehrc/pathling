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

import au.csiro.pathling.export.fhir.Parameters;
import au.csiro.pathling.export.fhir.Parameters.Parameter;
import au.csiro.pathling.export.fhir.Reference;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BulkExportRequestTest {


  @Test
  void testToParametersWithAllDefaults() {
    final BulkExportRequest request = BulkExportRequest.builder()
        .build();
    assertEquals(Parameters.of(), request.toParameters());
  }

  @Test
  void testToParametersWithWithAllValuesSet() {
    final BulkExportRequest request = BulkExportRequest.builder()
        ._outputFormat("fhir+ndjson")
        ._since(Instant.parse("2023-08-01T00:00:00Z"))
        ._type(List.of("Patient", "Condition"))
        ._elements(List.of("Patient.name", "Patient.birthDate"))
        ._typeFilter(List.of("Patient?active=true", "Condition?clinicalStatus=active"))
        .patient(List.of(Reference.of("Patient/00"), Reference.of("Patient/01")))
        .build();
    assertEquals(
        Parameters.of(
            Parameter.of("_outputFormat", "fhir+ndjson"),
            Parameter.of("_since", Instant.parse("2023-08-01T00:00:00Z")),
            Parameter.of("_type", "Patient,Condition"),
            Parameter.of("_elements", "Patient.name,Patient.birthDate"),
            Parameter.of("_typeFilter", "Patient?active=true,Condition?clinicalStatus=active"),
            Parameter.of("patient", Reference.of("Patient/00")),
            Parameter.of("patient", Reference.of("Patient/01"))
        ),
        request.toParameters());
  }
}
