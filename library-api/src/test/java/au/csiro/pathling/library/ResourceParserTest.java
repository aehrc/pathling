/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Claim;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ResourceParserTest {

  private static final String PATIENT_REF = "Patient/704c9750-f6e6-473b-ee83-fbd48e07fe3f";
  private static final String CONDITION_REF = "Condition/2383c155-6345-e842-b7d7-3f6748ca634b";
  private static final String CLAIM_REF = "Claim/47a08521-15ad-de17-f731-9b79065ddd72";

  private static final String ENCOUNTER_URN = "urn:uuid:16dceeb0-620b-f259-8b1a-87dc65e5f78a";

  private static final String REFERENCE_RELATIVE = "Patient/2383c155-6345-e842-b7d7-000000000001";
  private static final String REFERENCE_ABSOLUTE =
      "http://foo.bar.com/Encounter/2383c155-6345-e842-b7d7-000000000002";
  private static final String REFERENCE_CONDITIONAL =
      "Organization?identifier=https://github.com/synthetichealth/synthea|fa51267f-96dd-340c-ad1c-76080f4525f6";

  private Patient patient;
  private Condition condition;
  private Claim claim;
  private Condition condition1;

  @BeforeEach
  void setUp() throws IOException {
    final String testBundleJson =
        Resources.toString(
            Resources.getResource("test-data/references/test-bundle.json"), Charsets.UTF_8);

    final Bundle bundle =
        (Bundle)
            ResourceParser.build(FhirVersionEnum.R4, PathlingContext.FHIR_JSON)
                .parse(testBundleJson);

    patient = (Patient) bundle.getEntry().get(0).getResource();
    condition = (Condition) bundle.getEntry().get(1).getResource();
    claim = (Claim) bundle.getEntry().get(2).getResource();
    condition1 = (Condition) bundle.getEntry().get(3).getResource();
  }

  @Test
  void testKeepsProvidedResourceIds() {
    assertEquals(PATIENT_REF, patient.getId());
    assertEquals(CONDITION_REF, condition.getId());
    assertEquals(CLAIM_REF, claim.getId());
  }

  @Test
  void testResolvesKnownURNReferences() {
    // resource level reference
    assertEquals(PATIENT_REF, condition.getSubject().getReference());
    // element lever reference
    assertEquals(CONDITION_REF, claim.getDiagnosis().get(0).getDiagnosisReference().getReference());
    // reference in an extension
    assertEquals(PATIENT_REF, ((Reference) claim.getExtension().get(0).getValue()).getReference());
    // reference in the extension of a primitive value
    assertEquals(
        CONDITION_REF,
        ((Reference) claim.getStatusElement().getExtension().get(0).getValue()).getReference());
  }

  @Test
  void testKeepsUnresolvedURNReferences() {
    assertEquals(ENCOUNTER_URN, condition.getEncounter().getReference());
  }

  @Test
  void testPreservesNonURNReferences() {
    assertEquals(REFERENCE_RELATIVE, condition1.getSubject().getReference());
    assertEquals(REFERENCE_ABSOLUTE, condition1.getEncounter().getReference());
    assertEquals(REFERENCE_CONDITIONAL, claim.getProvider().getReference());
  }
}
