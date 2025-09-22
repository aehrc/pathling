/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific 
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.support.r4;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;

class R4FhirConversionSupportTest {

  @Test
  void testRewritesResolvedURNReference() {
    final IBaseResource referenceResource = new Patient().setId(new IdType("Patient", "1234"));
    final Reference urnReference = (Reference) new Reference().setReference("urn:uuid:1234")
        .setResource(referenceResource);
    R4FhirConversionSupport.resolveURNReference(urnReference);
    assertEquals("Patient/1234", urnReference.getReference());
  }

  @Test
  void testKeepsResolvedURNReferenceWithNoId() {
    final IBaseResource referenceResource = new Patient();
    final Reference urnReference = (Reference) new Reference().setReference("urn:uuid:1234")
        .setResource(referenceResource);
    R4FhirConversionSupport.resolveURNReference(urnReference);
    assertEquals("urn:uuid:1234", urnReference.getReference());
  }

  @Test
  void testKeepsUnresolvedNonURNReference() {
    final Reference urnReference = new Reference().setReference("Condition/2345");
    R4FhirConversionSupport.resolveURNReference(urnReference);
    assertEquals("Condition/2345", urnReference.getReference());
  }

  @Test
  void testKeepsUnresolvedURNReference() {
    final Reference urnReference = new Reference().setReference("urn:uuid:1234");
    R4FhirConversionSupport.resolveURNReference(urnReference);
    assertEquals("urn:uuid:1234", urnReference.getReference());
  }

  @Test
  void testNoOpForNonReferences() {
    final StringType nonReference = new StringType("someString");
    final StringType nonReferenceCopy = nonReference.copy();
    R4FhirConversionSupport.resolveURNReference(nonReference);
    assertTrue(nonReferenceCopy.equalsDeep(nonReference), "Non-reference should not be modified");
  }

}
