/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.support.r4;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.Test;

class FhirTraversalTest {

  static final UriType URI_1 = new UriType("http://example.com/1");
  static final StringType STRING_1 = new StringType("test1");
  static final Extension EXTENSION_1 = new Extension(URI_1).setValue(STRING_1);
  static final UriType URI_2 = new UriType("http://example.com/2");
  static final StringType STRING_2 = new StringType("test2");
  static final Extension EXTENSION_2 = new Extension(URI_2).setValue(STRING_2);

  final Set<Base> collection = new HashSet<>();

  void collectObject(final Base base) {
    collection.add(base);
  }

  @Test
  void testResourceWithNoElements() {
    final Resource patient = new Patient();
    FhirTraversal.processRecursive(patient, this::collectObject);
    assertEquals(Set.of(patient), collection);
  }

  @Test
  void testTraverseOfElements() {
    final StringType familyName1 = new StringType("Smith1");
    final StringType familyName2 = new StringType("Smith2");
    final HumanName name1 = new HumanName().setFamilyElement(familyName1);
    final HumanName name2 = new HumanName().setFamilyElement(familyName2);
    final IdType id = new IdType("Patient", "212121");
    final Patient patient = new Patient();
    patient.setIdElement(id);
    patient.addName(name1).addName(name2);
    FhirTraversal.processRecursive(patient, this::collectObject);
    assertEquals(Set.of(familyName1, familyName2, name1, name2, id, patient),
        collection);
  }

  @Test
  void testTraverseExtensionsOnPrimitives() {
    final IdType id = new IdType("Patient", "212121");
    id.addExtension(EXTENSION_1).addExtension(EXTENSION_2);
    final Resource patient = new Patient().setIdElement(id);
    FhirTraversal.processRecursive(patient, this::collectObject);
    assertEquals(Set.of(URI_1, STRING_1, EXTENSION_1, URI_2, STRING_2, EXTENSION_2, id, patient),
        collection);
  }

  @Test
  void testTraverseExtensionsResources() {
    final IdType id = new IdType("Patient", "212121");
    final Patient patient = new Patient();
    patient.setIdElement(id);
    patient.addExtension(EXTENSION_1).addExtension(EXTENSION_2);
    FhirTraversal.processRecursive(patient, this::collectObject);
    assertEquals(Set.of(URI_1, STRING_1, EXTENSION_1, URI_2, STRING_2, EXTENSION_2, id, patient),
        collection);
  }

  @Test
  void testTraverseExtensionsOnElements() {
    final StringType familyName1 = new StringType("Smith1");
    final HumanName name1 = new HumanName().setFamilyElement(familyName1);
    name1.addExtension(EXTENSION_1).addExtension(EXTENSION_2);

    final IdType id = new IdType("Patient", "212121");
    final Patient patient = new Patient();
    patient.setIdElement(id);
    patient.addName(name1);

    FhirTraversal.processRecursive(patient, this::collectObject);
    assertEquals(
        Set.of(URI_1, STRING_1, EXTENSION_1, URI_2, STRING_2, EXTENSION_2, id, name1, familyName1,
            patient),
        collection);
  }

}
