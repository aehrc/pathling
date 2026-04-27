/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.dsl;

import static au.csiro.pathling.test.dsl.TypeInfoExpectation.toTypeInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Tests for {@link TypeInfoExpectation} parsing. */
class TypeInfoExpectationTest {

  @Test
  void parsesSystemType() {
    final TypeInfoExpectation result = toTypeInfo("System.Integer(System.Any)");
    assertEquals("System", result.getNamespace());
    assertEquals("Integer", result.getName());
    assertEquals("System.Any", result.getBaseType());
  }

  @Test
  void parsesFhirResourceType() {
    final TypeInfoExpectation result = toTypeInfo("FHIR.Patient(FHIR.Resource)");
    assertEquals("FHIR", result.getNamespace());
    assertEquals("Patient", result.getName());
    assertEquals("FHIR.Resource", result.getBaseType());
  }

  @Test
  void parsesFhirElementType() {
    final TypeInfoExpectation result = toTypeInfo("FHIR.boolean(FHIR.Element)");
    assertEquals("FHIR", result.getNamespace());
    assertEquals("boolean", result.getName());
    assertEquals("FHIR.Element", result.getBaseType());
  }

  @Test
  void parsesSystemObjectType() {
    final TypeInfoExpectation result = toTypeInfo("System.Object(System.Any)");
    assertEquals("System", result.getNamespace());
    assertEquals("Object", result.getName());
    assertEquals("System.Any", result.getBaseType());
  }

  @Test
  void throwsOnInvalidFormat() {
    assertThrows(IllegalArgumentException.class, () -> toTypeInfo("InvalidFormat"));
  }

  @Test
  void throwsOnMissingBaseType() {
    assertThrows(IllegalArgumentException.class, () -> toTypeInfo("System.Integer"));
  }

  @Test
  void throwsOnEmptyString() {
    assertThrows(IllegalArgumentException.class, () -> toTypeInfo(""));
  }
}
