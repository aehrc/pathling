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

package au.csiro.pathling.security;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
class PathlingAuthorityTest {

  @Nonnull
  static PathlingAuthority auth(@Nonnull final String authority) {
    return PathlingAuthority.fromAuthority(authority);
  }

  @SuppressWarnings("SameParameterValue")
  static void assertFailsValidation(@Nonnull final String authority,
      @Nonnull final String message) {
    assertThrows(IllegalArgumentException.class, () -> PathlingAuthority.fromAuthority(authority),
        message);
  }

  static void assertFailsValidation(@Nonnull final String authority) {
    assertThrows(IllegalArgumentException.class, () -> PathlingAuthority.fromAuthority(authority),
        "Authority is not recognized: " + authority);
  }

  @Test
  void testValidation() {
    // positive cases
    PathlingAuthority.fromAuthority("pathling");
    PathlingAuthority.fromAuthority("pathling:read");
    PathlingAuthority.fromAuthority("pathling:write");
    PathlingAuthority.fromAuthority("pathling:aggregate");
    PathlingAuthority.fromAuthority("pathling:search");
    PathlingAuthority.fromAuthority("pathling:extract");
    PathlingAuthority.fromAuthority("pathling:import");
    PathlingAuthority.fromAuthority("pathling:import-pnp");
    PathlingAuthority.fromAuthority("pathling:create");
    PathlingAuthority.fromAuthority("pathling:update");
    for (final ResourceType resourceType : ResourceType.values()) {
      PathlingAuthority.fromAuthority("pathling:read:" + resourceType.toCode());
      PathlingAuthority.fromAuthority("pathling:write:" + resourceType.toCode());
    }

    // negative cases
    assertFailsValidation("*");
    assertFailsValidation("read");
    assertFailsValidation("read:Patient");
    assertFailsValidation("pathling:read:*");
    assertFailsValidation("pathling:*");
    assertFailsValidation("pathling:*:*");
    assertFailsValidation("pathling::");
    assertFailsValidation("pathling::Patient");
    assertFailsValidation("pathling:search:Patient", "Subject not supported for action: search");
    assertFailsValidation("pathling:se_arch");
    assertFailsValidation("pathling:read:Clinical_Impression");
  }

  @Test
  void testResourceAccessSubsumedBy() {
    final PathlingAuthority patientRead = PathlingAuthority.fromAuthority("pathling:read:Patient");
    final PathlingAuthority conditionWrite = PathlingAuthority
        .fromAuthority("pathling:write:Condition");

    // positive cases
    assertTrue(patientRead.subsumedBy(auth("pathling:read:Patient")));
    assertTrue(patientRead.subsumedBy(auth("pathling:read")));
    assertTrue(patientRead.subsumedBy(auth("pathling")));

    assertTrue(conditionWrite.subsumedBy(auth("pathling:write:Condition")));
    assertTrue(conditionWrite.subsumedBy(auth("pathling:write")));
    assertTrue(conditionWrite.subsumedBy(auth("pathling")));

    // negative cases
    assertFalse(patientRead.subsumedBy(auth("pathling:write")));
    assertFalse(patientRead.subsumedBy(auth("pathling:write:Patient")));
    assertFalse(conditionWrite.subsumedBy(auth("pathling:read")));
    assertFalse(conditionWrite.subsumedBy(auth("pathling:write:DiagnosticReport")));
  }

  @Test
  void testOperationAccessSubsumedBy() {
    final PathlingAuthority search = PathlingAuthority.fromAuthority("pathling:search");

    // positive cases
    assertTrue(search.subsumedBy(auth("pathling:search")));
    assertTrue(search.subsumedBy(auth("pathling")));

    // negative cases
    assertFalse(search.subsumedBy(auth("pathling:import")));
    assertFalse(search.subsumedBy(auth("pathling:read")));
    assertFalse(search.subsumedBy(auth("pathling:write:ClinicalImpression")));
  }

  @Test
  void testSubsumedByAny() {
    final PathlingAuthority search = PathlingAuthority.fromAuthority("pathling:search");

    // Negative cases
    assertFalse(search.subsumedByAny(Collections.emptyList()));
    assertFalse(search.subsumedByAny(Arrays.asList(
        auth("pathling:import"),
        auth("pathling:aggregate")
    )));

    assertTrue(search.subsumedByAny(Arrays.asList(
        auth("pathling:import"),
        auth("pathling:search")
    )));
  }

  @Test
  void testResourceAccess() {
    final PathlingAuthority authority = PathlingAuthority
        .resourceAccess(ResourceAccess.AccessType.READ, ResourceType.PATIENT);
    assertEquals("pathling:read:Patient", authority.getAuthority());
    assertTrue(authority.getAction().isPresent());
    assertEquals("read", authority.getAction().get());
    assertTrue(authority.getSubject().isPresent());
    assertEquals("Patient", authority.getSubject().get());
  }

  @Test
  void testOperationAccess() {
    final PathlingAuthority authority = PathlingAuthority.operationAccess("aggregate");
    assertEquals("pathling:aggregate", authority.getAuthority());
    assertTrue(authority.getAction().isPresent());
    assertEquals("aggregate", authority.getAction().get());
  }

}
