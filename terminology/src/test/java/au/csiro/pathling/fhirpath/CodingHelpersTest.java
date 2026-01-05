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

package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.Test;

class CodingHelpersTest {

  @Nonnull
  static Coding newCoding(
      @Nullable final String system, @Nullable final String code, @Nullable final String version) {
    return new Coding(system, code, null).setVersion(version);
  }

  @Nonnull
  static Coding newCoding(@Nullable final String system, @Nullable final String code) {
    return newCoding(system, code, null);
  }

  static void assertCodingEq(@Nullable final Coding left, @Nullable final Coding right) {
    assertTrue(CodingHelpers.codingEquals(left, right));
    assertTrue(CodingHelpers.codingEquals(right, left));
  }

  static void assertCodingNotEq(@Nullable final Coding left, @Nullable final Coding right) {
    assertFalse(CodingHelpers.codingEquals(left, right));
    assertFalse(CodingHelpers.codingEquals(right, left));
  }

  @Test
  void codingEquals() {
    assertCodingEq(null, null);
    assertCodingEq(newCoding("s1", "c1", "v1"), newCoding("s1", "c1", "v1"));
    assertCodingEq(newCoding("s1", "c1"), newCoding("s1", "c1", "v1"));
    assertCodingEq(newCoding("s1", "c1"), newCoding("s1", "c1"));
    assertCodingEq(newCoding("s1", null), newCoding("s1", null));
    assertCodingEq(newCoding(null, null), newCoding(null, null));
  }

  @Test
  void codingNotEquals() {
    assertCodingNotEq(newCoding("s1", "c1", "v1"), newCoding("s1", "c1", "v2"));
    assertCodingNotEq(newCoding("s1", "c1", "v1"), newCoding("s1", "c2"));
    assertCodingNotEq(newCoding("s1", "c1", "v1"), newCoding("s1", null));
    assertCodingNotEq(newCoding("s1", "c1"), newCoding("s2", "c1"));
    assertCodingNotEq(newCoding("s1", "c1"), newCoding(null, "c1"));
  }
}
