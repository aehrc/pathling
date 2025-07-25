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

package au.csiro.pathling.fhirpath.literal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author Piotr Szul
 */
class CodingLiteralTest {

  static ImmutableCoding parseLiteral(final String s) {
    return ImmutableCoding.of(CodingLiteral.fromString(s));
  }

  @Test
  void emptyCodingFromLiteral() {
    assertEquals(ImmutableCoding.empty(), parseLiteral("|"));
    assertEquals(ImmutableCoding.empty(), parseLiteral("||"));
    assertEquals(ImmutableCoding.empty(), parseLiteral("|||"));
    assertEquals(ImmutableCoding.empty(), parseLiteral("||||"));
    assertEquals(ImmutableCoding.empty(), parseLiteral("''|''"));
    assertEquals(ImmutableCoding.empty(), parseLiteral("''|''|''"));
    assertEquals(ImmutableCoding.empty(), parseLiteral("''|''||''"));
  }

  @Test
  void fromLiteral() {
    assertEquals(ImmutableCoding.of("system", null, "code", null, null),
        parseLiteral("system|code"));
    assertEquals(ImmutableCoding.of("system", "version", "code", null, null),
        parseLiteral("system|code|version"));
    assertEquals(ImmutableCoding.of("system", "version", "code", "display", null),
        parseLiteral("system|code|version|display"));
    assertEquals(ImmutableCoding.of("system", "version", "code", "display", true),
        parseLiteral("system|code|version|display|true"));
    assertEquals(ImmutableCoding.of("system", null, "code", "display", null),
        parseLiteral("system|code||display"));
    assertEquals(ImmutableCoding.of("system", null, "code", null, false),
        parseLiteral("system|code|||false"));
  }

  @Test
  void fromLiteralWithEncoding() {
    assertEquals(ImmutableCoding.of("system", null, "code", null, null),
        parseLiteral("'system'|'code'"));
    assertEquals(ImmutableCoding.of("system", null, "some|code", " \\(),'", true),
        parseLiteral("'system'|'some|code'||' \\\\(),\\''|'true'"));
  }

  @Test
  void toLiteral() {
    assertEquals("|", CodingLiteral.toLiteral(ImmutableCoding.empty().toCoding()));
    assertEquals("system|code",
        CodingLiteral.toLiteral(ImmutableCoding.of("system", null, "code", null, null).toCoding()));
    assertEquals("system|code|version",
        CodingLiteral
            .toLiteral(ImmutableCoding.of("system", "version", "code", null, null).toCoding()));
    assertEquals("system|code|version|display",
        CodingLiteral.toLiteral(
            ImmutableCoding.of("system", "version", "code", "display", null).toCoding()));
    assertEquals("system|code|version|display|true",
        CodingLiteral.toLiteral(
            ImmutableCoding.of("system", "version", "code", "display", true).toCoding()));
    assertEquals("system|code||display",
        CodingLiteral
            .toLiteral(ImmutableCoding.of("system", null, "code", "display", null).toCoding()));
    assertEquals("system||||false",
        CodingLiteral.toLiteral(ImmutableCoding.of("system", null, null, null, false).toCoding()));
  }

  @Test
  void toLiteralQuoted() {
    assertEquals("system|'special|code'|version|'\\'some display\\''",
        CodingLiteral.toLiteral(
            ImmutableCoding.of("system", "version", "special|code", "'some display'", null)
                .toCoding()));
  }

  @Test
  void fromMalformedString() {
    assertThrows(IllegalArgumentException.class, () -> CodingLiteral.fromString(
        "http://snomed.info/sct"));
  }
}
