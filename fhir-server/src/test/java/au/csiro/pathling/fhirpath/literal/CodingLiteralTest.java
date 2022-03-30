/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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
@Tag("UnitTest")
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