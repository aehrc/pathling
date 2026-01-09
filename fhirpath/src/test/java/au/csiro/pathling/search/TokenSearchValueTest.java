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

package au.csiro.pathling.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TokenSearchValue} parsing.
 */
class TokenSearchValueTest {

  @Test
  void testParseCodeOnly() {
    final TokenSearchValue result = TokenSearchValue.parse("male");

    assertNull(result.getSystem());
    assertEquals("male", result.getCode());
    assertFalse(result.isExplicitNoSystem());
  }

  @Test
  void testParseSystemAndCode() {
    final TokenSearchValue result = TokenSearchValue.parse("http://example.org|ABC123");

    assertEquals("http://example.org", result.getSystem());
    assertEquals("ABC123", result.getCode());
    assertFalse(result.isExplicitNoSystem());
  }

  @Test
  void testParseExplicitNoSystem() {
    final TokenSearchValue result = TokenSearchValue.parse("|ABC123");

    assertNull(result.getSystem());
    assertEquals("ABC123", result.getCode());
    assertTrue(result.isExplicitNoSystem());
  }

  @Test
  void testParseSystemOnly() {
    final TokenSearchValue result = TokenSearchValue.parse("http://example.org|");

    assertEquals("http://example.org", result.getSystem());
    assertNull(result.getCode());
    assertFalse(result.isExplicitNoSystem());
  }

  @Test
  void testParseCodeWithSpecialCharacters() {
    final TokenSearchValue result = TokenSearchValue.parse("http://loinc.org|12345-6");

    assertEquals("http://loinc.org", result.getSystem());
    assertEquals("12345-6", result.getCode());
    assertFalse(result.isExplicitNoSystem());
  }

  @Test
  void testParseEmptyString() {
    final TokenSearchValue result = TokenSearchValue.parse("");

    assertNull(result.getSystem());
    assertEquals("", result.getCode());
    assertFalse(result.isExplicitNoSystem());
  }

  @Test
  void testParsePipeOnly() {
    final TokenSearchValue result = TokenSearchValue.parse("|");

    assertNull(result.getSystem());
    assertNull(result.getCode());
    assertTrue(result.isExplicitNoSystem());
  }

  // ========== getSimpleCode() tests ==========

  @Test
  void testRequiresSimpleCodeSuccess() {
    final TokenSearchValue result = TokenSearchValue.parse("male");
    assertEquals("male", result.requiresSimpleCode());
  }

  @Test
  void testRequiresSimpleCodeWithExplicitNoSystem() {
    // |code syntax should work - system is null
    final TokenSearchValue result = TokenSearchValue.parse("|ABC123");
    assertEquals("ABC123", result.requiresSimpleCode());
  }

  @Test
  void testRequiresSimpleCodeFailsWithSystem() {
    final TokenSearchValue result = TokenSearchValue.parse("http://example.org|ABC123");

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        result::requiresSimpleCode);

    assertTrue(exception.getMessage().contains("System|code syntax is not supported"));
    assertTrue(exception.getMessage().contains("http://example.org"));
  }

  @Test
  void testRequiresSimpleCodeFailsWithSystemOnly() {
    final TokenSearchValue result = TokenSearchValue.parse("http://example.org|");

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        result::requiresSimpleCode);

    assertTrue(exception.getMessage().contains("System|code syntax is not supported"));
  }

  @Test
  void testRequiresSimpleCodeFailsWithNoCode() {
    final TokenSearchValue result = TokenSearchValue.parse("|");

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        result::requiresSimpleCode);

    assertTrue(exception.getMessage().contains("code value is required"));
  }
}
