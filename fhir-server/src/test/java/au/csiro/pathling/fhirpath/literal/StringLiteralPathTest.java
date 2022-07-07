/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.escapeFhirPathString;
import static au.csiro.pathling.fhirpath.literal.StringLiteral.unescapeFhirPathString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class StringLiteralPathTest {

  @Test
  void unescapeForwardSlash() {
    final String result = unescapeFhirPathString("\\/home\\/user\\/docs");
    assertEquals("/home/user/docs", result);
  }

  @Test
  void unescapeFormFeed() {
    final String result = unescapeFhirPathString("Some\\fthing");
    assertEquals("Some\u000Cthing", result);
  }

  @Test
  void unescapeNewLine() {
    final String result = unescapeFhirPathString("Some\\nthing");
    assertEquals("Some\nthing", result);
  }

  @Test
  void unescapeCarriageReturn() {
    final String result = unescapeFhirPathString("Some\\rthing");
    assertEquals("Some\rthing", result);
  }

  @Test
  void unescapeTab() {
    final String result = unescapeFhirPathString("Some\\tthing");
    assertEquals("Some\u0009thing", result);
  }

  @Test
  void unescapeBackTick() {
    final String result = unescapeFhirPathString("\\`code\\`");
    assertEquals("`code`", result);
  }

  @Test
  void unescapeSingleQuote() {
    final String result = unescapeFhirPathString("Some string and it\\'s problems");
    assertEquals("Some string and it's problems", result);
  }

  @Test
  void unescapeBackSlash() {
    final String result = unescapeFhirPathString("C:\\\\Temp");
    assertEquals("C:\\Temp", result);
  }

  @Test
  void escapeSingleQuote() {
    final String result = escapeFhirPathString("Some string and it's problems");
    assertEquals("Some string and it\\'s problems", result);
  }

}
