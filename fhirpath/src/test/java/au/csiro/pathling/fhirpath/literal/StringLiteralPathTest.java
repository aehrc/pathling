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

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.escapeFhirPathString;
import static au.csiro.pathling.fhirpath.literal.StringLiteral.unescapeFhirPathString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
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
    assertEquals("Some\tthing", result);
  }

  @Test
  void unescapeBackTick() {
    final String result = unescapeFhirPathString("\\`code\\`");
    assertEquals("`code`", result);
  }

  @Test
  void unescapeSingleQuote() {
    final String result = unescapeFhirPathString("Some \\\"test\\\"");
    assertEquals("Some \"test\"", result);
  }

  @Test
  void unescapeDoubleQuote() {
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

  @Test
  void unescapeUnicode() {
    final String result = unescapeFhirPathString("P\\u0065ter");
    assertEquals("Peter", result);
  }
  
}
