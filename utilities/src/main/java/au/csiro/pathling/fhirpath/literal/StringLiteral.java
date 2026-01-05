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

import static java.util.Map.entry;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.commons.text.translate.AggregateTranslator;
import org.apache.commons.text.translate.CharSequenceTranslator;
import org.apache.commons.text.translate.LookupTranslator;
import org.apache.commons.text.translate.UnicodeUnescaper;

/**
 * Utility class for handling FHIRPath string literal escaping and unescaping.
 *
 * @author John Grimes
 */
public abstract class StringLiteral {

  private StringLiteral() {}

  /**
   * On the way back out, we only do the minimal escaping to guarantee syntactical correctness.
   *
   * @param value the value to apply escaping to
   * @return the escaped result
   */
  @Nonnull
  public static String escapeFhirPathString(@Nonnull final String value) {
    return value.replace("'", "\\'");
  }

  private static final Map<CharSequence, CharSequence> FHIR_CTRL_UNESCAPE_MAP =
      Map.ofEntries(entry("\\n", "\n"), entry("\\t", "\t"), entry("\\f", "\f"), entry("\\r", "\r"));

  private static final Map<CharSequence, CharSequence> FHIR_CHAR_UNESCAPE_MAP =
      Map.ofEntries(
          entry("\\`", "`"),
          entry("\\'", "'"),
          entry("\\\"", "\""),
          entry("\\/", "/"),
          entry("\\\\", "\\"));

  private static final CharSequenceTranslator UNESCAPE_FHIR =
      new AggregateTranslator(
          new UnicodeUnescaper(),
          new LookupTranslator(FHIR_CTRL_UNESCAPE_MAP),
          new LookupTranslator(FHIR_CHAR_UNESCAPE_MAP));

  /**
   * This method implements the rules for dealing with strings in the FHIRPath specification.
   *
   * @param value the string to be unescaped
   * @return the unescaped result
   * @see <a href="https://hl7.org/fhirpath/index.html#string">String</a>
   */
  @Nonnull
  public static String unescapeFhirPathString(@Nonnull final String value) {
    return UNESCAPE_FHIR.translate(value);
  }
}
