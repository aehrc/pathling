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

package au.csiro.pathling.io.transform;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

/**
 * The storage of a decimal, which is the lexical form of the source as text (FR-002).
 *
 * <p>A trailing zero, exponent notation, a leading sign and a digit count beyond any fixed-point
 * type all survive only if nothing on the path parses the value as a number, so the lexical form is
 * a property of how the source is read rather than of anything done to the column afterwards. The
 * reader is therefore asked for every primitive as text, and the definitions impose the real type
 * on the others; the option is not per-field, which is why it lives here beside the reason it is
 * set.
 *
 * <p>The numeric value is supplied by the annotation beside the element, which is added later.
 *
 * <p>Egress has the mirror problem. A decimal is text in the layout and a number in the document,
 * and the JSON writer quotes text; nothing asks it not to, per column or otherwise. So the value is
 * marked on its way into the document and the marks, with the quotes around them, are removed from
 * the document afterwards. The mark is a control character, which the writer escapes: a value
 * carrying the six characters of an escape sequence is escaped again and cannot match, so only a
 * raw control character in the data could collide, and only where the whole value is one mark, the
 * characters a decimal is written with, and the other mark.
 */
public final class DecimalTransform {

  /** The reader option that infers every JSON primitive as text rather than as a number. */
  @Nonnull private static final String PRIMITIVES_AS_STRING = "primitivesAsString";

  @Nonnull
  private static final Map<String, String> READ_OPTIONS = Map.of(PRIMITIVES_AS_STRING, "true");

  /**
   * The mark wrapped around a decimal in the document, chosen because the JSON writer escapes it
   * and the FHIR primitive types have no use for it.
   */
  @Nonnull private static final String MARK = "\u0001";

  /**
   * Matches a marked decimal and the quotes the writer put around it, capturing the value. The
   * characters admitted between the marks are the ones a decimal is written with, so a string that
   * somehow carried the marks would still have to be a decimal to be affected.
   */
  @Nonnull private static final String MARKED = "\"\\\\u0001([-+0-9.eE]+)\\\\u0001\"";

  /** The captured value, which replaces the marks and the quotes together. */
  @Nonnull private static final String UNQUOTED = "$1";

  private DecimalTransform() {}

  /**
   * Returns the options a JSON read needs in order to deliver a decimal as the text of the source.
   *
   * @return the read options
   */
  @Nonnull
  public static Map<String, String> lexicalReadOptions() {
    return READ_OPTIONS;
  }

  /**
   * Returns the stored value of a decimal, which is the text the source carried.
   *
   * <p>Nothing here parses the value. The cast reconciles the column with the type the element is
   * stored as, which is text and an array of it respectively for a singular and a repeating
   * element; where the source was read through a path that had already parsed the number, it
   * renders that parsed value as text, and that path is the documented loss of lexical form
   * (FR-020) rather than a second chance at preserving it.
   *
   * @param source the column the source was read into
   * @param target the type the element is stored as
   * @return the stored value
   */
  @Nonnull
  public static Column storedValue(@Nonnull final Column source, @Nonnull final DataType target) {
    return source.cast(target);
  }

  /**
   * Returns the value a decimal takes in the document under construction, which is its stored text
   * between two marks.
   *
   * @param stored the stored value
   * @return the marked value, which is null where the stored value is
   */
  @Nonnull
  public static Column markedValue(@Nonnull final Column stored) {
    return functions.concat(functions.lit(MARK), stored, functions.lit(MARK));
  }

  /**
   * Returns a document with every marked decimal in it turned from text into a number, which is
   * what makes the round trip of a decimal lossless rather than merely faithful in its characters.
   *
   * @param document the document the JSON writer produced
   * @return the document, with the decimals unquoted
   */
  @Nonnull
  public static Column unmarkedDocument(@Nonnull final Column document) {
    return functions.regexp_replace(document, MARKED, UNQUOTED);
  }
}
