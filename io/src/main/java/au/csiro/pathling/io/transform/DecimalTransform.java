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
 */
public final class DecimalTransform {

  /** The reader option that infers every JSON primitive as text rather than as a number. */
  @Nonnull private static final String PRIMITIVES_AS_STRING = "primitivesAsString";

  @Nonnull
  private static final Map<String, String> READ_OPTIONS = Map.of(PRIMITIVES_AS_STRING, "true");

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
}
