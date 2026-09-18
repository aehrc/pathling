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
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The storage of a primitive value, which fails where the value contradicts its declared type
 * (decision 57).
 *
 * <p>A plain cast cannot be relied upon for that. It raises on a malformed value only while {@code
 * spark.sql.ansi.enabled} is on, which is a setting of the caller's session rather than of
 * Pathling; with it off, the same value becomes a silent null, and an integer written with a
 * fraction is silently truncated. Even with it on, the cast to a boolean accepts {@code yes},
 * {@code y}, {@code t} and {@code 1}, which FHIR does not. A decimal is stored as text, so no cast
 * notices it at all.
 *
 * <p>So the value is checked against the lexical form FHIR declares for its type, and cast with
 * {@code try_cast}, whose result does not depend on ANSI mode. A value that fails either raises an
 * error naming the element and its type. The value itself is not named, because it is patient data
 * and the error is destined for a log.
 */
final class PrimitiveValues {

  /**
   * The lexical form of each primitive type whose stored value depends on it, as the regular
   * expressions the R4 definitions declare. The types stored as text are absent, because storing
   * them changes nothing about them and they are not this layout's to validate.
   *
   * <p>{@code integer64} is an R5 type, given the form of {@code integer}, because the storage
   * mapping carries it.
   */
  @Nonnull
  private static final Map<String, String> LEXICAL_FORMS =
      Map.of(
          "boolean", "true|false",
          "integer", "-?([0]|([1-9][0-9]*))",
          "unsignedInt", "[0]|([1-9][0-9]*)",
          "positiveInt", "[1-9][0-9]*",
          "integer64", "-?([0]|([1-9][0-9]*))",
          "decimal", "-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?");

  private PrimitiveValues() {}

  /**
   * Returns the stored value of a primitive element, descending through an array where the element
   * repeats so that each value is checked on its own.
   *
   * @param source the column the source was read into, as text
   * @param type the type the definitions declare for the element
   * @param target the type the element is stored as
   * @param path the path of the element, which a failure names
   * @return the stored value
   */
  @Nonnull
  static Column storedValue(
      @Nonnull final Column source,
      @Nonnull final FHIRDefinedType type,
      @Nonnull final DataType target,
      @Nonnull final String path) {
    if (target instanceof final ArrayType array) {
      return functions.transform(
          source, value -> storedValue(value, type, array.elementType(), path));
    }
    final Column stored =
        FHIRDefinedType.DECIMAL.equals(type)
            ? DecimalTransform.storedValue(source, target)
            : source.try_cast(target);
    return Optional.ofNullable(LEXICAL_FORMS.get(type.toCode()))
        .map(
            form ->
                functions
                    .when(
                        source
                            .isNull()
                            .or(source.rlike("^(" + form + ")$").and(stored.isNotNull())),
                        stored)
                    .otherwise(
                        functions
                            .raise_error(
                                functions.lit(
                                    "A value of "
                                        + path
                                        + " does not conform to its declared type "
                                        + type.toCode()))
                            .cast(target)))
        .orElse(stored);
  }
}
