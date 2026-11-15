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

package au.csiro.pathling.fhirpath.column;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.utilities.CanonicalStructure;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * The columns of a FHIR decimal as the engine holds it after traversal (decisions 141, 147 and
 * 152).
 *
 * <p>A stored decimal is a flat structure of its stored text, {@code value}, and its numeric
 * annotation, {@code __numeric}, where the schema carries it, so a decimal over unannotated data is
 * {@code struct(value)}. No annotation is computed as it is traversed; only whether its number
 * decodes is tested, so that a decimal the engine's System.Decimal cannot hold is absent, as it was
 * before the element held its text. Its number, the engine's System.Decimal, is read through {@link
 * #number}, which takes the stored annotation where the decimal carries it and decodes the text
 * otherwise, decided by Spark as it resolves the plan (design §4.3).
 *
 * <p>A computed decimal, such as a literal or the result of arithmetic, is a bare {@link
 * SystemDecimal#TYPE}, and is never held in this form.
 */
public final class StoredDecimal {

  /** The name of the stored text of a decimal. */
  @Nonnull public static final String VALUE = StoredPrimitive.VALUE;

  /** The name of the numeric annotation of a decimal. */
  @Nonnull public static final String NUMERIC = LayoutFields.NUMERIC_ANNOTATION;

  /**
   * The canonical order of the fields of a decimal, by which decimals of different shapes merge.
   */
  @Nonnull
  public static final CanonicalStructure STRUCTURE = FlatStructure.of(List.of(VALUE, NUMERIC));

  private StoredDecimal() {}

  /**
   * Returns a decimal where its number decodes, and null where it does not. A decimal without a
   * text, which only a primitive held with its id and extensions can be, is returned as it is.
   *
   * @param decimal the decimal
   * @return the decimal, null where its text does not decode
   */
  @Nonnull
  static Column decodable(@Nonnull final Column decimal) {
    // The decimal is bound once, as the guard and the value both read it.
    return let(decimal, d -> when(text(d).isNull().or(number(d).isNotNull()), d));
  }

  /**
   * Returns the stored text of a decimal.
   *
   * @param decimal the decimal
   * @return its text
   */
  @Nonnull
  public static Column text(@Nonnull final Column decimal) {
    return decimal.getField(VALUE);
  }

  /**
   * Returns the number of a decimal, as the engine's System.Decimal: its numeric annotation where
   * the decimal carries it, and the decoding of its text otherwise. A text that does not decode
   * gives null.
   *
   * @param decimal the decimal
   * @return its number, typed as {@link SystemDecimal#TYPE}
   */
  @Nonnull
  public static Column number(@Nonnull final Column decimal) {
    final DecimalNumericKind kind = DecimalNumericKind.INSTANCE;
    return kind.read(
        kind.apply(NUMERIC, text(decimal), decimal, ColumnFunctions::annotationOrElse));
  }
}
