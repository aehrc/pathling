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
import org.apache.spark.sql.functions;

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
  @Nonnull public static final String VALUE = "value";

  /** The name of the numeric annotation of a decimal. */
  @Nonnull public static final String NUMERIC = LayoutFields.NUMERIC_ANNOTATION;

  /** The names of the annotations of a decimal. */
  @Nonnull private static final List<String> ANNOTATIONS = List.of(NUMERIC);

  /**
   * The canonical order of the fields of a decimal, by which decimals of different shapes merge.
   */
  @Nonnull
  public static final CanonicalStructure STRUCTURE = FlatStructure.of(List.of(VALUE, NUMERIC));

  private StoredDecimal() {}

  /**
   * Returns the decimal of a stored text and its metadata group: its text and the numeric
   * annotation where the group's schema carries it. A decimal whose number the engine's
   * System.Decimal cannot hold, such as {@code 1.0E40}, is null, as decoding its text alone gave
   * before the element held its text.
   *
   * @param text the stored text of the decimal
   * @param group the metadata group beside it
   * @return the decimal, null where the text is or where its number does not decode
   */
  @Nonnull
  public static Column fromStored(@Nonnull final Column text, @Nonnull final Column group) {
    return when(
        text.isNotNull(),
        decodable(
            ColumnFunctions.resolveOrNone(
                group, ANNOTATIONS, List.of(VALUE), List.of(text), false)));
  }

  /**
   * Returns the decimals of a repeating stored decimal and its metadata groups, paired by position,
   * each with its text and the numeric annotation where the groups' schema carries it, with the
   * decimals that have no text left out. A decimal whose number the engine's System.Decimal cannot
   * hold is a null item, as decoding its text alone gave.
   *
   * @param texts the stored texts of the decimal
   * @param groups the metadata groups beside them
   * @return the decimals present, null where the texts are
   */
  @Nonnull
  public static Column fromStoredArray(@Nonnull final Column texts, @Nonnull final Column groups) {
    return functions.transform(
        functions.filter(
            ColumnFunctions.resolveOrNone(
                groups, ANNOTATIONS, List.of(VALUE), List.of(texts), true),
            decimal -> decimal.getField(VALUE).isNotNull()),
        StoredDecimal::decodable);
  }

  /** Returns a decimal where its number decodes, and null where it does not. */
  @Nonnull
  private static Column decodable(@Nonnull final Column decimal) {
    // The decimal is bound once, as the guard and the value both read it.
    return let(decimal, d -> when(number(d).isNotNull(), d));
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
        ColumnFunctions.annotationOrElse(decimal, NUMERIC, kind.compute(NUMERIC, text(decimal))));
  }
}
