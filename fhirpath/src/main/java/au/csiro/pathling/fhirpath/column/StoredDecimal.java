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
import java.util.stream.Stream;
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
   * Returns the decimal of a stored text and its metadata group, or the decimals of a repeating one
   * paired by position, each with its text, the named metadata fields of its group, such as a
   * primitive's {@code id} and {@code extension}, and the numeric annotation where the group's
   * schema carries it. Nothing is decoded, and nothing is left out: a decimal without a text is a
   * structure without a value.
   *
   * @param texts the stored text of the decimal, or the texts where it repeats
   * @param groups the metadata group beside it, or the groups where it repeats
   * @param metadata the names of the metadata fields to read from the groups, where they have them
   * @param repeating whether the decimal repeats
   * @return the decimals, paired with their groups
   */
  @Nonnull
  public static Column paired(
      @Nonnull final Column texts,
      @Nonnull final Column groups,
      @Nonnull final List<String> metadata,
      final boolean repeating) {
    return ColumnFunctions.resolveOrNone(
        groups, fieldsOf(metadata), List.of(VALUE), List.of(texts), repeating);
  }

  /** The names of the fields read from a metadata group: the metadata fields, then the number. */
  @Nonnull
  private static List<String> fieldsOf(@Nonnull final List<String> metadata) {
    return Stream.concat(metadata.stream(), ANNOTATIONS.stream()).toList();
  }

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
        ColumnFunctions.annotationOrElse(decimal, NUMERIC, kind.compute(NUMERIC, text(decimal))));
  }
}
