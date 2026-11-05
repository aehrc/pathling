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

import static au.csiro.pathling.schema.ColumnBinding.bindOnce;
import static au.csiro.pathling.sql.SqlFunctions.let;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import au.csiro.pathling.schema.date.FhirDateTime;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The columns of System dates and dateTimes, the form in which the engine holds every date,
 * dateTime and instant (FR-004, FR-022, FR-023).
 *
 * <p>A System date is a structure of its text, {@code value}, and its range, {@code range}, which
 * is a structure of its lower bound, {@code __start}, and its upper bound, {@code __end}. A FHIR
 * date carries nothing more than its value and its range until the engine reads a primitive's
 * metadata, so it is converted to a System date as it is traversed, as a FHIR decimal is converted
 * to a System decimal. The range of a traversed date is the stored range annotations where the
 * schema carries them, and is computed from the text otherwise, decided once for the schema. The
 * range of a literal is computed as the query is built.
 *
 * <p>The range is computed from the text of a date as it is traversed or converted, whether it is
 * compared or not. A text outside FHIR's date grammar is not conformant, and under {@code trust}
 * the engine's behaviour on it is undefined (ticket 31).
 */
public final class SystemDateTime {

  /** The name of the text of a System date. */
  @Nonnull public static final String VALUE = "value";

  /** The name of the range of a System date. */
  @Nonnull public static final String RANGE = "range";

  /** The type of a System date. */
  @Nonnull
  public static final StructType DATA_TYPE =
      new StructType().add(VALUE, DataTypes.StringType).add(RANGE, DateRangeKind.DATA_TYPE);

  private SystemDateTime() {}

  /**
   * Returns the System date of a text, with the range computed from it.
   *
   * @param text the text of the date
   * @return the System date, null where the text is
   */
  @Nonnull
  public static Column fromText(@Nonnull final Column text) {
    return let(text, t -> of(t, DateRangeKind.INSTANCE.compute(t)));
  }

  /**
   * Returns the System date of a text that may not be a date, as a conversion to a date gives it:
   * null where the text does not follow FHIR's date grammar, or names a date or time that does not
   * exist.
   *
   * @param text the text
   * @return the System date, null where the text is not a date
   */
  @Nonnull
  public static Column tryFromText(@Nonnull final Column text) {
    // The range is bound once, so that the text is parsed once for the check and the value.
    return let(
        text,
        t ->
            bindOnce(
                DateRangeKind.INSTANCE.computeOrNull(t),
                range -> when(range.isNotNull(), struct(t.alias(VALUE), range.alias(RANGE)))));
  }

  /**
   * Returns the System date of a date and its metadata group, with the stored range annotations
   * where the schema carries them, and the range computed from the text otherwise.
   *
   * @param text the stored text of the date
   * @param group the metadata group beside it
   * @return the System date, null where the text is
   */
  @Nonnull
  public static Column fromStored(@Nonnull final Column text, @Nonnull final Column group) {
    return of(text, storedRange(text, group));
  }

  /**
   * Returns the System dates of a repeating date and its metadata groups, aligned with the texts by
   * position, with the stored range annotations where the schema carries them, and the ranges
   * computed from the texts otherwise.
   *
   * <p>The groups are paired with the texts by position, so a missing array of groups gives each
   * date a null range rather than shifting the ranges of other dates.
   *
   * @param texts the stored texts of the date
   * @param groups the metadata groups beside them
   * @return the System dates, null where the texts are
   */
  @Nonnull
  public static Column fromStoredArray(@Nonnull final Column texts, @Nonnull final Column groups) {
    final DateRangeKind kind = DateRangeKind.INSTANCE;
    // The ranges are read as fields of all the groups, so that the read prunes to them.
    final Column ranges =
        ColumnFunctions.resolveFieldsOrElse(
            groups, kind.getFields(), functions.transform(texts, kind::compute));
    return functions.transform(
        functions.arrays_zip(
            texts.alias(VALUE), functions.coalesce(ranges, functions.array()).alias(RANGE)),
        pair -> of(pair.getField(VALUE), pair.getField(RANGE)));
  }

  /**
   * Returns the absent System date, typed as one.
   *
   * @return a null System date
   */
  @Nonnull
  public static Column empty() {
    return lit(null).cast(DATA_TYPE);
  }

  /**
   * Returns the System date of a literal, with the range computed as the query is built.
   *
   * @param text the text of the date
   * @return the System date
   */
  @Nonnull
  public static Column fromLiteral(@Nonnull final String text) {
    final FhirDateTime parsed = FhirDateTime.parse(text);
    // Instants, which Spark takes as they are, as it takes the ones the range annotation's kind
    // computes.
    return struct(
        lit(text).alias(VALUE),
        struct(
                lit(parsed.getLowerBoundary()).alias(LayoutFields.START_ANNOTATION),
                lit(parsed.getUpperBoundary()).alias(LayoutFields.END_ANNOTATION))
            .alias(RANGE));
  }

  /**
   * Returns the text of a System date.
   *
   * @param date the System date
   * @return its text
   */
  @Nonnull
  public static Column text(@Nonnull final Column date) {
    return date.getField(VALUE);
  }

  /**
   * Returns the range of a System date, a structure of its {@code __start} and {@code __end}.
   *
   * @param date the System date
   * @return its range
   */
  @Nonnull
  public static Column range(@Nonnull final Column date) {
    return date.getField(RANGE);
  }

  /**
   * Returns the range of one stored date: the stored range annotations where the schema carries
   * them, and the range computed from the text otherwise, from one parse for both bounds.
   */
  @Nonnull
  private static Column storedRange(@Nonnull final Column text, @Nonnull final Column group) {
    final DateRangeKind kind = DateRangeKind.INSTANCE;
    return ColumnFunctions.resolveFieldsOrElse(group, kind.getFields(), kind.compute(text));
  }

  /** Pairs a text with its range, null where the text is. */
  @Nonnull
  private static Column of(@Nonnull final Column text, @Nonnull final Column range) {
    return when(text.isNotNull(), struct(text.alias(VALUE), range.alias(RANGE)));
  }
}
