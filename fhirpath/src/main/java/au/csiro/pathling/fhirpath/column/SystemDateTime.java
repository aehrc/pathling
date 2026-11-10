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
 * <p>A System date is a flat structure of its text, {@code value}, and its range annotations, its
 * lower bound, {@code __start}, and its upper bound, {@code __end} (decision 152). A FHIR date
 * carries nothing more than its value and its range until the engine reads a primitive's metadata,
 * so it is converted to a System date as it is traversed, as a FHIR decimal is converted to a
 * System decimal. Each bound of a traversed date is the stored annotation where the schema carries
 * it, and is computed from the text otherwise, decided once for the schema and for each bound on
 * its own. The two bounds computed from one text share one memoised parse of it (decision 145). The
 * range of a literal is computed as the query is built.
 *
 * <p>The range is computed from the text of a date as it is traversed or converted, whether it is
 * compared or not. A text outside FHIR's date grammar is not conformant, and under {@code trust}
 * the engine's behaviour on it is undefined (ticket 31).
 */
public final class SystemDateTime {

  /** The name of the text of a System date. */
  @Nonnull public static final String VALUE = "value";

  /** The name of the lower bound of a System date. */
  @Nonnull public static final String START = LayoutFields.START_ANNOTATION;

  /** The name of the upper bound of a System date. */
  @Nonnull public static final String END = LayoutFields.END_ANNOTATION;

  /** The type of a System date. */
  @Nonnull
  public static final StructType DATA_TYPE =
      new StructType()
          .add(VALUE, DataTypes.StringType)
          .add(START, DataTypes.TimestampType)
          .add(END, DataTypes.TimestampType);

  private SystemDateTime() {}

  /**
   * Returns the System date of a text, with the range computed from it.
   *
   * @param text the text of the date
   * @return the System date, null where the text is
   */
  @Nonnull
  public static Column fromText(@Nonnull final Column text) {
    // The text is bound once, because each bound reads it, and a traced text must be evaluated once
    // (#2594). The memoised parse saves the parse, not the evaluation of the text.
    final DateRangeKind kind = DateRangeKind.INSTANCE;
    return let(text, t -> of(t, kind.start(t), kind.end(t)));
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
    final DateRangeKind kind = DateRangeKind.INSTANCE;
    return let(
        text,
        t ->
            when(
                kind.startOrNull(t).isNotNull(),
                struct(
                    t.alias(VALUE),
                    kind.startOrNull(t).alias(START),
                    kind.endOrNull(t).alias(END))));
  }

  /**
   * Returns the System date of a date and its metadata group, with each stored range annotation
   * where the schema carries it, and the bound computed from the text otherwise.
   *
   * @param text the stored text of the date
   * @param group the metadata group beside it
   * @return the System date, null where the text is
   */
  @Nonnull
  public static Column fromStored(@Nonnull final Column text, @Nonnull final Column group) {
    return of(text, storedBound(START, text, group), storedBound(END, text, group));
  }

  /**
   * Returns the System dates of a repeating date and its metadata groups, aligned with the texts by
   * position, with the stored range annotations where the schema carries them, and the bounds
   * computed from the texts otherwise.
   *
   * <p>The groups are paired with the texts by position, so a missing array of groups gives each
   * date null bounds rather than shifting the bounds of other dates.
   *
   * @param texts the stored texts of the date
   * @param groups the metadata groups beside them
   * @return the System dates, null where the texts are
   */
  @Nonnull
  public static Column fromStoredArray(@Nonnull final Column texts, @Nonnull final Column groups) {
    return functions.transform(
        functions.arrays_zip(
            texts.alias(VALUE),
            storedBounds(START, texts, groups).alias(START),
            storedBounds(END, texts, groups).alias(END)),
        each -> of(each.getField(VALUE), each.getField(START), each.getField(END)));
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
        lit(parsed.getLowerBoundary()).alias(START),
        lit(parsed.getUpperBoundary()).alias(END));
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
   * Returns the lower bound of a System date, {@code __start}.
   *
   * @param date the System date
   * @return its lower bound
   */
  @Nonnull
  public static Column start(@Nonnull final Column date) {
    return date.getField(START);
  }

  /**
   * Returns the upper bound of a System date, {@code __end}.
   *
   * @param date the System date
   * @return its upper bound
   */
  @Nonnull
  public static Column end(@Nonnull final Column date) {
    return date.getField(END);
  }

  /**
   * Returns one bound of one stored date: the stored annotation where the schema carries it, and
   * the bound computed from the text otherwise.
   */
  @Nonnull
  private static Column storedBound(
      @Nonnull final String bound, @Nonnull final Column text, @Nonnull final Column group) {
    return DateRangeKind.INSTANCE.apply(bound, text, group, ColumnFunctions::resolveOrElse);
  }

  /**
   * Returns one bound of each of the dates of a repeating date, aligned with them by position: the
   * stored annotation read as one field of all the groups, so that the read prunes to it, where the
   * schema carries it, and the bound computed from each text otherwise. A missing array of groups
   * gives no bounds, which the pairing with the texts fills with nulls.
   */
  @Nonnull
  private static Column storedBounds(
      @Nonnull final String bound, @Nonnull final Column texts, @Nonnull final Column groups) {
    final DateRangeKind kind = DateRangeKind.INSTANCE;
    return functions.coalesce(
        ColumnFunctions.resolveOrElse(
            groups, bound, functions.transform(texts, text -> kind.compute(bound, text))),
        functions.array());
  }

  /** Pairs a text with its bounds, null where the text is. */
  @Nonnull
  private static Column of(
      @Nonnull final Column text, @Nonnull final Column start, @Nonnull final Column end) {
    return when(text.isNotNull(), struct(text.alias(VALUE), start.alias(START), end.alias(END)));
  }
}
