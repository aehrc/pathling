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

package au.csiro.pathling.schema.annotation.kinds;

import static au.csiro.pathling.schema.LayoutFields.END_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.START_ANNOTATION;

import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationSwitch;
import au.csiro.pathling.schema.annotation.PerThreadLruCache;
import au.csiro.pathling.schema.date.FhirDateTime;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * The range annotations of a {@code date}, {@code dateTime} or {@code instant}, {@code __start} and
 * {@code __end} (FR-004, decision 135): the earliest and the latest instant the precision the value
 * states allows, as {@link FhirDateTime} computes them.
 *
 * <p>Each bound is its own function of the value, {@code date_start} and {@code date_end}, so that
 * either can be read without the other. Both come from one parse of the value, which is memoised in
 * a cache held per thread, so that computing both bounds of a value parses it once, whether they
 * are computed side by side or in separate passes over an array (decision 145). It is the
 * computation the engine compares dates with where the schema does not carry the annotations, so
 * that the stored and the computed range agree. A time without a timezone is taken as UTC, as the
 * engine takes it (owner, 2026-10-21).
 *
 * <p>The annotations are fields of the date's metadata group, {@code _<element>}, after its {@code
 * id} and {@code extension}, at Spark's {@code TimestampType}. The upper bound is the latest
 * instant to the microsecond, the precision of the type.
 */
public final class DateRangeKind extends AnnotationKind {

  /** The kind. The set of kinds is closed, so there is only the one. */
  @Nonnull public static final DateRangeKind INSTANCE = new DateRangeKind();

  /**
   * The number of parsed dates each thread's cache holds. A plan that reads the two bounds of the
   * dates of an array in two passes parses each date once only where the array is no longer than
   * this ({@code evidence/annotations-design-memoisation.md}).
   */
  public static final int CACHE_CAPACITY = 64;

  /** The parses of dates, memoised per thread. A parse that fails is not cached. */
  @Nonnull
  private static final PerThreadLruCache<String, FhirDateTime> PARSES =
      new PerThreadLruCache<>(CACHE_CAPACITY, FhirDateTime::parse);

  @Nonnull
  private static final UserDefinedFunction START =
      functions
          .udf((UDF1<String, Instant>) DateRangeKind::startOf, DataTypes.TimestampType)
          .withName("date_start");

  @Nonnull
  private static final UserDefinedFunction END =
      functions
          .udf((UDF1<String, Instant>) DateRangeKind::endOf, DataTypes.TimestampType)
          .withName("date_end");

  @Nonnull
  private static final UserDefinedFunction START_OR_NULL =
      functions
          .udf((UDF1<String, Instant>) DateRangeKind::startOrNullOf, DataTypes.TimestampType)
          .withName("date_start_or_null");

  @Nonnull
  private static final UserDefinedFunction END_OR_NULL =
      functions
          .udf((UDF1<String, Instant>) DateRangeKind::endOrNullOf, DataTypes.TimestampType)
          .withName("date_end_or_null");

  private DateRangeKind() {
    super(AnnotationSwitch.RANGE, List.of(START_ANNOTATION, END_ANNOTATION));
  }

  /**
   * Computes one bound of the range of a date from its stored text.
   *
   * <p>A text that does not follow FHIR's date grammar fails the computation with an {@link
   * IllegalArgumentException}, as it fails a comparison in the engine.
   *
   * @param field the bound, {@code __start} or {@code __end}
   * @param value the stored text of the date
   * @return the bound, typed as a timestamp, null where the text is
   */
  @Override
  @Nonnull
  public Column compute(@Nonnull final String field, @Nonnull final Column value) {
    return START_ANNOTATION.equals(requireField(field)) ? start(value) : end(value);
  }

  /**
   * Computes the lower bound of the range of a date from its stored text, {@code date_start}.
   *
   * @param value the stored text of the date
   * @return the lower bound, null where the text is
   */
  @Nonnull
  public Column start(@Nonnull final Column value) {
    return START.apply(value);
  }

  /**
   * Computes the upper bound of the range of a date from its stored text, {@code date_end}.
   *
   * @param value the stored text of the date
   * @return the upper bound, null where the text is
   */
  @Nonnull
  public Column end(@Nonnull final Column value) {
    return END.apply(value);
  }

  /**
   * Computes the lower bound of the range of a text that may not be a date, as a conversion to a
   * date does.
   *
   * @param value the text
   * @return the lower bound, null where the text is or where it does not follow FHIR's date grammar
   */
  @Nonnull
  public Column startOrNull(@Nonnull final Column value) {
    return START_OR_NULL.apply(value);
  }

  /**
   * Computes the upper bound of the range of a text that may not be a date, as a conversion to a
   * date does.
   *
   * @param value the text
   * @return the upper bound, null where the text is or where it does not follow FHIR's date grammar
   */
  @Nonnull
  public Column endOrNull(@Nonnull final Column value) {
    return END_OR_NULL.apply(value);
  }

  /**
   * Returns the number of times a date has been parsed for a bound in this JVM, which shows how
   * often the memoised parse is shared.
   *
   * @return the number of parses
   */
  public static long getParseCount() {
    return PARSES.getComputationCount();
  }

  /**
   * The lower bound of a parsed date. The bounds are given to Spark as instants, which it takes as
   * they are, on the proleptic Gregorian calendar and in no time zone, as it takes the literals the
   * engine compares them with (decision 138).
   */
  @Nullable
  private static Instant startOf(@Nullable final String text) {
    return boundOf(text, FhirDateTime::getLowerBoundary);
  }

  /** The upper bound of a parsed date. */
  @Nullable
  private static Instant endOf(@Nullable final String text) {
    return boundOf(text, FhirDateTime::getUpperBoundary);
  }

  @Nullable
  private static Instant startOrNullOf(@Nullable final String text) {
    return boundOrNullOf(text, FhirDateTime::getLowerBoundary);
  }

  @Nullable
  private static Instant endOrNullOf(@Nullable final String text) {
    return boundOrNullOf(text, FhirDateTime::getUpperBoundary);
  }

  /** A bound of a date, failing where the text is not a date. */
  @Nullable
  private static Instant boundOf(
      @Nullable final String text, @Nonnull final Function<FhirDateTime, Instant> bound) {
    if (text == null) {
      return null;
    }
    try {
      return bound.apply(PARSES.get(text));
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("Invalid date/dateTime format: " + text, e);
    }
  }

  /** A bound of a text that may not be a date, null where it is not one. */
  @Nullable
  private static Instant boundOrNullOf(
      @Nullable final String text, @Nonnull final Function<FhirDateTime, Instant> bound) {
    if (text == null) {
      return null;
    }
    try {
      return bound.apply(PARSES.get(text));
    } catch (final DateTimeParseException e) {
      return null;
    }
  }
}
