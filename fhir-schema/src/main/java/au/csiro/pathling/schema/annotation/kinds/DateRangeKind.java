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
import au.csiro.pathling.schema.date.FhirDateTime;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The range annotations of a {@code date}, {@code dateTime} or {@code instant}, {@code __start} and
 * {@code __end} (FR-004, decision 135): the earliest and the latest instant the precision the value
 * states allows, as {@link FhirDateTime} computes them.
 *
 * <p>Both bounds come from one parse of the value, so the kind is one function returning both
 * (owner, 2026-10-26). It is the computation the engine compares dates with where the schema does
 * not carry the annotations, so that the stored and the computed range agree. A time without a
 * timezone is taken as UTC, as the engine takes it (owner, 2026-10-21).
 *
 * <p>The annotations are fields of the date's metadata group, {@code _<element>}, after its {@code
 * id} and {@code extension}, at Spark's {@code TimestampType}. The upper bound is the latest
 * instant to the microsecond, the precision of the type.
 */
public final class DateRangeKind extends AnnotationKind {

  /** The kind. The set of kinds is closed, so there is only the one. */
  @Nonnull public static final DateRangeKind INSTANCE = new DateRangeKind();

  /** The type {@link #compute(Column)} returns: a structure of the two bounds. */
  @Nonnull
  public static final StructType DATA_TYPE =
      new StructType()
          .add(START_ANNOTATION, DataTypes.TimestampType)
          .add(END_ANNOTATION, DataTypes.TimestampType);

  @Nonnull
  private static final UserDefinedFunction RANGE =
      functions.udf((UDF1<String, Row>) DateRangeKind::rangeOf, DATA_TYPE).withName("date_range");

  @Nonnull
  private static final UserDefinedFunction RANGE_OR_NULL =
      functions
          .udf((UDF1<String, Row>) DateRangeKind::rangeOrNullOf, DATA_TYPE)
          .withName("date_range_or_null");

  private DateRangeKind() {
    super(AnnotationSwitch.RANGE, List.of(START_ANNOTATION, END_ANNOTATION));
  }

  /**
   * Computes the range of a date from its stored text.
   *
   * <p>A text that does not follow FHIR's date grammar fails the computation with an {@link
   * IllegalArgumentException}, as it fails a comparison in the engine.
   *
   * @param value the stored text of the date
   * @return the range, in a structure typed as {@link #DATA_TYPE}, null where the text is
   */
  @Override
  @Nonnull
  public Column compute(@Nonnull final Column value) {
    return RANGE.apply(value);
  }

  /**
   * Computes the range of a date from a text that may not be a date, as a conversion to a date
   * does.
   *
   * @param value the text
   * @return the range, in a structure typed as {@link #DATA_TYPE}, null where the text is or where
   *     it does not follow FHIR's date grammar
   */
  @Nonnull
  public Column computeOrNull(@Nonnull final Column value) {
    return RANGE_OR_NULL.apply(value);
  }

  /** The function the range is computed by. */
  @Nullable
  private static Row rangeOf(@Nullable final String text) {
    if (text == null) {
      return null;
    }
    try {
      return rangeOf(FhirDateTime.parse(text));
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("Invalid date/dateTime format: " + text, e);
    }
  }

  /**
   * The range of a parsed date. The bounds are given to Spark as instants, which it takes as they
   * are, on the proleptic Gregorian calendar and in no time zone, as it takes the literals the
   * engine compares them with (decision 138).
   */
  @Nonnull
  private static Row rangeOf(@Nonnull final FhirDateTime parsed) {
    return RowFactory.create(parsed.getLowerBoundary(), parsed.getUpperBoundary());
  }

  /** The function the range of a text that may not be a date is computed by. */
  @Nullable
  private static Row rangeOrNullOf(@Nullable final String text) {
    if (text == null) {
      return null;
    }
    try {
      return rangeOf(FhirDateTime.parse(text));
    } catch (final DateTimeParseException e) {
      return null;
    }
  }
}
