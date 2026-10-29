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

package au.csiro.pathling.search.filter;

import static au.csiro.pathling.encoders.ColumnFunctions.resolveOrElse;
import static au.csiro.pathling.encoders.ColumnFunctions.resolveOrNull;
import static au.csiro.pathling.encoders.ColumnFunctions.resolveStringOrNull;
import static au.csiro.pathling.schema.ColumnBinding.bindOnce;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPathDateTime;
import au.csiro.pathling.fhirpath.column.SystemDateTime;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Matches elements using range-based comparisons for date search parameters.
 *
 * <p>Per FHIR specification, date searches are intrinsically range-based. Both the element value
 * and search value represent implicit ranges based on their precision. The search value can be
 * prefixed with a comparison operator (e.g., "ge2023-01-15" for greater-or-equal).
 *
 * <p>This matcher supports both scalar date types (date, dateTime, instant) and Period types. For
 * scalar types, the precision-aware boundaries are the range each value carries as a System date,
 * which is the stored range annotations where the schema carries them, and is computed from one
 * parse of each value otherwise. For Period types, the lower bound of the start and the upper bound
 * of the end are read the same way, with null values treated as negative and positive infinity
 * respectively.
 *
 * <p>Supported prefixes:
 *
 * <ul>
 *   <li>{@code eq} (default) - ranges overlap
 *   <li>{@code ne} - ranges do not overlap
 *   <li>{@code gt} - resource ends after parameter
 *   <li>{@code ge} - resource starts at or after parameter start
 *   <li>{@code lt} - resource starts before parameter
 *   <li>{@code le} - resource ends at or before parameter end
 * </ul>
 *
 * <p>Examples of implicit ranges:
 *
 * <ul>
 *   <li>Element "2013-01-14" (day precision) → [2013-01-14T00:00:00, 2013-01-15T00:00:00)
 *   <li>Search "2013-01" (month precision) → [2013-01-01T00:00:00, 2013-02-01T00:00:00)
 *   <li>Search "2013-01-14T10:00" (minute precision) → [2013-01-14T10:00:00, 2013-01-14T10:01:00)
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#date">Date Search</a>
 * @see <a href="https://hl7.org/fhir/search.html#prefix">Search Prefixes</a>
 */
public class DateMatcher implements ElementMatcher {

  /**
   * Practical minimum timestamp for unbounded Period start. Using year 0001 to avoid Spark
   * timestamp overflow issues with Instant.MIN.
   */
  private static final Instant MIN_TIMESTAMP =
      LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC);

  /**
   * Practical maximum timestamp for unbounded Period end. Using year 9999 to avoid Spark timestamp
   * overflow issues with Instant.MAX.
   */
  private static final Instant MAX_TIMESTAMP =
      LocalDateTime.of(9999, 12, 31, 23, 59, 59).toInstant(ZoneOffset.UTC);

  private final boolean isPeriodType;

  /**
   * Creates a DateMatcher for scalar date types (date, dateTime, instant).
   *
   * @return a new DateMatcher configured for scalar date types
   */
  @Nonnull
  public static DateMatcher forScalarDates() {
    return new DateMatcher(false);
  }

  /**
   * Creates a DateMatcher for Period types.
   *
   * @return a new DateMatcher configured for Period types
   */
  @Nonnull
  public static DateMatcher forPeriod() {
    return new DateMatcher(true);
  }

  /**
   * Creates a DateMatcher for either scalar date types or Period type. Prefer using {@link
   * #forScalarDates()} or {@link #forPeriod()} factory methods.
   *
   * @param isPeriodType true if the element is a Period type, false for scalar date types
   */
  private DateMatcher(final boolean isPeriodType) {
    this.isPeriodType = isPeriodType;
  }

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    if (isPeriodType) {
      // Period: start/end are STRING fields representing dateTime values. The lower bound of the
      // start and the upper bound of the end are each read from the stored range annotation where
      // the schema carries it, and computed from one parse of the text otherwise, and a null one is
      // taken as infinity. Each bound is read through the tolerant traversal, so a bound the
      // schema does not carry is open, as a null one is (FR-024, FR-054).
      final Column resourceLow =
          coalesce(bound(element, "start", LayoutFields.START_ANNOTATION), lit(MIN_TIMESTAMP));
      final Column resourceHigh =
          coalesce(bound(element, "end", LayoutFields.END_ANNOTATION), lit(MAX_TIMESTAMP));
      // The element itself must not be null (the resource must have a period).
      return element.isNotNull().and(compare(searchValue, resourceLow, resourceHigh));
    }
    // Scalar date/dateTime/instant: a System date, whose range is read once for both bounds.
    return bindOnce(SystemDateTime.range(element), range -> matchRange(range, searchValue));
  }

  /** Matches the range of a date, a structure of its two bounds, against the search value. */
  @Nonnull
  private static Column matchRange(@Nonnull final Column range, @Nonnull final String searchValue) {
    return compare(
        searchValue,
        range.getField(LayoutFields.START_ANNOTATION),
        range.getField(LayoutFields.END_ANNOTATION));
  }

  /**
   * Returns one bound of one of a Period's dates: the stored range annotation where the schema
   * carries it, and the bound computed from the date's text otherwise.
   */
  @Nonnull
  private static Column bound(
      @Nonnull final Column period, @Nonnull final String date, @Nonnull final String annotation) {
    return resolveOrElse(
        resolveOrNull(period, LayoutFields.metadataGroupName(date), DataTypes.NullType),
        annotation,
        DateRangeKind.INSTANCE.compute(resolveStringOrNull(period, date)).getField(annotation));
  }

  /**
   * Compares the range of an element with the range of the search value, as the search value's
   * prefix says.
   */
  @Nonnull
  private static Column compare(
      @Nonnull final String searchValue,
      @Nonnull final Column resourceLow,
      @Nonnull final Column resourceHigh) {
    // Parse the prefix and the date from the search value.
    final SearchPrefix prefix = SearchPrefix.fromValue(searchValue);
    final String dateValue = SearchPrefix.stripPrefix(searchValue);

    // Parse the date value to determine its precision and compute its boundaries.
    final FhirPathDateTime searchDateTime = FhirPathDateTime.parse(dateValue);
    // Instants, which Spark takes as they are, as it takes the range annotation's bounds.
    final Instant paramLow = searchDateTime.getLowerBoundary();
    final Instant paramHigh = searchDateTime.getUpperBoundary();
    return compare(prefix, resourceLow, resourceHigh, paramLow, paramHigh);
  }

  /** Compares the range of an element with the range of the search value, as a prefix says. */
  @Nonnull
  private static Column compare(
      @Nonnull final SearchPrefix prefix,
      @Nonnull final Column resourceLow,
      @Nonnull final Column resourceHigh,
      @Nonnull final Instant paramLow,
      @Nonnull final Instant paramHigh) {
    return switch (prefix) {
      case EQ ->
          resourceLow
              .leq(lit(paramHigh))
              .and(lit(paramLow).leq(resourceHigh)); // Overlap (current behavior).
      case NE -> resourceLow.gt(lit(paramHigh)).or(resourceHigh.lt(lit(paramLow))); // No overlap.
      case GT -> resourceHigh.gt(lit(paramHigh));
      case GE -> resourceLow.geq(lit(paramLow));
      case LT -> resourceLow.lt(lit(paramLow));
      case LE -> resourceHigh.leq(lit(paramHigh));
    };
  }
}
