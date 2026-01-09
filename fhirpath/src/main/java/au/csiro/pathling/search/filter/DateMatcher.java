/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPathDateTime;
import au.csiro.pathling.sql.misc.HighBoundaryForDateTime;
import au.csiro.pathling.sql.misc.LowBoundaryForDateTime;
import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.spark.sql.Column;

/**
 * Matches elements using range-based comparisons for date search parameters.
 * <p>
 * Per FHIR specification, date searches are intrinsically range-based. Both the element value and
 * search value represent implicit ranges based on their precision. The search value can be prefixed
 * with a comparison operator (e.g., "ge2023-01-15" for greater-or-equal).
 * <p>
 * This matcher supports both scalar date types (date, dateTime, instant) and Period types. For
 * scalar types, UDFs are used to compute precision-aware boundaries. For Period types, the start
 * and end fields are extracted and boundaries computed, with null values treated as negative and
 * positive infinity respectively.
 * <p>
 * Supported prefixes:
 * <ul>
 *   <li>{@code eq} (default) - ranges overlap</li>
 *   <li>{@code ne} - ranges do not overlap</li>
 *   <li>{@code gt} - resource ends after parameter</li>
 *   <li>{@code ge} - resource starts at or after parameter start</li>
 *   <li>{@code lt} - resource starts before parameter</li>
 *   <li>{@code le} - resource ends at or before parameter end</li>
 * </ul>
 * <p>
 * Examples of implicit ranges:
 * <ul>
 *   <li>Element "2013-01-14" (day precision) → [2013-01-14T00:00:00, 2013-01-15T00:00:00)</li>
 *   <li>Search "2013-01" (month precision) → [2013-01-01T00:00:00, 2013-02-01T00:00:00)</li>
 *   <li>Search "2013-01-14T10:00" (minute precision) → [2013-01-14T10:00:00, 2013-01-14T10:01:00)</li>
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
  private static final Timestamp MIN_TIMESTAMP = Timestamp.from(
      LocalDateTime.of(1, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC));

  /**
   * Practical maximum timestamp for unbounded Period end. Using year 9999 to avoid Spark timestamp
   * overflow issues with Instant.MAX.
   */
  private static final Timestamp MAX_TIMESTAMP = Timestamp.from(
      LocalDateTime.of(9999, 12, 31, 23, 59, 59).toInstant(ZoneOffset.UTC));

  private final boolean isPeriodType;

  /**
   * Creates a DateMatcher for scalar date types (date, dateTime, instant).
   */
  public DateMatcher() {
    this(false);
  }

  /**
   * Creates a DateMatcher for either scalar date types or Period type.
   *
   * @param isPeriodType true if the element is a Period type, false for scalar date types
   */
  public DateMatcher(final boolean isPeriodType) {
    this.isPeriodType = isPeriodType;
  }

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    // Parse prefix and date from search value
    final SearchPrefix prefix = SearchPrefix.fromValue(searchValue);
    final String dateValue = SearchPrefix.stripPrefix(searchValue);

    // Parse the date value to determine its precision and compute boundaries
    final FhirPathDateTime searchDateTime = FhirPathDateTime.parse(dateValue);
    final Timestamp paramLow = Timestamp.from(searchDateTime.getLowerBoundary());
    final Timestamp paramHigh = Timestamp.from(searchDateTime.getUpperBoundary());

    // Get element boundaries based on FHIR type
    final Column resourceLow;
    final Column resourceHigh;

    if (isPeriodType) {
      // Period: start/end are STRING fields representing dateTime values
      // Apply UDFs to get precision-aware boundaries, coalesce null to infinity
      resourceLow = coalesce(
          callUDF(LowBoundaryForDateTime.FUNCTION_NAME, element.getField("start")),
          lit(MIN_TIMESTAMP));
      resourceHigh = coalesce(
          callUDF(HighBoundaryForDateTime.FUNCTION_NAME, element.getField("end")),
          lit(MAX_TIMESTAMP));
    } else {
      // Scalar date/dateTime/instant: apply UDFs directly to the element
      resourceLow = callUDF(LowBoundaryForDateTime.FUNCTION_NAME, element);
      resourceHigh = callUDF(HighBoundaryForDateTime.FUNCTION_NAME, element);
    }

    // Apply comparison based on prefix
    final Column comparison = switch (prefix) {
      case EQ -> resourceLow.leq(lit(paramHigh))
          .and(lit(paramLow).leq(resourceHigh));  // overlap (current behavior)
      case NE -> resourceLow.gt(lit(paramHigh))
          .or(resourceHigh.lt(lit(paramLow)));    // no overlap
      case GT -> resourceHigh.gt(lit(paramHigh));
      case GE -> resourceLow.geq(lit(paramLow));
      case LT -> resourceLow.lt(lit(paramLow));
      case LE -> resourceHigh.leq(lit(paramHigh));
    };

    // For Period type, ensure the element itself is not null (resource must have a period)
    if (isPeriodType) {
      return element.isNotNull().and(comparison);
    }
    return comparison;
  }
}
