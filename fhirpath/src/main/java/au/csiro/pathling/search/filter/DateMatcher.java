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
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPathDateTime;
import au.csiro.pathling.sql.misc.HighBoundaryForDateTime;
import au.csiro.pathling.sql.misc.LowBoundaryForDateTime;
import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
import org.apache.spark.sql.Column;

/**
 * Matches elements using range-based comparisons for date search parameters.
 * <p>
 * Per FHIR specification, date searches are intrinsically range-based. Both the element value and
 * search value represent implicit ranges based on their precision. The search value can be prefixed
 * with a comparison operator (e.g., "ge2023-01-15" for greater-or-equal).
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

    // Get element boundaries using UDFs (handles date strings and precision)
    final Column resourceLow = callUDF(LowBoundaryForDateTime.FUNCTION_NAME, element);
    final Column resourceHigh = callUDF(HighBoundaryForDateTime.FUNCTION_NAME, element);

    // Apply comparison based on prefix
    return switch (prefix) {
      case EQ -> resourceLow.leq(lit(paramHigh))
          .and(lit(paramLow).leq(resourceHigh));  // overlap (current behavior)
      case NE -> resourceLow.gt(lit(paramHigh))
          .or(resourceHigh.lt(lit(paramLow)));    // no overlap
      case GT -> resourceHigh.gt(lit(paramHigh));
      case GE -> resourceLow.geq(lit(paramLow));
      case LT -> resourceLow.lt(lit(paramLow));
      case LE -> resourceHigh.leq(lit(paramHigh));
    };
  }
}
