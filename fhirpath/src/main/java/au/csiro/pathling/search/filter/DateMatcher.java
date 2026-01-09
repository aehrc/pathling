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
 * Matches elements using range overlap for date search parameters.
 * <p>
 * Per FHIR specification, date searches are intrinsically range-based. Both the element value and
 * search value represent implicit ranges based on their precision. For the default `eq` prefix,
 * a match occurs when the ranges overlap.
 * <p>
 * Examples of implicit ranges:
 * <ul>
 *   <li>Element "2013-01-14" (day precision) → [2013-01-14T00:00:00, 2013-01-15T00:00:00)</li>
 *   <li>Search "2013-01" (month precision) → [2013-01-01T00:00:00, 2013-02-01T00:00:00)</li>
 *   <li>Search "2013-01-14T10:00" (minute precision) → [2013-01-14T10:00:00, 2013-01-14T10:01:00)</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#date">Date Search</a>
 */
public class DateMatcher implements ElementMatcher {

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    // Parse the search value to determine its precision and compute boundaries
    final FhirPathDateTime searchDateTime = FhirPathDateTime.parse(searchValue);
    final Timestamp searchLow = Timestamp.from(searchDateTime.getLowerBoundary());
    final Timestamp searchHigh = Timestamp.from(searchDateTime.getUpperBoundary());

    // Get element boundaries using UDFs (handles date strings and precision)
    final Column elementLow = callUDF(LowBoundaryForDateTime.FUNCTION_NAME, element);
    final Column elementHigh = callUDF(HighBoundaryForDateTime.FUNCTION_NAME, element);

    // Overlap condition: elementLow <= searchHigh AND searchLow <= elementHigh
    return elementLow.leq(lit(searchHigh))
        .and(lit(searchLow).leq(elementHigh));
  }
}
