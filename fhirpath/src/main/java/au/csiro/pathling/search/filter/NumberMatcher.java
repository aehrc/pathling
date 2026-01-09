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

import static org.apache.spark.sql.functions.lit;

import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import org.apache.spark.sql.Column;

/**
 * Matches elements using direct comparison for number search parameters.
 * <p>
 * This is a simplified implementation that performs direct value comparison without the complex
 * implicit range semantics based on significant figures that the full FHIR specification
 * describes.
 * <p>
 * Supported prefixes:
 * <ul>
 *   <li>{@code eq} (default) - exact value match</li>
 *   <li>{@code ne} - value does not match</li>
 *   <li>{@code gt} - value greater than search value</li>
 *   <li>{@code ge} - value greater than or equal to search value</li>
 *   <li>{@code lt} - value less than search value</li>
 *   <li>{@code le} - value less than or equal to search value</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#number">FHIR Number Search</a>
 * @see <a href="https://hl7.org/fhir/search.html#prefix">Search Prefixes</a>
 */
public class NumberMatcher implements ElementMatcher {

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    // Parse prefix and number from search value
    final NumberPrefix prefix = NumberPrefix.fromValue(searchValue);
    final String numberValue = NumberPrefix.stripPrefix(searchValue);

    // Parse the number value as BigDecimal for precise comparison
    final BigDecimal searchNumber = new BigDecimal(numberValue);

    // Apply comparison based on prefix
    return switch (prefix) {
      case EQ -> element.equalTo(lit(searchNumber));
      case NE -> element.notEqual(lit(searchNumber));
      case GT -> element.gt(lit(searchNumber));
      case GE -> element.geq(lit(searchNumber));
      case LT -> element.lt(lit(searchNumber));
      case LE -> element.leq(lit(searchNumber));
    };
  }
}
