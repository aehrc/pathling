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

import au.csiro.pathling.fhirpath.FhirPathNumber;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Matches elements using FHIR numeric search semantics.
 * <p>
 * FHIR numeric search uses implicit range semantics based on significant figures for equality
 * comparisons. For example, searching for "100" (3 significant figures) matches any value in the
 * range [99.5, 100.5).
 * <p>
 * For integer types (integer, positiveInt, unsignedInt), the eq prefix uses exact match semantics
 * when the search value has no fractional part, and returns no matches when the search value has a
 * non-zero fractional part.
 * <p>
 * Comparison prefixes (gt, ge, lt, le) use exact value semantics without implicit ranges.
 * <p>
 * Supported prefixes:
 * <ul>
 *   <li>{@code eq} (default) - value is within the implicit range (or exact match for integers)</li>
 *   <li>{@code ne} - value is outside the implicit range (or not equal for integers)</li>
 *   <li>{@code gt} - value greater than search value (exact)</li>
 *   <li>{@code ge} - value greater than or equal to search value (exact)</li>
 *   <li>{@code lt} - value less than search value (exact)</li>
 *   <li>{@code le} - value less than or equal to search value (exact)</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#number">FHIR Number Search</a>
 * @see <a href="https://hl7.org/fhir/search.html#prefix">Search Prefixes</a>
 */
public class NumberMatcher implements ElementMatcher {

  private static final Set<FHIRDefinedType> INTEGER_TYPES = Set.of(
      FHIRDefinedType.INTEGER,
      FHIRDefinedType.POSITIVEINT,
      FHIRDefinedType.UNSIGNEDINT
  );

  @Nonnull
  private final FHIRDefinedType fhirType;

  /**
   * Creates a NumberMatcher for the specified FHIR type.
   *
   * @param fhirType the FHIR type of the element being matched
   */
  public NumberMatcher(@Nonnull final FHIRDefinedType fhirType) {
    this.fhirType = fhirType;
  }

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    // Parse prefix and number from search value
    final SearchPrefix prefix = SearchPrefix.fromValue(searchValue);
    final String numberValue = SearchPrefix.stripPrefix(searchValue);

    // For integer types, use special integer matching semantics for eq/ne
    if (isIntegerType() && (prefix == SearchPrefix.EQ || prefix == SearchPrefix.NE)) {
      return matchIntegerType(element, numberValue, prefix);
    }

    // For eq/ne on decimal types, use range-based semantics
    // For comparison prefixes (gt, ge, lt, le), use exact value semantics
    return switch (prefix) {
      case EQ, NE -> matchWithRangeSemantics(element, numberValue, prefix);
      case GT, GE, LT, LE -> matchWithExactSemantics(element, numberValue, prefix);
    };
  }

  /**
   * Checks if the target FHIR type is an integer type.
   *
   * @return true if the type is integer, positiveInt, or unsignedInt
   */
  private boolean isIntegerType() {
    return INTEGER_TYPES.contains(fhirType);
  }

  /**
   * Matches integer types using exact match semantics.
   * <p>
   * If the search value has a non-zero fractional part, no matches are possible (returns false). If
   * the search value has no fractional part, exact match semantics are used.
   *
   * @param element the column to match against
   * @param numberValue the numeric search value (without prefix)
   * @param prefix the search prefix (EQ or NE)
   * @return a Column expression for the match condition
   */
  @Nonnull
  private Column matchIntegerType(@Nonnull final Column element,
      @Nonnull final String numberValue,
      @Nonnull final SearchPrefix prefix) {
    final FhirPathNumber searchNumber = FhirPathNumber.parse(numberValue);

    // If search value has a fractional part, no integer can match
    if (searchNumber.hasFractionalPart()) {
      return switch (prefix) {
        case EQ -> lit(false);  // No integer equals a fractional value
        case NE -> lit(true);   // All integers are not equal to a fractional value
        default -> throw new IllegalArgumentException("Unexpected prefix: " + prefix);
      };
    }

    // For integer search values, use exact match semantics
    final BigDecimal searchValueDecimal = searchNumber.getValue();
    return switch (prefix) {
      case EQ -> element.equalTo(lit(searchValueDecimal));
      case NE -> element.notEqual(lit(searchValueDecimal));
      default -> throw new IllegalArgumentException("Unexpected prefix: " + prefix);
    };
  }

  /**
   * Matches using range semantics based on significant figures.
   * <p>
   * For scalar resource values (treated as having infinite precision), the value matches if it
   * falls within the parameter's implicit range [lower, upper).
   *
   * @param element the column to match against
   * @param numberValue the numeric search value (without prefix)
   * @param prefix the search prefix (EQ or NE)
   * @return a Column expression for the match condition
   */
  @Nonnull
  private Column matchWithRangeSemantics(@Nonnull final Column element,
      @Nonnull final String numberValue,
      @Nonnull final SearchPrefix prefix) {
    // Parse the number to get precision-aware boundaries
    final FhirPathNumber searchNumber = FhirPathNumber.parse(numberValue);
    final BigDecimal lowerBoundary = searchNumber.getLowerBoundary();
    final BigDecimal upperBoundary = searchNumber.getUpperBoundary();

    // For resource values (treated as points with infinite precision):
    // EQ: value is within [lower, upper)
    // NE: value is outside [lower, upper)
    return switch (prefix) {
      case EQ -> element.geq(lit(lowerBoundary)).and(element.lt(lit(upperBoundary)));
      case NE -> element.lt(lit(lowerBoundary)).or(element.geq(lit(upperBoundary)));
      default -> throw new IllegalArgumentException("Unexpected prefix for range semantics: "
          + prefix);
    };
  }

  /**
   * Matches using exact value semantics (infinite precision).
   * <p>
   * Comparison prefixes ignore the implicit range and treat both the search value and resource
   * value as having arbitrary precision.
   *
   * @param element the column to match against
   * @param numberValue the numeric search value (without prefix)
   * @param prefix the search prefix (GT, GE, LT, or LE)
   * @return a Column expression for the match condition
   */
  @Nonnull
  private Column matchWithExactSemantics(@Nonnull final Column element,
      @Nonnull final String numberValue,
      @Nonnull final SearchPrefix prefix) {
    // Parse the number value as BigDecimal for precise comparison
    final BigDecimal searchNumber = new BigDecimal(numberValue);

    return switch (prefix) {
      case GT -> element.gt(lit(searchNumber));
      case GE -> element.geq(lit(searchNumber));
      case LT -> element.lt(lit(searchNumber));
      case LE -> element.leq(lit(searchNumber));
      default -> throw new IllegalArgumentException("Unexpected prefix for exact semantics: "
          + prefix);
    };
  }
}
