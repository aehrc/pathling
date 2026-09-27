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

import static au.csiro.pathling.search.filter.FhirFieldNames.CODE;
import static au.csiro.pathling.search.filter.FhirFieldNames.SYSTEM;
import static au.csiro.pathling.search.filter.FhirFieldNames.UNIT;
import static au.csiro.pathling.search.filter.FhirFieldNames.VALUE;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.encoders.terminology.ucum.Ucum.ValueWithUnit;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.sql.misc.CanonicalQuantityCode;
import au.csiro.pathling.sql.misc.CanonicalQuantityValue;
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.sql.types.FlexiDecimalSupport;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Matches Quantity elements using FHIR quantity search semantics.
 *
 * <p>Supports matching by numeric value with optional system and code constraints. Uses range-based
 * semantics (for eq/ne) or exact value semantics (for gt/ge/lt/le).
 *
 * <p>UCUM Normalization: When the search specifies a UCUM system ({@code
 * http://unitsofmeasure.org}) with a code, the search value and code are canonicalized using the
 * UCUM library. This enables matching across equivalent unit representations (e.g., {@code 1000 mg}
 * matches {@code 1 g}). The canonical form of the element is computed from its value and code, and
 * only for an element whose system is UCUM (FR-022). If canonicalization fails, the matcher falls
 * back to exact value comparison.
 *
 * <p>Every field of the element is read by name through the tolerant traversal, so the element may
 * have any shape the definitions allow, including one pruned to the fields it populates, and its
 * value is decoded from the text it is stored as (FR-054).
 *
 * <p>Supported search formats:
 *
 * <ul>
 *   <li>{@code [number]} - matches by value only, ignoring units
 *   <li>{@code [number]|[system]|[code]} - matches value with exact system and code
 *   <li>{@code [number]||[code]} - matches value and (code or unit), any system
 * </ul>
 *
 * <p>Matching Logic:
 *
 * <ul>
 *   <li>When system is specified: must match exact system AND exact code
 *   <li>When system is NOT specified: code is matched against EITHER code field OR unit field
 * </ul>
 *
 * <p>Supported prefixes:
 *
 * <ul>
 *   <li>{@code eq} (default) - value is within the implicit range
 *   <li>{@code ne} - value is outside the implicit range
 *   <li>{@code gt} - value greater than search value
 *   <li>{@code ge} - value greater than or equal to search value
 *   <li>{@code lt} - value less than search value
 *   <li>{@code le} - value less than or equal to search value
 * </ul>
 *
 * <p>See SPEC_CLARIFICATIONS.md in the project root for interpretation details on format validation
 * and UCUM normalization behavior.
 *
 * @see <a href="https://hl7.org/fhir/search.html#quantity">FHIR Quantity Search</a>
 */
public class QuantityMatcher implements ElementMatcher {

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    final QuantitySearchValue parsedValue = QuantitySearchValue.parse(searchValue);

    // Try UCUM-aware matching if the search specifies UCUM system with a code
    return tryMatchWithUcumNormalization(element, parsedValue)
        .orElseGet(() -> matchStandard(element, parsedValue));
  }

  /**
   * Matches using standard (non-UCUM) semantics.
   *
   * <p>Handles value matching combined with optional system/code constraints based on their
   * presence.
   */
  @Nonnull
  private Column matchStandard(
      @Nonnull final Column element, @Nonnull final QuantitySearchValue parsedValue) {
    final Column valueMatch =
        matchValue(value(element), parsedValue.getNumericValue(), parsedValue.getPrefix());

    return parsedValue
        .getSystem()
        .map(system -> matchWithSystem(element, system, parsedValue, valueMatch))
        .orElseGet(
            () ->
                parsedValue
                    .getCode()
                    .map(code -> valueMatch.and(matchCodeOrUnit(element, code)))
                    .orElse(valueMatch));
  }

  /**
   * Matches value combined with system and code constraints.
   *
   * <p>Used when system is explicitly specified in the search. Requires exact system match and
   * exact code match (if code is specified).
   */
  @Nonnull
  private Column matchWithSystem(
      @Nonnull final Column element,
      @Nonnull final String system,
      @Nonnull final QuantitySearchValue parsedValue,
      @Nonnull final Column valueMatch) {
    return valueMatch
        .and(field(element, SYSTEM).equalTo(lit(system)))
        .and(matchOptionalField(field(element, CODE), parsedValue.getCode()));
  }

  /**
   * Attempts UCUM-aware matching using canonical values.
   *
   * <p>When the search specifies a UCUM system with a code, this method canonicalizes the search
   * value and compares against the canonical form computed from the resource's Quantity. This
   * enables matching across equivalent unit representations (e.g., 1000 mg matches 1 g).
   *
   * @param element the Quantity element column
   * @param parsedValue the parsed search value
   * @return an Optional containing the match condition if UCUM normalization applies, or empty if
   *     standard matching should be used
   */
  @Nonnull
  private Optional<Column> tryMatchWithUcumNormalization(
      @Nonnull final Column element, @Nonnull final QuantitySearchValue parsedValue) {
    // UCUM normalization only applies when both system and code are specified and system is UCUM
    return parsedValue
        .getSystem()
        .filter(UcumUnit.UCUM_SYSTEM_URI::equals)
        .flatMap(system -> buildUcumMatch(element, parsedValue, system));
  }

  /**
   * Builds UCUM-aware match condition with fallback to standard matching.
   *
   * <p>Canonicalizes the search value using UCUM and compares against the canonical form computed
   * from the element. If canonicalization fails, returns empty to trigger standard matching
   * fallback.
   */
  @Nonnull
  private Optional<Column> buildUcumMatch(
      @Nonnull final Column element,
      @Nonnull final QuantitySearchValue parsedValue,
      @Nonnull final String system) {
    return parsedValue
        .getCode()
        .flatMap(
            code -> {
              final BigDecimal numericValue = new BigDecimal(parsedValue.getNumericValue());
              return Optional.ofNullable(Ucum.getCanonical(numericValue, code))
                  .map(
                      canonical ->
                          buildUcumMatchCondition(element, system, code, canonical, parsedValue));
            });
  }

  /**
   * Builds the actual UCUM match condition with fallback logic.
   *
   * <p>Uses coalesce pattern: try canonical match first, fall back to standard matching if null.
   */
  @Nonnull
  private Column buildUcumMatchCondition(
      @Nonnull final Column element,
      @Nonnull final String system,
      @Nonnull final String code,
      @Nonnull final ValueWithUnit canonical,
      @Nonnull final QuantitySearchValue parsedValue) {
    // The canonical form of the element is computed from its stored text and code, and only where
    // the element is a UCUM quantity, because the search asks for a UCUM quantity.
    final Column text = field(element, VALUE);
    final Column elementCode = field(element, CODE);
    final Column canonicalMatch =
        when(
            field(element, SYSTEM)
                .equalTo(lit(UcumUnit.UCUM_SYSTEM_URI))
                .and(
                    callUDF(CanonicalQuantityCode.FUNCTION_NAME, text, elementCode)
                        .equalTo(lit(canonical.unit()))),
            matchCanonicalValue(
                callUDF(CanonicalQuantityValue.FUNCTION_NAME, text, elementCode),
                canonical.value(),
                parsedValue.getPrefix()));

    final Column standardMatch =
        matchValue(value(element), parsedValue.getNumericValue(), parsedValue.getPrefix())
            .and(field(element, SYSTEM).equalTo(lit(system)))
            .and(elementCode.equalTo(lit(code)));

    return coalesce(canonicalMatch, standardMatch);
  }

  /**
   * Matches an optional field constraint.
   *
   * <p>If the constraint is present, the field must equal the value. If the constraint is empty,
   * any value is accepted (no constraint).
   */
  @Nonnull
  private Column matchOptionalField(
      @Nonnull final Column column, @Nonnull final Optional<String> constraint) {
    return constraint.map(value -> column.equalTo(lit(value))).orElse(lit(true));
  }

  /**
   * Matches a code against either the code field OR the unit field.
   *
   * <p>Used when the search specifies code without system (format: {@code [number]||[code]}). Per
   * FHIR spec, this matches "code or unit".
   *
   * <p>Handles NULL values properly using coalesce to ensure the result is always a boolean.
   */
  @Nonnull
  private Column matchCodeOrUnit(@Nonnull final Column element, @Nonnull final String code) {
    return matchFieldOrNull(field(element, CODE), code)
        .or(matchFieldOrNull(field(element, UNIT), code));
  }

  /**
   * Matches a field value, treating NULL as FALSE.
   *
   * <p>Spark SQL comparisons with NULL return NULL, which breaks boolean logic. This helper
   * converts NULL to FALSE for proper OR/AND chaining.
   */
  @Nonnull
  private Column matchFieldOrNull(@Nonnull final Column field, @Nonnull final String value) {
    return coalesce(field.equalTo(lit(value)), lit(false));
  }

  /**
   * Reads a text field of the element through the tolerant traversal, so that a field the schema
   * does not carry is null. The value is read as the text it is stored as on either layout.
   */
  @Nonnull
  private static Column field(@Nonnull final Column element, @Nonnull final String name) {
    return ColumnFunctions.resolveOrNull(element, name, DataTypes.StringType);
  }

  /** Reads the value of the element, decoded from its text for computation. */
  @Nonnull
  private static Column value(@Nonnull final Column element) {
    return field(element, VALUE).try_cast(DecimalCollection.getDecimalType());
  }

  /**
   * Matches the numeric value using range or exact semantics based on prefix.
   *
   * <p>Delegates to {@link NumericMatchingSupport} for the actual comparison logic.
   */
  @Nonnull
  private Column matchValue(
      @Nonnull final Column valueColumn,
      @Nonnull final String numericValue,
      @Nonnull final SearchPrefix prefix) {
    return switch (prefix) {
      case EQ, NE ->
          NumericMatchingSupport.matchWithRangeSemantics(valueColumn, numericValue, prefix);
      case GT, GE, LT, LE ->
          NumericMatchingSupport.matchWithExactSemantics(valueColumn, numericValue, prefix);
    };
  }

  /**
   * Matches canonical value using FlexiDecimal comparison.
   *
   * <p>The canonical value is computed as a FlexiDecimal struct, which requires special comparison
   * methods to handle the value and scale encoding.
   *
   * <p>Note: Range semantics (eq/ne) are simplified for canonical values since the canonicalization
   * process already normalizes values to a consistent form. We use equality comparison for 'eq' and
   * inequality for 'ne'.
   */
  @Nonnull
  private Column matchCanonicalValue(
      @Nonnull final Column canonicalizedValueColumn,
      @Nonnull final BigDecimal searchCanonicalValue,
      @Nonnull final SearchPrefix prefix) {
    final Column searchValueLiteral = FlexiDecimalSupport.toLiteral(searchCanonicalValue);

    return switch (prefix) {
      case EQ -> FlexiDecimal.equalTo(canonicalizedValueColumn, searchValueLiteral);
      case NE -> FlexiDecimal.equalTo(canonicalizedValueColumn, searchValueLiteral).equalTo(false);
      case GT -> FlexiDecimal.gt(canonicalizedValueColumn, searchValueLiteral);
      case GE -> FlexiDecimal.geq(canonicalizedValueColumn, searchValueLiteral);
      case LT -> FlexiDecimal.lt(canonicalizedValueColumn, searchValueLiteral);
      case LE -> FlexiDecimal.leq(canonicalizedValueColumn, searchValueLiteral);
    };
  }
}
