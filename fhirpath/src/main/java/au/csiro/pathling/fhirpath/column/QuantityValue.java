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

package au.csiro.pathling.fhirpath.column;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.unit.CalendarDurationUnit;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.sql.misc.ConvertQuantityToUnit;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.function.BinaryOperator;
import org.apache.spark.sql.Column;

/**
 * Utility class for working with Quantity columns in SQL operations.
 * <p>
 * Wraps a Quantity struct Column and provides methods for:
 * <ul>
 *   <li>Unit-based conversions (toQuantity, convertsToQuantity)</li>
 *   <li>Quantity comparisons (equals, less than, greater than, etc.)</li>
 *   <li>Accessing different representations (original, normalized, literal)</li>
 * </ul>
 * <p>
 * This class centralizes all Quantity-related SQL column operations and provides a unified
 * abstraction through the {@link ValueWithUnit} record.
 *
 * @author Piotr Szul
 */
public class QuantityValue {

  @Nonnull
  private final Column quantityColumn;

  private QuantityValue(@Nonnull final Column quantityColumn) {
    this.quantityColumn = quantityColumn;
  }

  /**
   * Creates an SQLQuantity wrapper for the specified Quantity column.
   *
   * @param quantityColumn the Quantity struct column to wrap
   * @return an SQLQuantity instance
   */
  @Nonnull
  public static QuantityValue of(@Nonnull final Column quantityColumn) {
    return new QuantityValue(quantityColumn);
  }

  /**
   * Creates an SQLQuantity wrapper for the specified ColumnRepresentation singular value.
   *
   * @param columnRepresentation the ColumnRepresentation to extract single value from.
   * @return an SQLQuantity instance
   */
  @Nonnull
  public static QuantityValue of(@Nonnull final ColumnRepresentation columnRepresentation) {
    return new QuantityValue(
        columnRepresentation.singular("Singular Quantity collection required").getValue());
  }


  /**
   * A helper record to hold a value and a unit column.
   * <p>
   * Provides methods to extract different representations of a Quantity (normalized, original,
   * literal) and to compare two {@link ValueWithUnit} instances using a provided comparator,
   * ensuring that the units are the same.
   */
  public record ValueWithUnit(
      @Nonnull Column value,
      @Nonnull Column unit,
      @Nullable Column system
  ) {

    /**
     * Extracts the normalized (canonicalized) value and unit from a Quantity column.
     * <p>
     * Uses canonicalized_value and canonicalized_code fields, with null system (as canonicalized
     * quantities are system-independent).
     *
     * @param quantity the Quantity column
     * @return a ValueWithUnit instance containing the normalized value and unit
     */
    @Nonnull
    public static ValueWithUnit normalizedValueOf(@Nonnull final Column quantity) {
      return new ValueWithUnit(
          quantity.getField(QuantityEncoding.CANONICALIZED_VALUE_COLUMN),
          quantity.getField(QuantityEncoding.CANONICALIZED_CODE_COLUMN),
          null
      );
    }

    /**
     * Extracts the original value and unit from a Quantity column.
     * <p>
     * Uses value, code (canonical unit code), and system fields. This representation is used for
     * decimal-based comparisons.
     *
     * @param quantity the Quantity column
     * @return a ValueWithUnit instance containing the original value and unit
     */
    @Nonnull
    public static ValueWithUnit originalValueOf(@Nonnull final Column quantity) {
      return new ValueWithUnit(
          quantity.getField(QuantityEncoding.VALUE_COLUMN),
          quantity.getField(QuantityEncoding.CODE_COLUMN),
          quantity.getField(QuantityEncoding.SYSTEM_COLUMN)
      );
    }

    /**
     * Extracts the literal value and unit from a Quantity column.
     * <p>
     * Uses value, unit (literal unit string as written, e.g., "days" instead of "day"), and system
     * fields. This representation is used for exact string matching in unit conversion.
     *
     * @param quantity the Quantity column
     * @return a ValueWithUnit instance containing the literal value and unit
     */
    @Nonnull
    public static ValueWithUnit literalValueOf(@Nonnull final Column quantity) {
      return new ValueWithUnit(
          quantity.getField(QuantityEncoding.VALUE_COLUMN),
          quantity.getField(QuantityEncoding.UNIT_COLUMN),
          lit(null)
      );
    }

    /**
     * Compares this ValueWithUnit with another using the provided value comparator.
     * <p>
     * First checks that units match (and systems match if present), then applies the comparator to
     * the values. Returns null if units/systems don't match.
     *
     * @param other the ValueWithUnit to compare with
     * @param comparator the binary operator to apply to values (e.g., Column::equalTo)
     * @return Column expression with comparison result, or null if units don't match
     */
    @Nonnull
    public Column compare(@Nonnull final ValueWithUnit other,
        @Nonnull final BinaryOperator<Column> comparator) {
      // assert that both (this and other) systems are either non-null or null
      // that is we compare either two original or two normalized quantities
      if ((system == null) != (other.system == null)) {
        throw new IllegalArgumentException("Both quantities must have system or neither.");
      }
      return when(
          nonNull(system)
          ? requireNonNull(system).equalTo(requireNonNull(other.system))
              .and(unit.equalTo(other.unit))
          : unit.equalTo(other.unit),
          comparator.apply(value, other.value)
      );
    }
  }

  /**
   * Gets the original (non-normalized) representation of this Quantity.
   * <p>
   * Returns a ValueWithUnit with value, code (canonical unit), and system fields.
   *
   * @return ValueWithUnit representing the original Quantity
   */
  @Nonnull
  public ValueWithUnit originalValue() {
    return ValueWithUnit.originalValueOf(quantityColumn);
  }

  /**
   * Gets the normalized (canonicalized) representation of this Quantity.
   * <p>
   * Returns a ValueWithUnit with canonicalized_value, canonicalized_code, and null system.
   *
   * @return ValueWithUnit representing the normalized Quantity
   */
  @Nonnull
  public ValueWithUnit normalizedValue() {
    return ValueWithUnit.normalizedValueOf(quantityColumn);
  }

  /**
   * Gets the full Quantity struct column.
   *
   * @return the Quantity column
   */
  @Nonnull
  public Column getColumn() {
    return quantityColumn;
  }

  /**
   * Checks if this quantity uses the UCUM unit system.
   * <p>
   * Returns a boolean column that evaluates to true if the quantity's system field equals
   * {@link UcumUnit#UCUM_SYSTEM_URI}, false otherwise.
   *
   * @return Column of BooleanType indicating if this is a UCUM quantity
   */
  @Nonnull
  public Column isUcum() {
    return quantityColumn.getField(QuantityEncoding.SYSTEM_COLUMN)
        .equalTo(lit(UcumUnit.UCUM_SYSTEM_URI));
  }

  /**
   * Checks if this quantity uses the FHIRPath calendar duration system.
   * <p>
   * Returns a boolean column that evaluates to true if the quantity's system field equals
   * {@link CalendarDurationUnit#FHIRPATH_CALENDAR_DURATION_SYSTEM_URI},
   * false otherwise.
   *
   * @return Column of BooleanType indicating if this is a calendar duration quantity
   */
  @Nonnull
  public Column isCalendarDuration() {
    return quantityColumn.getField(QuantityEncoding.SYSTEM_COLUMN)
        .equalTo(lit(CalendarDurationUnit.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI));
  }

  /**
   * Converts this quantity to the specified unit if units match or are compatible via UCUM
   * conversion, or returns null otherwise. It matches the FHIRPath toXXX() conversion semantics
   * returning null when conversion is not possible.
   * <p>
   * First checks if this is a UCUM or calendar duration quantity. For exact unit match with valid
   * system, returns the quantity as-is (fast path). Otherwise, attempts UCUM unit conversion via
   * the UDF. Returns the converted quantity if conversion is successful, or null if conversion is
   * not possible.
   * <p>
   * Non-UCUM/non-calendar quantities (e.g., Money with system "urn:iso:std:iso:4217") will always
   * return null, even if the unit string happens to match the target unit.
   * <p>
   * This implements the FHIRPath toQuantity(unit) conversion semantics with full UCUM unit
   * conversion support for compatible units (e.g., 'kg' to 'g', 'wk' to 'd').
   *
   * @param targetUnit the target unit to convert to
   * @return Column expression that evaluates to the converted quantity or null if conversion fails
   */
  @Nonnull
  public Column toUnit(@Nonnull final Column targetUnit) {
    final ValueWithUnit literal = ValueWithUnit.literalValueOf(quantityColumn);

    // Try UCUM conversion (will return null for non-UCUM/non-calendar quantities)
    final Column ucumConverted = callUDF(ConvertQuantityToUnit.FUNCTION_NAME,
        quantityColumn, targetUnit);

    // Short-circuit: exact match only if unit matches AND system is UCUM or calendar duration
    // For non-UCUM/non-calendar systems (e.g., Money), fall through to UCUM conversion (returns null)
    final Column hasValidSystem = isUcum().or(isCalendarDuration());
    final Column exactMatchWithValidSystem = literal.unit().equalTo(targetUnit)
        .and(hasValidSystem);

    // Return exact match if available (fast path), otherwise UCUM conversion result (or null)
    return when(exactMatchWithValidSystem, quantityColumn)
        .otherwise(coalesce(ucumConverted, lit(null).cast(QuantityEncoding.dataType())));
  }

  /**
   * Checks if this quantity can be converted to the specified unit via exact match or UCUM
   * conversion.
   * <p>
   * First checks if this is a UCUM or calendar duration quantity with exact unit match. If not,
   * checks if UCUM unit conversion is possible via the UDF. Returns true if the unit matches (with
   * valid system) or conversion is possible, false if not convertible, or null if the quantity row
   * is null (to propagate empty values in the caller's coalesce logic).
   * <p>
   * Non-UCUM/non-calendar quantities (e.g., Money with system "urn:iso:std:iso:4217") will always
   * return false, even if the unit string happens to match the target unit.
   * <p>
   * This implements the FHIRPath convertsToQuantity(unit) validation semantics with full UCUM unit
   * conversion support.
   *
   * @param targetUnit the target unit to check compatibility with
   * @return Column of BooleanType that evaluates to true if convertible and  null for empty rows
   */
  @Nonnull
  public Column convertibleToUnit(@Nonnull final Column targetUnit) {
    final ValueWithUnit literal = ValueWithUnit.literalValueOf(quantityColumn);

    // Check exact string match with valid system (UCUM or calendar duration)
    final Column hasValidSystem = isUcum().or(isCalendarDuration());
    final Column exactMatchWithValidSystem = literal.unit().equalTo(targetUnit)
        .and(hasValidSystem);

    // Check UCUM convertibility by attempting conversion and checking if result is non-null
    final Column ucumConverted = callUDF(ConvertQuantityToUnit.FUNCTION_NAME,
        quantityColumn, targetUnit);
    final Column ucumConvertible = ucumConverted.isNotNull();

    // Return true if either exact match (with valid system) or UCUM conversion is possible
    // Return null if quantity is null (for empty propagation)
    return when(quantityColumn.isNotNull(), exactMatchWithValidSystem.or(ucumConvertible));
  }
}
