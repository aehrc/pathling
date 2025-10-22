/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
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
   * Converts this quantity to the specified unit if units match, or returns null otherwise. It
   * matches the FHIRPath toXXX() conversion semantics returning null when conversion is not
   * possible.
   * <p>
   * Performs exact string matching on the 'unit' field (literal as written). Returns the original
   * quantity if the unit matches the target unit, or null (cast to Quantity type) if it doesn't
   * match.
   * <p>
   * This implements the FHIRPath toQuantity(unit) conversion semantics with the allowance that
   * "Implementations are not required to support a complete UCUM implementation, and may return
   * empty when the unit argument is used and it is different than the input quantity unit."
   *
   * @param targetUnit the target unit to match against
   * @return Column expression that evaluates to the quantity if units match, null otherwise
   */
  @Nonnull
  public Column toUnit(@Nonnull final Column targetUnit) {
    final ValueWithUnit literal = ValueWithUnit.literalValueOf(quantityColumn);
    return when(literal.unit().equalTo(targetUnit), quantityColumn)
        .otherwise(lit(null).cast(QuantityEncoding.dataType()));
  }

  /**
   * Checks if this quantity can be converted to the specified unit.
   * <p>
   * Performs exact string matching on the 'unit' field (literal as written). Returns true if the
   * unit matches the target unit, or null if the quantity row is null (to propagate empty values in
   * the caller's coalesce logic).
   * <p>
   * This implements the FHIRPath convertsToQuantity(unit) validation semantics.
   *
   * @param targetUnit the target unit to check compatibility with
   * @return Column expression that evaluates to true if convertible, null for empty propagation
   */
  @Nonnull
  public Column convertibleToUnit(@Nonnull final Column targetUnit) {
    final ValueWithUnit literal = ValueWithUnit.literalValueOf(quantityColumn);
    return when(quantityColumn.isNotNull(), literal.unit().equalTo(targetUnit));
  }
}
