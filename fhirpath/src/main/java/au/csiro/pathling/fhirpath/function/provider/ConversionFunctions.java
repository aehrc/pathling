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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.functions;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Contains functions for converting values between types.
 * <p>
 * This implementation provides 16 FHIRPath conversion functions:
 * <ul>
 *   <li>8 conversion functions: toBoolean, toInteger, toDecimal, toString, toDate, toDateTime, toTime, toQuantity</li>
 *   <li>8 validation functions: convertsToBoolean, convertsToInteger, convertsToDecimal, convertsToString, convertsToDate, convertsToDateTime, convertsToTime, convertsToQuantity</li>
 * </ul>
 * <p>
 * <b>Note:</b> The toLong() and convertsToLong() functions are not implemented as they are marked
 * as STU (Standard for Trial Use) in the FHIRPath specification and are not yet finalized.
 * When these functions are finalized in the FHIRPath specification, they can be added following
 * the same pattern as the other conversion functions.
 * <p>
 * The actual conversion and validation logic is delegated to package-private helper classes:
 * <ul>
 *   <li>{@link ConversionLogic} - handles type conversion orchestration and logic</li>
 *   <li>{@link ValidationLogic} - handles conversion validation orchestration and logic</li>
 * </ul>
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
 * Conversion</a>
 */
@UtilityClass
@SuppressWarnings("unused")
public class ConversionFunctions {

  // ========== PUBLIC API - CONVERSION FUNCTIONS ==========

  /**
   * Converts the input to a Boolean value. Per FHIRPath specification: - String: 'true', 't',
   * 'yes', 'y', '1', '1.0' → true (case-insensitive) - String: 'false', 'f', 'no', 'n', '0', '0.0'
   * → false (case-insensitive) - Integer: 1 → true, 0 → false - Decimal: 1.0 → true, 0.0 → false -
   * All other inputs → empty
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#tobooleanboolean">FHIRPath
   * Specification - toBoolean</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toBoolean(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.BOOLEAN);
  }

  /**
   * Converts the input to an Integer value. Returns the integer value for boolean (true=1,
   * false=0), integer, or valid integer strings. Returns empty for all other inputs.
   *
   * @param input The input collection
   * @return An {@link IntegerCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#toIntegerinteger">FHIRPath
   * Specification - toInteger</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toInteger(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.INTEGER);
  }

  /**
   * Converts the input to a Decimal value. Returns the decimal value for boolean (true=1.0,
   * false=0.0), integer, decimal, or valid decimal strings. Returns empty for all other inputs.
   *
   * @param input The input collection
   * @return A {@link DecimalCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#todecimaldecimal">FHIRPath
   * Specification - toDecimal</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toDecimal(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.DECIMAL);
  }

  /**
   * Converts the input to a String value. All primitive types can be converted to string.
   *
   * @param input The input collection
   * @return A {@link StringCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#tostringstring">FHIRPath Specification
   * - toString</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toString(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.STRING);
  }

  /**
   * Converts the input to a Date value. Accepts strings in ISO 8601 date format.
   *
   * @param input The input collection
   * @return A {@link DateCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * toDate</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toDate(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.DATE);
  }

  /**
   * Converts the input to a DateTime value. Accepts strings in ISO 8601 datetime format.
   *
   * @param input The input collection
   * @return A {@link DateTimeCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * toDateTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toDateTime(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.DATETIME);
  }

  /**
   * Converts the input to a Time value. Accepts strings in ISO 8601 time format.
   *
   * @param input The input collection
   * @return A {@link TimeCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * toTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toTime(@Nonnull final Collection input) {
    return ConversionLogic.performConversion(input, FhirPathType.TIME);
  }

  /**
   * Converts the input to a Quantity value. Per FHIRPath specification:
   * <ul>
   *   <li>Boolean: true → 1.0 '1', false → 0.0 '1'</li>
   *   <li>Integer/Decimal: Convert to Quantity with default unit '1'</li>
   *   <li>Quantity: returns as-is</li>
   *   <li>String: Parse as FHIRPath quantity literal (e.g., "10 'mg'", "4 days")</li>
   *   <li>All other inputs → empty</li>
   * </ul>
   * <p>
   * The optional {@code unit} parameter specifies a target unit for validation. If provided,
   * the function returns the converted quantity only if its unit code matches the target unit
   * (exact string match). If units don't match, returns empty.
   * <p>
   * <b>Note:</b> Full UCUM unit conversion and calendar duration conversions are not yet
   * supported. Future enhancements will add these capabilities.
   *
   * @param input The input collection
   * @param unit Optional target unit for validation (null if not specified)
   * @return A {@link QuantityCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * toQuantity</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection toQuantity(@Nonnull final Collection input,
      @Nullable final Collection unit) {
    // First convert to Quantity using standard conversion
    final Collection converted = ConversionLogic.performConversion(input, FhirPathType.QUANTITY);
    // If unit provided and result is QuantityCollection, apply unit conversion
    if (unit != null && converted instanceof QuantityCollection quantityCollection) {
      return quantityCollection.toUnit(requireNonNull(unit));
    } else {
      return converted;
    }
  }

  // ========== PUBLIC API - VALIDATION FUNCTIONS ==========

  /**
   * Checks if the input can be converted to a Boolean value. Per FHIRPath specification: - Boolean:
   * always convertible - String: 'true', 't', 'yes', 'y', '1', '1.0', 'false', 'f', 'no', 'n', '0',
   * '0.0' (case-insensitive) - Integer: 0 or 1 - Decimal: 0.0 or 1.0 - Empty collection: returns
   * empty
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToBoolean</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToBoolean(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.BOOLEAN);
  }

  /**
   * Checks if the input can be converted to an Integer value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToInteger</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToInteger(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.INTEGER);
  }

  /**
   * Checks if the input can be converted to a Decimal value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDecimal</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToDecimal(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.DECIMAL);
  }

  /**
   * Checks if the input can be converted to a String value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToString</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToString(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.STRING);
  }

  /**
   * Checks if the input can be converted to a Date value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDate</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToDate(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.DATE);
  }

  /**
   * Checks if the input can be converted to a DateTime value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDateTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToDateTime(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.DATETIME);
  }

  /**
   * Checks if the input can be converted to a Time value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToTime(@Nonnull final Collection input) {
    return ValidationLogic.performValidation(input, FhirPathType.TIME);
  }

  /**
   * Checks if the input can be converted to a Quantity value.
   * <p>
   * The optional {@code unit} parameter specifies a target unit for validation. If provided, the
   * function returns true only if the input can be converted to a Quantity AND the resulting
   * quantity's unit code matches the target unit (exact string match). If units don't match,
   * returns false.
   * <p>
   * <b>Note:</b> Full UCUM unit conversion and calendar duration conversions are not yet
   * supported. Future enhancements will add these capabilities.
   *
   * @param input The input collection
   * @param unit Optional target unit for validation (null if not specified)
   * @return A {@link BooleanCollection} containing {@code true} if convertible, {@code false}
   * otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToQuantity</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public Collection convertsToQuantity(@Nonnull final Collection input,
      @Nullable final Collection unit) {
    final Collection canConvertToQuantity = ValidationLogic.performValidation(input,
        FhirPathType.QUANTITY);

    // Only evaluate if unit is provided
    @Nullable final Collection converted =
        unit != null
        ? ConversionLogic.performConversion(input, FhirPathType.QUANTITY)
        : null;

    if (unit != null && converted instanceof QuantityCollection quantityCollection) {
      final Collection canConvertToUnit = quantityCollection.convertibleToUnit(
          requireNonNull(unit));
      return canConvertToQuantity.mapColumn(
          c -> functions.coalesce(canConvertToUnit.getColumnValue(), c));
    } else {
      return canConvertToQuantity;
    }
  }
}
