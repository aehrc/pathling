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

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Contains functions for converting values between types.
 * <p>
 * This implementation provides 14 FHIRPath conversion functions:
 * <ul>
 *   <li>7 conversion functions: toBoolean, toInteger, toDecimal, toString, toDate, toDateTime, toTime</li>
 *   <li>7 validation functions: convertsToBoolean, convertsToInteger, convertsToDecimal, convertsToString, convertsToDate, convertsToDateTime, convertsToTime</li>
 * </ul>
 * <p>
 * <b>Note:</b> The toLong() and convertsToLong() functions are not implemented as they are marked
 * as STU (Standard for Trial Use) in the FHIRPath specification and are not yet finalized.
 * When these functions are finalized in the FHIRPath specification, they can be added following
 * the same pattern as the other conversion functions.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
 * Conversion</a>
 */
@SuppressWarnings("unused")
public class ConversionFunctions {

  private ConversionFunctions() {
  }

  // ========== PUBLIC API - CONVERSION FUNCTIONS ==========

  /**
   * Converts the input to a Boolean value. Per FHIRPath specification:
   * - String: 'true', 't', 'yes', 'y', '1', '1.0' → true (case-insensitive)
   * - String: 'false', 'f', 'no', 'n', '0', '0.0' → false (case-insensitive)
   * - Integer: 1 → true, 0 → false
   * - Decimal: 1.0 → true, 0.0 → false
   * - All other inputs → empty
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#tobooleanboolean">FHIRPath
   * Specification - toBoolean</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection toBoolean(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.BOOLEAN,
        ConversionFunctions::convertToBoolean,
        BooleanCollection::build
    );
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
  public static Collection toInteger(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.INTEGER,
        ConversionFunctions::convertToInteger,
        IntegerCollection::build
    );
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
  public static Collection toDecimal(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.DECIMAL,
        ConversionFunctions::convertToDecimal,
        DecimalCollection::build
    );
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
  public static Collection toString(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.STRING,
        ConversionFunctions::convertToString,
        StringCollection::build
    );
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
  public static Collection toDate(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.DATE,
        ConversionFunctions::convertToDate,
        (repr) -> DateCollection.build(repr, Optional.empty())
    );
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
  public static Collection toDateTime(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.DATETIME,
        ConversionFunctions::convertToDateTime,
        (repr) -> DateTimeCollection.build(repr, Optional.empty())
    );
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
  public static Collection toTime(@Nonnull final Collection input) {
    return performConversion(
        input,
        FhirPathType.TIME,
        ConversionFunctions::convertToTime,
        (repr) -> TimeCollection.build(repr, Optional.empty())
    );
  }

  // ========== PUBLIC API - VALIDATION FUNCTIONS ==========

  /**
   * Returns true if the input can be converted to a Boolean value. Per FHIRPath specification:
   * - Boolean: always convertible
   * - String: 'true', 't', 'yes', 'y', '1', '1.0', 'false', 'f', 'no', 'n', '0', '0.0' (case-insensitive)
   * - Integer: 0 or 1
   * - Decimal: 0.0 or 1.0
   * - Empty collection: returns empty
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToBoolean</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToBoolean(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.BOOLEAN,
        ConversionFunctions::validateConversionToBoolean
    );
  }

  /**
   * Returns true if the input can be converted to an Integer value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToInteger</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToInteger(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.INTEGER,
        ConversionFunctions::validateConversionToInteger
    );
  }

  /**
   * Returns true if the input can be converted to a Decimal value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDecimal</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToDecimal(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.DECIMAL,
        ConversionFunctions::validateConversionToDecimal
    );
  }

  /**
   * Returns true if the input can be converted to a String value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToString</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToString(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.STRING,
        ConversionFunctions::validateConversionToString
    );
  }

  /**
   * Returns true if the input can be converted to a Date value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDate</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToDate(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.DATE,
        ConversionFunctions::validateConversionToDate
    );
  }

  /**
   * Returns true if the input can be converted to a DateTime value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDateTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToDateTime(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.DATETIME,
        ConversionFunctions::validateConversionToDateTime
    );
  }

  /**
   * Returns true if the input can be converted to a Time value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty
   * for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToTime(@Nonnull final Collection input) {
    return performValidation(
        input,
        FhirPathType.TIME,
        ConversionFunctions::validateConversionToTime
    );
  }

  // ========== TEMPLATE METHODS (Orchestration) ==========

  /**
   * Template method for performing type conversions. Handles common orchestration: empty check,
   * singular check, identity conversion, delegation to conversion logic, and result building.
   *
   * @param input The input collection to convert
   * @param targetType The target FHIRPath type
   * @param conversionLogic Function that performs the actual type conversion
   * @param collectionBuilder Function that builds the result collection from a DefaultRepresentation
   * @param <T> The type of collection to return
   * @return The converted collection or EmptyCollection if conversion fails
   */
  private static <T extends Collection> Collection performConversion(
      @Nonnull final Collection input,
      @Nonnull final FhirPathType targetType,
      @Nonnull final BiFunction<FhirPathType, Column, Column> conversionLogic,
      @Nonnull final Function<DefaultRepresentation, T> collectionBuilder) {

    // Step 1: Handle empty collection
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    // Step 2: Ensure singular (throws if multiple elements)
    final Collection singular = input.asSingular();
    final FhirPathType sourceType = singular.getType().orElse(null);

    // Step 3: Identity conversion - already target type
    if (sourceType == targetType) {
      return singular;
    }

    // Step 4: Apply conversion logic
    final Column value = singular.getColumn().getValue();
    final Column result = conversionLogic.apply(sourceType, value);

    // Step 5: Build result collection or return empty
    if (result != null) {
      return collectionBuilder.apply(new DefaultRepresentation(result));
    } else {
      // No conversion available - but we must still enforce singularity on the input
      // Create a check using the original input (before singularization)
      final Column singularityCheck = input.getColumn().ensureSingular();
      final Column nullWithCheck = coalesce(singularityCheck, lit(null));
      return collectionBuilder.apply(new DefaultRepresentation(nullWithCheck));
    }
  }

  /**
   * Template method for performing conversion validation. Handles common orchestration for
   * convertsToXXX functions.
   *
   * @param input The input collection to validate
   * @param targetType The target FHIRPath type
   * @param validationLogic Function that checks if conversion is possible
   * @return BooleanCollection indicating whether conversion is possible
   */
  private static Collection performValidation(
      @Nonnull final Collection input,
      @Nonnull final FhirPathType targetType,
      @Nonnull final BiFunction<FhirPathType, Column, Column> validationLogic) {

    // Step 1: Handle empty collection
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    // Step 2: Ensure singular
    final Collection singular = input.asSingular();
    final FhirPathType sourceType = singular.getType().orElse(null);

    // Step 3: Identity check - same type is always convertible
    if (sourceType == targetType) {
      // Enforce singularity on the original input (before singularization)
      // Use coalesce to incorporate the check into the result: returns true if check passes, error if multiple elements
      final Column checkAndTrue = coalesce(input.getColumn().ensureSingular(), lit(true));
      return BooleanCollection.build(new DefaultRepresentation(checkAndTrue));
    }

    // Step 4: Apply validation logic
    final Column value = singular.getColumn().getValue();
    final Column canConvert = validationLogic.apply(sourceType, value);

    // Step 5: Build boolean result with singularity check
    // Enforce singularity on the original input - if array, error; if singular, return validation result
    final Column withCheck = coalesce(input.getColumn().ensureSingular(), canConvert);
    return BooleanCollection.build(new DefaultRepresentation(withCheck));
  }

  // ========== CONVERSION LOGIC METHODS ==========

  /**
   * Converts a value to Boolean based on source type.
   * <ul>
   *   <li>STRING: Special handling for "1.0"/"0.0", then cast</li>
   *   <li>INTEGER: Only 0 or 1</li>
   *   <li>DECIMAL: Only 0.0 or 1.0</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToBoolean(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // String: Handle '1.0' and '0.0' specially, use SparkSQL cast for other values.
        // SparkSQL cast handles 'true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0' (case-insensitive).
          when(value.equalTo(lit("1.0")), lit(true))
              .when(value.equalTo(lit("0.0")), lit(false))
              .otherwise(value.cast(DataTypes.BooleanType));
      case INTEGER ->
        // Integer: Only 0 or 1 can be converted (1 → true, 0 → false, otherwise null).
          when(value.equalTo(lit(1)), lit(true))
              .when(value.equalTo(lit(0)), lit(false));
      case DECIMAL ->
        // Decimal: Only 0.0 or 1.0 can be converted (1.0 → true, 0.0 → false, otherwise null).
          when(value.equalTo(lit(1.0)), lit(true))
              .when(value.equalTo(lit(0.0)), lit(false));
      default -> null;
    };
  }

  /**
   * Converts a value to Integer based on source type.
   * <ul>
   *   <li>BOOLEAN/STRING: Direct cast</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToInteger(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, STRING ->
        // Boolean/String: Use SparkSQL cast (true → 1, false → 0, string → int or null).
          value.cast(DataTypes.IntegerType);
      default -> null;
    };
  }

  /**
   * Converts a value to Decimal based on source type.
   * <ul>
   *   <li>BOOLEAN/INTEGER/STRING: Direct cast to Decimal</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToDecimal(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, STRING ->
        // Boolean/Integer/String: cast to decimal.
          value.cast(DecimalCollection.getDecimalType());
      default -> null;
    };
  }

  /**
   * Converts a value to String based on source type.
   * <ul>
   *   <li>All primitive types: Direct cast to String</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToString(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, DECIMAL, DATE, DATETIME, TIME ->
        // All primitive types can be cast to string.
          value.cast(DataTypes.StringType);
      default -> null;
    };
  }

  /**
   * Converts a value to Date based on source type.
   * <ul>
   *   <li>STRING: Validates format (YYYY or YYYY-MM or YYYY-MM-DD) and returns the string value</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToDate(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // Date values are stored as strings in FHIR. Validate format before accepting.
        // Date format: YYYY or YYYY-MM or YYYY-MM-DD
          when(value.rlike("^\\d{4}(-\\d{2}(-\\d{2})?)?$"), value);
      default -> null;
    };
  }

  /**
   * Converts a value to DateTime based on source type.
   * <ul>
   *   <li>STRING: Validates format (YYYY-MM-DDThh:mm:ss with optional timezone) and returns the string value</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToDateTime(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // DateTime values are stored as strings in FHIR. Validate format before accepting.
        // DateTime format: YYYY-MM-DDThh:mm:ss with optional timezone
          when(value.rlike("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})?$"), value);
      default -> null;
    };
  }

  /**
   * Converts a value to Time based on source type.
   * <ul>
   *   <li>STRING: Validates format (hh:mm:ss with optional milliseconds) and returns the string value</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column, or null if conversion is not possible
   */
  private static Column convertToTime(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // Time values are stored as strings in FHIR. Validate format before accepting.
        // Time format: hh:mm:ss or hh:mm:ss.fff
          when(value.rlike("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$"), value);
      default -> null;
    };
  }

  // ========== VALIDATION LOGIC METHODS ==========

  /**
   * Validates if a value can be converted to Boolean.
   * <ul>
   *   <li>STRING: Check for "1.0"/"0.0" or valid boolean cast</li>
   *   <li>INTEGER: Must be 0 or 1</li>
   *   <li>DECIMAL: Must be 0.0 or 1.0</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToBoolean(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING -> {
        // For strings: check if '1.0'/'0.0' or if cast to boolean succeeds.
        final Column is10or00 = value.equalTo(lit("1.0")).or(value.equalTo(lit("0.0")));
        final Column castSucceeds = value.cast(DataTypes.BooleanType).isNotNull();
        yield value.isNotNull().and(is10or00.or(castSucceeds));
      }
      case INTEGER ->
        // Only 0 and 1 can be converted.
          value.equalTo(lit(0)).or(value.equalTo(lit(1)));
      case DECIMAL ->
        // Only 0.0 and 1.0 can be converted.
          value.equalTo(lit(0.0)).or(value.equalTo(lit(1.0)));
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to Integer.
   * <ul>
   *   <li>BOOLEAN: Always true (any boolean can cast to int)</li>
   *   <li>STRING: Check if cast succeeds</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToInteger(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN ->
        // Boolean can always be converted.
          lit(true);
      case STRING ->
        // Check if value is not null and casting to integer returns non-null.
          value.isNotNull().and(value.cast(DataTypes.IntegerType).isNotNull());
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to Decimal.
   * <ul>
   *   <li>BOOLEAN/INTEGER: Always true</li>
   *   <li>STRING: Check if cast succeeds</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToDecimal(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER ->
        // Boolean and integer can always be converted.
          lit(true);
      case STRING ->
        // Check if value is not null and casting to decimal returns non-null.
          value.isNotNull().and(value.cast(DecimalCollection.getDecimalType()).isNotNull());
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to String. All supported types can convert to String.
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToString(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, DECIMAL, DATE, DATETIME, TIME ->
        // All primitive types can be converted to string.
          lit(true);
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to Date.
   * <ul>
   *   <li>STRING: Check if matches date pattern (YYYY or YYYY-MM or YYYY-MM-DD)</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToDate(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // String can be converted only if it matches the date format: YYYY or YYYY-MM or YYYY-MM-DD
          value.isNotNull().and(value.rlike("^\\d{4}(-\\d{2}(-\\d{2})?)?$"));
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to DateTime.
   * <ul>
   *   <li>STRING: Check if matches datetime pattern (YYYY-MM-DDThh:mm:ss with optional timezone)</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToDateTime(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // String can be converted only if it matches ISO 8601 datetime format.
        // Simplified check: YYYY-MM-DDThh:mm:ss with optional timezone
          value.isNotNull().and(
              value.rlike("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})?$")
          );
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to Time.
   * <ul>
   *   <li>STRING: Check if matches time pattern (hh:mm:ss with optional milliseconds)</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to true if convertible, false otherwise
   */
  private static Column validateConversionToTime(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
        // String can be converted only if it matches time format: hh:mm:ss or hh:mm:ss.fff
          value.isNotNull().and(value.rlike("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$"));
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }
}
