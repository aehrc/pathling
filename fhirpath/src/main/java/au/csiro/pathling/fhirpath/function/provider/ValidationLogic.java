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
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Package-private utility class containing conversion validation orchestration and logic.
 * <p>
 * This class provides the template method for performing conversion validation and all
 * type-specific validation helper methods used by {@link ConversionFunctions}.
 *
 * @author John Grimes
 */
@UtilityClass
class ValidationLogic {

  // Regex constants used by validation logic
  private static final String INTEGER_REGEX = ConversionLogic.INTEGER_REGEX;
  private static final String DECIMAL_REGEX = "^(\\+|-)?\\d+(\\.\\d+)?$";
  private static final String DATE_REGEX = ConversionLogic.DATE_REGEX;
  private static final String DATETIME_REGEX = ConversionLogic.DATETIME_REGEX;
  private static final String TIME_REGEX = ConversionLogic.TIME_REGEX;
  private static final String QUANTITY_REGEX = ConversionLogic.QUANTITY_REGEX;

  // Registry mapping target types to their validation functions
  private static final Map<FhirPathType, BiFunction<FhirPathType, Column, Column>> VALIDATION_REGISTRY =
      Map.ofEntries(
          Map.entry(FhirPathType.BOOLEAN, ValidationLogic::validateConversionToBoolean),
          Map.entry(FhirPathType.INTEGER, ValidationLogic::validateConversionToInteger),
          Map.entry(FhirPathType.DECIMAL, ValidationLogic::validateConversionToDecimal),
          Map.entry(FhirPathType.STRING, ValidationLogic::validateConversionToString),
          Map.entry(FhirPathType.DATE, ValidationLogic::validateConversionToDate),
          Map.entry(FhirPathType.DATETIME, ValidationLogic::validateConversionToDateTime),
          Map.entry(FhirPathType.TIME, ValidationLogic::validateConversionToTime),
          Map.entry(FhirPathType.QUANTITY, ValidationLogic::validateConversionToQuantity)
      );

  /**
   * Template method for performing conversion validation. Handles common orchestration for
   * convertsToXXX functions.
   * <p>
   * The validation function is automatically determined from the target type using the internal
   * registry.
   *
   * @param input The input collection to validate
   * @param targetType The target FHIRPath type
   * @return BooleanCollection indicating whether conversion is possible
   */
  Collection performValidation(
      @Nonnull final Collection input,
      @Nonnull final FhirPathType targetType) {

    // Handle explicit empty collection
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    // Look up validation function from registry
    final BiFunction<FhirPathType, Column, Column> validationLogic = VALIDATION_REGISTRY.get(targetType);

    final Column singularValue = input.getColumn().singular().getValue();
    // Use Nothing when the type is not known to enforce default value for a non-convertible type
    final FhirPathType sourceType = input.getType().orElse(FhirPathType.NOTHING);
    final Column result = sourceType == targetType
                          ? lit(true)
                          : validationLogic.apply(sourceType, singularValue);

    return BooleanCollection.build(new DefaultRepresentation(
        // this triggers singularity check if the result is null
        coalesce(
            when(singularValue.isNotNull(), result),
            input.getColumn().ensureSingular()
        )
        // implicit null otherwise
    ));
  }

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
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToBoolean(@Nonnull final FhirPathType sourceType,
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
   *   <li>BOOLEAN: Always {@code true} (any boolean can cast to int)</li>
   *   <li>STRING: Check if cast succeeds</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToInteger(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN ->
        // Boolean can always be converted.
          lit(true);
      case STRING ->
        // String must match integer format: (\+|-)?\d+ (no decimal point).
          value.rlike(INTEGER_REGEX);
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }

  /**
   * Validates if a value can be converted to Decimal.
   * <ul>
   *   <li>BOOLEAN/INTEGER: Always {@code true}</li>
   *   <li>STRING: Check if cast succeeds</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToDecimal(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER ->
        // Boolean and integer can always be converted.
          lit(true);
      case STRING ->
        // String must match decimal format: (\+|-)?\d+(\.\d+)?
          value.rlike(DECIMAL_REGEX);
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
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToString(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, DECIMAL, DATE, DATETIME, TIME, QUANTITY ->
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
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToDate(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    if (sourceType == FhirPathType.STRING) {
      // String can be converted only if it matches the date format: YYYY or YYYY-MM or YYYY-MM-DD
      return value.rlike(DATE_REGEX);
    }
    // Other types cannot be converted.
    return lit(false);
  }

  /**
   * Validates if a value can be converted to DateTime.
   * <ul>
   *   <li>STRING: Check if matches datetime pattern (YYYY-MM-DDThh:mm:ss with optional timezone)</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToDateTime(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    if (sourceType == FhirPathType.STRING) {
      // String can be converted only if it matches ISO 8601 datetime format.
      // Supports partial precision: YYYY, YYYY-MM, YYYY-MM-DD, YYYY-MM-DDThh, etc.
      return value.rlike(DATETIME_REGEX);
    }
    // Other types cannot be converted.
    return lit(false);
  }

  /**
   * Validates if a value can be converted to Time.
   * <ul>
   *   <li>STRING: Check if matches time pattern (hh:mm:ss with optional milliseconds)</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToTime(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    if (sourceType == FhirPathType.STRING) {
      // String can be converted only if it matches time format.
      // Supports partial precision: hh, hh:mm, hh:mm:ss, hh:mm:ss.fff
      return value.rlike(TIME_REGEX);
    }
    // Other types cannot be converted.
    return lit(false);
  }

  /**
   * Validates if a value can be converted to Quantity.
   * <ul>
   *   <li>BOOLEAN/INTEGER/DECIMAL: Always {@code true}</li>
   *   <li>STRING: Check if matches quantity pattern (value + unit)</li>
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return Column expression evaluating to {@code true} if convertible, {@code false} otherwise
   */
  Column validateConversionToQuantity(@Nonnull final FhirPathType sourceType,
      @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, DECIMAL ->
        // Boolean, Integer, and Decimal can always be converted to Quantity
          lit(true);
      case STRING ->
        // String can be converted only if it matches the quantity format:
        // value (optional +/-) + optional whitespace + unit (quoted UCUM or bareword calendar)
          value.rlike(QUANTITY_REGEX);
      default ->
        // Other types cannot be converted.
          lit(false);
    };
  }
}
