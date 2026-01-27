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

package au.csiro.pathling.fhirpath.function.provider;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.misc.DecimalToLiteral;
import au.csiro.pathling.sql.misc.QuantityToLiteral;
import au.csiro.pathling.sql.misc.StringToQuantity;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Package-private utility class containing type conversion orchestration and logic.
 *
 * <p>This class provides the template method for performing type conversions and all type-specific
 * conversion helper methods used by {@link ConversionFunctions}.
 *
 * @author Piotr Szul
 */
@UtilityClass
class ConversionLogic {

  /**
   * Regex pattern for validating FHIRPath integer strings.
   *
   * <p>Matches: Optional sign (+ or -) followed by one or more digits.
   *
   * <p>Examples: "123", "+456", "-789", "0"
   *
   * <p>Pattern: ^(\+|-)?\d+$
   */
  static final String INTEGER_REGEX = "^(\\+|-)?\\d+$";

  /**
   * Regex pattern for validating FHIR date strings with partial precision support.
   *
   * <p>Matches three valid formats:
   *
   * <ul>
   *   <li>YYYY - Year only (e.g., "2023")
   *   <li>YYYY-MM - Year and month (e.g., "2023-06")
   *   <li>YYYY-MM-DD - Full date (e.g., "2023-06-15")
   * </ul>
   *
   * <p>Pattern: ^\d{4}(-\d{2}(-\d{2})?)?$
   */
  static final String DATE_REGEX = "^\\d{4}(-\\d{2}(-\\d{2})?)?$";

  /**
   * Regex pattern for validating FHIR dateTime strings with partial precision and timezone support.
   *
   * <p>Matches progressively more precise formats:
   *
   * <ul>
   *   <li>YYYY - Year (e.g., "2023")
   *   <li>YYYY-MM - Year and month (e.g., "2023-06")
   *   <li>YYYY-MM-DD - Date (e.g., "2023-06-15")
   *   <li>YYYY-MM-DDThh - Date with hour (e.g., "2023-06-15T14")
   *   <li>YYYY-MM-DDThh:mm - Date with hour and minute (e.g., "2023-06-15T14:30")
   *   <li>YYYY-MM-DDThh:mm:ss - Date with seconds (e.g., "2023-06-15T14:30:45")
   *   <li>YYYY-MM-DDThh:mm:ss.fff - Date with fractional seconds (e.g., "2023-06-15T14:30:45.123")
   * </ul>
   *
   * <p>Optional timezone: Z for UTC or ±hh:mm offset (e.g., "2023-06-15T14:30:45+10:00")
   *
   * <p>Pattern: ^\d{4}(-\d{2}(-\d{2}(T\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+\-]\d{2}:\d{2})?)?)?)?$
   */
  static final String DATETIME_REGEX =
      "^\\d{4}(-\\d{2}(-\\d{2}(T\\d{2}(:\\d{2}(:\\d{2}(\\.\\d+)?)?)?(Z|[+\\-]\\d{2}:\\d{2})?)?)?)?$";

  /**
   * Regex pattern for validating FHIR time strings with partial precision support.
   *
   * <p>Matches three valid formats:
   *
   * <ul>
   *   <li>hh - Hour only (e.g., "14")
   *   <li>hh:mm - Hour and minute (e.g., "14:30")
   *   <li>hh:mm:ss - Hour, minute, and second (e.g., "14:30:45")
   *   <li>hh:mm:ss.fff - Hour, minute, second with fractional seconds (e.g., "14:30:45.123")
   * </ul>
   *
   * <p>Pattern: ^\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?$
   */
  static final String TIME_REGEX = "^\\d{2}(:\\d{2}(:\\d{2}(\\.\\d+)?)?)?$";

  /**
   * Regex pattern for validating FHIRPath quantity literal strings.
   *
   * <p>Matches a numeric value with optional unit specification:
   *
   * <ul>
   *   <li>Numeric part: Optional sign (+ or -), integer or decimal (e.g., "1", "-2.5", "+3.14")
   *   <li>Optional whitespace separator
   *   <li>Optional unit: Either quoted UCUM code (e.g., "'mg'") or calendar duration unit
   * </ul>
   *
   * <p>Valid calendar duration units (case-insensitive, singular or plural): year, month, week,
   * day, hour, minute, second, millisecond
   *
   * <p>Examples: "5", "1.5 'kg'", "3 weeks", "10.5 mg", "-2.5 'cm'"
   *
   * <p>Note: Unit is optional per FHIRPath spec. When absent, unitCode defaults to '1'.
   *
   * <p>Pattern:
   * ^[+-]?\d+(?:\.\d+)?\s*(?:'[^']+'|(?i:years?|months?|weeks?|days?|hours?|minutes?|seconds?|milliseconds?))?$
   */
  static final String QUANTITY_REGEX =
      "^[+-]?\\d+(?:\\.\\d+)?\\s*(?:'[^']+'|(?i:years?|months?|weeks?|days?|hours?|minutes?|seconds?|milliseconds?))?$";

  // Registry mapping target types to their conversion functions
  private static final Map<FhirPathType, BiFunction<FhirPathType, Column, Column>>
      CONVERSION_REGISTRY =
          Map.ofEntries(
              Map.entry(FhirPathType.BOOLEAN, ConversionLogic::convertToBoolean),
              Map.entry(FhirPathType.INTEGER, ConversionLogic::convertToInteger),
              Map.entry(FhirPathType.DECIMAL, ConversionLogic::convertToDecimal),
              Map.entry(FhirPathType.STRING, ConversionLogic::convertToString),
              Map.entry(FhirPathType.DATE, ConversionLogic::convertToDate),
              Map.entry(FhirPathType.DATETIME, ConversionLogic::convertToDateTime),
              Map.entry(FhirPathType.TIME, ConversionLogic::convertToTime),
              Map.entry(FhirPathType.QUANTITY, ConversionLogic::convertToQuantity));

  // Registry mapping target types to their collection builders
  private static final Map<FhirPathType, Function<DefaultRepresentation, ? extends Collection>>
      BUILDER_REGISTRY =
          Map.ofEntries(
              Map.entry(FhirPathType.BOOLEAN, BooleanCollection::build),
              Map.entry(FhirPathType.INTEGER, IntegerCollection::build),
              Map.entry(FhirPathType.DECIMAL, DecimalCollection::build),
              Map.entry(FhirPathType.STRING, StringCollection::build),
              Map.entry(FhirPathType.DATE, repr -> DateCollection.build(repr, Optional.empty())),
              Map.entry(
                  FhirPathType.DATETIME, repr -> DateTimeCollection.build(repr, Optional.empty())),
              Map.entry(FhirPathType.TIME, repr -> TimeCollection.build(repr, Optional.empty())),
              Map.entry(FhirPathType.QUANTITY, QuantityCollection::build));

  /**
   * Template method for performing type conversions. Handles common orchestration: empty check,
   * singular check, identity conversion, delegation to conversion logic, and result building.
   *
   * <p>The conversion function and collection builder are automatically determined from the target
   * type using internal registries.
   *
   * @param input The input collection to convert
   * @param targetType The target FHIRPath type
   * @return The converted collection or EmptyCollection if conversion fails
   */
  Collection performConversion(
      @Nonnull final Collection input, @Nonnull final FhirPathType targetType) {

    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    // Look up conversion function and builder from registries
    final BiFunction<FhirPathType, Column, Column> conversionLogic =
        CONVERSION_REGISTRY.get(targetType);
    @SuppressWarnings("unchecked")
    final Function<DefaultRepresentation, Collection> collectionBuilder =
        (Function<DefaultRepresentation, Collection>) BUILDER_REGISTRY.get(targetType);

    final Column singularValue = input.getColumn().singular().getValue();
    // Use Nothing when the type is not known to enforce default value for a non-convertible type
    final FhirPathType sourceType = input.getType().orElse(FhirPathType.NOTHING);
    final Column result =
        sourceType == targetType ? singularValue : conversionLogic.apply(sourceType, singularValue);

    return collectionBuilder.apply(
        new DefaultRepresentation(
            // this triggers singularity check if the result is null
            coalesce(result, input.getColumn().ensureSingular())
            // implicit null otherwise
            ));
  }

  /**
   * Converts a value to Boolean based on source type.
   *
   * <ul>
   *   <li>STRING: Special handling for "1.0"/"0.0", then cast
   *   <li>INTEGER: Only 0 or 1
   *   <li>DECIMAL: Only 0.0 or 1.0
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToBoolean(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    return switch (sourceType) {
      case STRING ->
          // String: Handle '1.0' and '0.0' specially, use SparkSQL cast for other values.
          // SparkSQL cast handles 'true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0'
          // (case-insensitive).
          when(value.equalTo(lit("1.0")), lit(true))
              .when(value.equalTo(lit("0.0")), lit(false))
              .otherwise(value.try_cast(DataTypes.BooleanType));
      case INTEGER ->
          // Integer: Only 0 or 1 can be converted (1 → true, 0 → false, otherwise null).
          when(value.equalTo(lit(1)), lit(true)).when(value.equalTo(lit(0)), lit(false));
      case DECIMAL ->
          // Decimal: Only 0.0 or 1.0 can be converted (1.0 → true, 0.0 → false, otherwise null).
          when(value.equalTo(lit(1.0)), lit(true)).when(value.equalTo(lit(0.0)), lit(false));
      default -> lit(null);
    };
  }

  /**
   * Converts a value to Integer based on source type.
   *
   * <ul>
   *   <li>BOOLEAN: Direct cast (true → 1, false → 0)
   *   <li>STRING: Validates integer format (regex: (\+|-)?\d+) then casts
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToInteger(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN ->
          // Boolean: Use SparkSQL cast (true → 1, false → 0).
          value.try_cast(DataTypes.IntegerType);
      case STRING ->
          // String: Only convert if it matches integer format (no decimal point).
          // Per FHIRPath spec, valid integer strings match: (\+|-)?\d+
          when(value.rlike(INTEGER_REGEX), value.try_cast(DataTypes.IntegerType));
      default -> lit(null);
    };
  }

  /**
   * Converts a value to Decimal based on source type.
   *
   * <ul>
   *   <li>BOOLEAN/INTEGER/STRING: Direct cast to Decimal
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToDecimal(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, STRING ->
          // Boolean/Integer/String: cast to decimal.
          value.try_cast(DecimalCollection.getDecimalType());
      default -> lit(null);
    };
  }

  /**
   * Converts a value to String based on source type.
   *
   * <ul>
   *   <li>BOOLEAN, INTEGER, DATE, DATETIME, TIME: Direct cast to String
   *   <li>DECIMAL: Use DecimalToLiteral UDF to format with trailing zeros removed
   *   <li>QUANTITY: Use QuantityToLiteral UDF to format as FHIRPath quantity literal
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToString(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN, INTEGER, DATE, DATETIME, TIME ->
          // Primitive types can be cast to string directly.
          value.try_cast(DataTypes.StringType);
      case DECIMAL ->
          // Decimal: Use DecimalToLiteral UDF to strip trailing zeros.
          // E.g., 101.990000 -> 101.99, 1.0 -> 1
          callUDF(DecimalToLiteral.FUNCTION_NAME, value, lit(null));
      case QUANTITY ->
          // Quantity: Use QuantityToLiteral UDF to format as FHIRPath quantity literal.
          // E.g., {value: 1, unitCode: "wk", system: "http://unitsofmeasure.org"} -> "1 'wk'"
          callUDF(QuantityToLiteral.FUNCTION_NAME, value);
      default -> lit(null);
    };
  }

  /**
   * Converts a value to Date based on source type.
   *
   * <ul>
   *   <li>STRING: Validates format (YYYY or YYYY-MM or YYYY-MM-DD) and returns the string value
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToDate(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    if (sourceType == FhirPathType.STRING) {
      // Date values are stored as strings in FHIR. Validate format before accepting.
      // Date format: YYYY or YYYY-MM or YYYY-MM-DD
      return when(value.rlike(DATE_REGEX), value);
    }
    return lit(null);
  }

  /**
   * Converts a value to DateTime based on source type.
   *
   * <ul>
   *   <li>STRING: Validates format (supports partial precision) and returns the string value
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToDateTime(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    if (sourceType == FhirPathType.STRING) {
      // DateTime values are stored as strings in FHIR. Validate using simplified pattern.
      // Supports partial precision: YYYY, YYYY-MM, YYYY-MM-DD, YYYY-MM-DDThh, etc.
      return when(value.rlike(DATETIME_REGEX), value);
    }
    return lit(null);
  }

  /**
   * Converts a value to Time based on source type.
   *
   * <ul>
   *   <li>STRING: Validates format (supports partial precision) and returns the string value
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToTime(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    if (sourceType == FhirPathType.STRING) {
      // Time values are stored as strings in FHIR. Validate using simplified pattern.
      // Supports partial precision: hh, hh:mm, hh:mm:ss, hh:mm:ss.fff
      return when(value.rlike(TIME_REGEX), value);
    }
    return lit(null);
  }

  /**
   * Converts a value to Quantity based on source type.
   *
   * <ul>
   *   <li>BOOLEAN: true → 1.0 '1', false → 0.0 '1' (null boolean → null)
   *   <li>INTEGER/DECIMAL: Encode as quantity with unitCode '1' (null → null)
   *   <li>STRING: Parse as FHIRPath quantity literal (validates format then calls StringToQuantity
   *       UDF)
   * </ul>
   *
   * @param sourceType The source FHIRPath type
   * @param value The source column value
   * @return The converted column
   */
  @Nonnull
  Column convertToQuantity(@Nonnull final FhirPathType sourceType, @Nonnull final Column value) {
    return switch (sourceType) {
      case BOOLEAN ->
          // Boolean: true → 1.0 '1', false → 0.0 '1', null → null
          // First cast to decimal, then encode as quantity with unitCode '1'
          // Use when() to return typed null for null values instead of empty struct
          QuantityEncoding.encodeNumeric(value);
      case INTEGER, DECIMAL ->
          // Integer/Decimal: Encode as quantity with default unitCode '1', null → null
          // Use when() to return typed null for null values instead of empty struct
          QuantityEncoding.encodeNumeric(value);
      case STRING ->
          // String: Parse as FHIRPath quantity literal using UDF
          // UDF returns null if string doesn't match quantity format
          callUDF(StringToQuantity.FUNCTION_NAME, value);
      default -> lit(null).try_cast(QuantityEncoding.dataType());
    };
  }
}
