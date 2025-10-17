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

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
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
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Contains functions for converting values between types.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
 * Conversion</a>
 */
@SuppressWarnings("unused")
public class ConversionFunctions {

  private ConversionFunctions() {
  }

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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already boolean, return as-is.
    if (inputType == FhirPathType.BOOLEAN) {
      return singular;
    }

    final ColumnRepresentation column = singular.getColumn();
    final Column value = column.getValue();

    final Column result = switch (inputType) {
      case STRING ->
        // String: Handle '1.0' and '0.0' specially, use SparkSQL cast for other values.
        // SparkSQL cast handles 'true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0' (case-insensitive).
          when(value.equalTo(lit("1.0")), lit(true))
              .when(value.equalTo(lit("0.0")), lit(false))
              .otherwise(value.cast(DataTypes.BooleanType));
      case INTEGER ->
        // Integer: cast to boolean (1 → true, 0 → false, otherwise null).
          value.cast(DataTypes.BooleanType);
      case DECIMAL ->
        // Decimal: cast to boolean (1.0 → true, 0.0 → false, otherwise null).
          value.cast(DataTypes.BooleanType);
      default ->
        // Unsupported type: return empty.
          null;
    };

    return result != null
           ? BooleanCollection.build(new DefaultRepresentation(result))
           : EmptyCollection.getInstance();
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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already integer, return as-is.
    if (inputType == FhirPathType.INTEGER) {
      return singular;
    }

    final ColumnRepresentation column = singular.getColumn();
    final Column value = column.getValue();

    final Column result = switch (inputType) {
      case BOOLEAN ->
        // Boolean: Use SparkSQL cast (true → 1, false → 0).
          value.cast(DataTypes.IntegerType);
      case STRING ->
        // String: try to cast to integer, returns null if invalid.
          value.cast(DataTypes.IntegerType);
      default -> null;
    };

    return result != null
           ? IntegerCollection.build(new DefaultRepresentation(result))
           : EmptyCollection.getInstance();
  }

  /**
   * Converts the input to a Long value. Similar to toInteger but for Long values.
   *
   * @param input The input collection
   * @return An {@link IntegerCollection} containing the converted value or empty
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * toLong</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection toLong(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already integer (we use integer for long values), return as-is.
    if (inputType == FhirPathType.INTEGER) {
      return singular;
    }

    final ColumnRepresentation column = singular.getColumn();
    final Column value = column.getValue();

    final Column result = switch (inputType) {
      case BOOLEAN ->
        // Boolean: true → 1, false → 0.
          when(value.equalTo(lit(true)), lit(1L))
              .when(value.equalTo(lit(false)), lit(0L));
      case STRING ->
        // String: try to cast to long, returns null if invalid.
          value.cast(DataTypes.LongType);
      case DECIMAL ->
        // Decimal: cast to long.
          value.cast(DataTypes.LongType);
      default -> null;
    };

    // Note: We return IntegerCollection as Pathling uses Integer for both int and long.
    return result != null
           ? IntegerCollection.build(new DefaultRepresentation(result.cast(DataTypes.IntegerType)))
           : EmptyCollection.getInstance();
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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already decimal, return as-is.
    if (inputType == FhirPathType.DECIMAL) {
      return singular;
    }

    final ColumnRepresentation column = singular.getColumn();
    final Column value = column.getValue();

    final Column result = switch (inputType) {
      case BOOLEAN ->
        // Boolean: Use SparkSQL cast (true → 1.0, false → 0.0).
          value.cast(DecimalCollection.getDecimalType());
      case INTEGER ->
        // Integer: cast to decimal.
          value.cast(DecimalCollection.getDecimalType());
      case STRING ->
        // String: try to cast to decimal, returns null if invalid.
          value.cast(DecimalCollection.getDecimalType());
      default -> null;
    };

    return result != null
           ? DecimalCollection.build(new DefaultRepresentation(result))
           : EmptyCollection.getInstance();
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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already string, return as-is.
    if (inputType == FhirPathType.STRING) {
      return singular;
    }

    // All primitive types can be cast to string.
    return switch (inputType) {
      case BOOLEAN, INTEGER, DECIMAL, DATE, DATETIME, TIME -> {
        final Column result = singular.getColumn().getValue().cast(DataTypes.StringType);
        yield StringCollection.build(new DefaultRepresentation(result));
      }
      default -> EmptyCollection.getInstance();
    };
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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already date, return as-is.
    if (inputType == FhirPathType.DATE) {
      return singular;
    }

    // Only string can be converted to date.
    return switch (inputType) {
      case STRING ->
        // Date values are stored as strings in FHIR, validation happens at parse time.
          DateCollection.build(singular.getColumn(), Optional.empty());
      default -> EmptyCollection.getInstance();
    };
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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already datetime, return as-is.
    if (inputType == FhirPathType.DATETIME) {
      return singular;
    }

    // Only string can be converted to datetime.
    return switch (inputType) {
      case STRING ->
        // DateTime values are stored as strings in FHIR, validation happens at parse time.
          DateTimeCollection.build(singular.getColumn(), Optional.empty());
      default -> EmptyCollection.getInstance();
    };
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
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // If already time, return as-is.
    if (inputType == FhirPathType.TIME) {
      return singular;
    }

    // Only string can be converted to time.
    return switch (inputType) {
      case STRING ->
        // Time values are stored as strings in FHIR, validation happens at parse time.
          TimeCollection.build(singular.getColumn(), Optional.empty());
      default -> EmptyCollection.getInstance();
    };
  }

  /**
   * Returns true if the input can be converted to a Boolean value. Per FHIRPath specification:
   * - Boolean: always convertible
   * - String: 'true', 't', 'yes', 'y', '1', '1.0', 'false', 'f', 'no', 'n', '0', '0.0' (case-insensitive)
   * - Integer: 0 or 1
   * - Decimal: 0.0 or 1.0
   * - Empty collection: returns empty
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToBoolean</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToBoolean(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // Boolean is already boolean.
    if (inputType == FhirPathType.BOOLEAN) {
      return BooleanCollection.build(new DefaultRepresentation(lit(true)));
    }

    final Column value = singular.getColumn().getValue();

    final Column canConvert = switch (inputType) {
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

    return BooleanCollection.build(new DefaultRepresentation(canConvert));
  }

  /**
   * Returns true if the input can be converted to an Integer value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToInteger</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToInteger(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    return switch (inputType) {
      case BOOLEAN, INTEGER ->
        // Boolean and integer can be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      case STRING -> {
        // Check if value is not null and casting to integer returns non-null.
        final Column value = singular.getColumn().getValue();
        final Column canConvert = value.isNotNull().and(value.cast(DataTypes.IntegerType).isNotNull());
        yield BooleanCollection.build(new DefaultRepresentation(canConvert));
      }
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }

  /**
   * Returns true if the input can be converted to a Long value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToLong</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToLong(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    return switch (inputType) {
      case BOOLEAN, INTEGER, DECIMAL ->
        // Boolean, integer, and decimal can be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      case STRING -> {
        // Check if value is not null and casting to long returns non-null.
        final Column value = singular.getColumn().getValue();
        final Column canConvert = value.isNotNull().and(value.cast(DataTypes.LongType).isNotNull());
        yield BooleanCollection.build(new DefaultRepresentation(canConvert));
      }
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }

  /**
   * Returns true if the input can be converted to a Decimal value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDecimal</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToDecimal(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    return switch (inputType) {
      case BOOLEAN, INTEGER, DECIMAL ->
        // Boolean, integer, and decimal can be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      case STRING -> {
        // Check if value is not null and casting to decimal returns non-null.
        final Column value = singular.getColumn().getValue();
        final Column canConvert = value.isNotNull().and(value.cast(DecimalCollection.getDecimalType()).isNotNull());
        yield BooleanCollection.build(new DefaultRepresentation(canConvert));
      }
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }

  /**
   * Returns true if the input can be converted to a String value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToString</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToString(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    // All primitive types can be converted to string.
    return switch (inputType) {
      case STRING, BOOLEAN, INTEGER, DECIMAL, DATE, DATETIME, TIME ->
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }

  /**
   * Returns true if the input can be converted to a Date value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDate</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToDate(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    return switch (inputType) {
      case DATE ->
        // Date is already date.
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      case STRING -> {
        // String can be converted only if it matches the date format: YYYY-MM-DD (with optional partial dates).
        final Column value = singular.getColumn().getValue();
        // Date format: YYYY or YYYY-MM or YYYY-MM-DD
        final Column canConvert = value.isNotNull().and(
            value.rlike("^\\d{4}(-\\d{2}(-\\d{2})?)?$")
        );
        yield BooleanCollection.build(new DefaultRepresentation(canConvert));
      }
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }

  /**
   * Returns true if the input can be converted to a DateTime value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToDateTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToDateTime(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    return switch (inputType) {
      case DATETIME ->
        // DateTime is already datetime.
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      case STRING -> {
        // String can be converted only if it matches ISO 8601 datetime format.
        final Column value = singular.getColumn().getValue();
        // Simplified check for datetime format: YYYY-MM-DDThh:mm:ss with optional timezone
        final Column canConvert = value.isNotNull().and(
            value.rlike("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})?$")
        );
        yield BooleanCollection.build(new DefaultRepresentation(canConvert));
      }
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }

  /**
   * Returns true if the input can be converted to a Time value.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing true if convertible, false otherwise, or empty for empty input
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
   * convertsToTime</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection convertsToTime(@Nonnull final Collection input) {
    if (input instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final Collection singular = input.asSingular();
    final FhirPathType inputType = singular.getType().orElse(null);

    return switch (inputType) {
      case TIME ->
        // Time is already time.
          BooleanCollection.build(new DefaultRepresentation(lit(true)));
      case STRING -> {
        // String can be converted only if it matches time format: hh:mm:ss with optional milliseconds.
        final Column value = singular.getColumn().getValue();
        // Time format: hh:mm:ss or hh:mm:ss.fff
        final Column canConvert = value.isNotNull().and(
            value.rlike("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$")
        );
        yield BooleanCollection.build(new DefaultRepresentation(canConvert));
      }
      default ->
        // Other types cannot be converted.
          BooleanCollection.build(new DefaultRepresentation(lit(false)));
    };
  }
}
