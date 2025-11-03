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

package au.csiro.pathling.fhirpath.encoding;

import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.QuantitySupport;
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.fhirpath.unit.CalendarDurationUnit;
import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.sql.types.FlexiDecimalSupport;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Object encoders/decoders for FHIRPath Quantity values.
 * <p>
 * Provides conversion between {@link FhirPathQuantity} objects and Spark SQL Row representation.
 *
 * @author Piotr Szul
 */
@Value(staticConstructor = "of")
public class QuantityEncoding {

  public static final String VALUE_COLUMN = "value";
  public static final String SYSTEM_COLUMN = "system";
  public static final String CODE_COLUMN = "code";
  public static final String UNIT_COLUMN = "unit";
  @Nonnull
  Column id;
  @Nonnull
  Column value;
  @Nonnull
  Column value_scale;
  @Nonnull
  Column comparator;
  @Nonnull
  Column unit;
  @Nonnull
  Column system;
  @Nonnull
  Column code;
  @Nonnull
  Column canonicalizedValue;
  @Nonnull
  Column canonicalizedCode;
  @Nonnull
  Column _fid;

  private static final Map<String, String> CALENDAR_DURATION_TO_UCUM = Stream.of(
          CalendarDurationUnit.values())
      .filter(CalendarDurationUnit::isDefinite)
      .collect(
          toUnmodifiableMap(CalendarDurationUnit::code,
              CalendarDurationUnit::getUcumEquivalent));

  public static final String CANONICALIZED_VALUE_COLUMN = QuantitySupport
      .VALUE_CANONICALIZED_FIELD_NAME();
  public static final String CANONICALIZED_CODE_COLUMN = QuantitySupport
      .CODE_CANONICALIZED_FIELD_NAME();

  /**
   * Converts this quantity to a struct column.
   *
   * @return the struct column representing the quantity.
   */
  @Nonnull
  public Column toStruct() {
    return struct(
        id.as("id"),
        value.cast(DecimalCustomCoder.decimalType()).as(VALUE_COLUMN),
        value_scale.as("value_scale"),
        comparator.as("comparator"),
        unit.as(UNIT_COLUMN),
        system.as(SYSTEM_COLUMN),
        code.as(CODE_COLUMN),
        canonicalizedValue.as(CANONICALIZED_VALUE_COLUMN),
        canonicalizedCode.as(CANONICALIZED_CODE_COLUMN),
        _fid.as("_fid")
    );
  }

  /**
   * Decodes a FhirPathQuantity from a Row.
   *
   * @param row the row to decode
   * @return the resulting FhirPathQuantity
   */
  @Nullable
  public static FhirPathQuantity decode(@Nullable final Row row) {
    if (row == null) {
      return null;
    }
    // Extract value with scale handling
    @Nullable final Integer scale = !row.isNullAt(2)
                                    ? row.getInt(2)
                                    : null;
    @Nullable final BigDecimal value = Optional.ofNullable(row.getDecimal(1))
        .map(bd -> nonNull(scale) && bd.scale() > scale
                   ? bd.setScale(scale, RoundingMode.HALF_UP)
                   : bd)
        .orElse(null);

    @Nullable final String unit = row.getString(4);
    @Nullable final String system = row.getString(5);
    @Nullable final String code = row.getString(6);
    return FhirPathQuantity.of(value, system, code, unit);
  }

  /**
   * @return A {@link StructType} for a Quantity
   */
  @Nonnull
  public static StructType dataType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField("id", DataTypes.StringType, true, metadata);
    final StructField value = new StructField(VALUE_COLUMN, DataTypes.createDecimalType(
        DecimalCustomCoder.precision(), DecimalCustomCoder.scale()), true, metadata);
    final StructField valueScale = new StructField("value_scale", DataTypes.IntegerType, true,
        metadata);
    final StructField comparator = new StructField("comparator", DataTypes.StringType, true,
        metadata);
    final StructField unit = new StructField(UNIT_COLUMN, DataTypes.StringType, true, metadata);
    final StructField system = new StructField(SYSTEM_COLUMN, DataTypes.StringType, true, metadata);
    final StructField code = new StructField(CODE_COLUMN, DataTypes.StringType, true, metadata);
    final StructField canonicalizedValue = new StructField(CANONICALIZED_VALUE_COLUMN,
        FlexiDecimal.DATA_TYPE, true, metadata);
    final StructField canonicalizedCode = new StructField(CANONICALIZED_CODE_COLUMN,
        DataTypes.StringType, true, metadata);
    final StructField fid = new StructField("_fid", DataTypes.IntegerType, true,
        metadata);
    return new StructType(
        new StructField[]{id, value, valueScale, comparator, unit, system, code, canonicalizedValue,
            canonicalizedCode, fid});
  }

  /**
   * Creates the structure representing the quantity column from its fields.
   *
   * @param id the id column
   * @param value the value column
   * @param value_scale the scale of the value column
   * @param comparator the comparator column
   * @param unit the unit column
   * @param system the system column
   * @param code the code column
   * @param canonicalizedValue the canonicalized value column
   * @param canonicalizedCode the canonicalized code column
   * @param _fid the _fid column
   * @return the SQL struct for the Quantity type.
   */
  @Nonnull
  private static Column toStruct(
      @Nonnull final Column id,
      @Nonnull final Column value,
      @Nonnull final Column value_scale,
      @Nonnull final Column comparator,
      @Nonnull final Column unit,
      @Nonnull final Column system,
      @Nonnull final Column code,
      @Nonnull final Column canonicalizedValue,
      @Nonnull final Column canonicalizedCode,
      @Nonnull final Column _fid
  ) {
    return QuantityEncoding.of(
            id, value, value_scale, comparator, unit, system, code, canonicalizedValue,
            canonicalizedCode, _fid)
        .toStruct();
  }

  /**
   * Encodes the quantity as a literal column that includes appropriate canonicalization.
   *
   * @param quantity the quantity to encode.
   * @return the column with the literal representation of the quantity.
   */
  @Nonnull
  public static Column encodeLiteral(@Nonnull final FhirPathQuantity quantity) {
    final BigDecimal value = quantity.getValue();
    final BigDecimal canonicalizedValue;
    final String canonicalizedCode;
    if (quantity.isUcum()) {
      // If it is a UCUM Quantity, use the UCUM library to canonicalize the value and code.
      canonicalizedValue = Ucum.getCanonicalValue(value, quantity.getCode());
      canonicalizedCode = Ucum.getCanonicalCode(value, quantity.getCode());
    } else if (quantity.isCalendarDuration() &&
        CALENDAR_DURATION_TO_UCUM.containsKey(quantity.getCode())) {
      // If it is a (supported) calendar duration, get the corresponding UCUM unit and then use the
      // UCUM library to canonicalize the value and code.
      final String resolvedCode = CALENDAR_DURATION_TO_UCUM.get(quantity.getCode());
      canonicalizedValue = Ucum.getCanonicalValue(value, resolvedCode);
      canonicalizedCode = Ucum.getCanonicalCode(value, resolvedCode);
    } else {
      // If it is neither a UCUM Quantity nor a calendar duration, it will not have a canonicalized
      // form available.
      canonicalizedValue = null;
      canonicalizedCode = null;
    }

    return toStruct(
        lit(null),
        lit(value),
        lit(value.scale()),
        lit(null),
        lit(quantity.getUnitName()),
        lit(quantity.getSystem()),
        lit(quantity.getCode()),
        FlexiDecimalSupport.toLiteral(canonicalizedValue),
        lit(canonicalizedCode),
        lit(null));
  }

  /**
   * Encodes a numeric column as a quantity with unit "1" in the UCUM system. Returns a fully null
   * struct when the input is null to ensure FHIRPath empty collection semantics.
   *
   * @param numericColumn the numeric column to encode
   * @return the column with the representation of the quantity, or fully null struct if input is
   * null
   */
  @Nonnull
  public static Column encodeNumeric(@Nonnull final Column numericColumn) {
    // Cast value to decimal type
    final Column decimalValue = numericColumn.cast(DecimalCustomCoder.decimalType());

    // Return fully null struct when value is null to maintain FHIRPath empty collection semantics
    return when(decimalValue.isNotNull(),
        toStruct(
            lit(null),
            decimalValue,
            // We cannot encode the scale of the results of arithmetic operations.
            lit(null),
            lit(null),
            lit("1"),
            lit(UcumUnit.UCUM_SYSTEM_URI),
            lit("1"),
            // we do not need to normalize this as the unit is always "1"
            // so it will be comparable with other quantities with unit "1"
            lit(null),
            lit(null),
            lit(null)))
        .otherwise(lit(null).cast(dataType()));
  }

  /**
   * Encodes a FhirPathQuantity to a Row representation.
   * <p>
   * This method converts a FhirPathQuantity into the Row representation used by Spark. It handles
   * canonicalization for both UCUM units and calendar duration units.
   *
   * @param quantity the FhirPathQuantity to encode
   * @return the Row representation of the quantity
   */
  @Nonnull
  public static Row encode(@Nonnull final FhirPathQuantity quantity) {
    final BigDecimal value = quantity.getValue();
    final BigDecimal canonicalizedValue;
    final String canonicalizedCode;

    if (quantity.isUcum()) {
      // If it is a UCUM Quantity, use the UCUM library to canonicalize the value and code.
      canonicalizedValue = Ucum.getCanonicalValue(value, quantity.getCode());
      canonicalizedCode = Ucum.getCanonicalCode(value, quantity.getCode());
    } else if (quantity.isCalendarDuration() &&
        CALENDAR_DURATION_TO_UCUM.containsKey(quantity.getCode())) {
      // If it is a (supported) calendar duration, get the corresponding UCUM unit and then use
      // the UCUM library to canonicalize the value and code.
      final String resolvedCode = CALENDAR_DURATION_TO_UCUM.get(quantity.getCode());
      canonicalizedValue = Ucum.getCanonicalValue(value, resolvedCode);
      canonicalizedCode = Ucum.getCanonicalCode(value, resolvedCode);
    } else {
      // If it is neither a UCUM Quantity nor a calendar duration, it will not have a
      // canonicalized form available.
      canonicalizedValue = null;
      canonicalizedCode = null;
    }

    // Create the Quantity Row with all fields:
    // id, value, value_scale, comparator, unit, system, code,
    // canonicalized_value, canonicalized_code, _fid
    return RowFactory.create(
        null,                                    // id
        value,                                   // value
        value.scale(),                           // value_scale
        null,                                    // comparator
        quantity.getUnitName(),                      // unit
        quantity.getSystem(),                    // system
        quantity.getCode(),                      // code
        FlexiDecimal.toValue(canonicalizedValue), // canonicalized_value (as FlexiDecimal Row)
        canonicalizedCode,                       // canonicalized_code
        null                                     // _fid
    );
  }
}
