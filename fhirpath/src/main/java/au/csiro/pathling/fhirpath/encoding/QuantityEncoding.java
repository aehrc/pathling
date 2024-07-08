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

package au.csiro.pathling.fhirpath.encoding;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.encoders.QuantitySupport;
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.fhirpath.CalendarDurationUtils;
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.sql.types.FlexiDecimalSupport;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;

/**
 * Object decoders/encoders for {@link Quantity}.
 *
 * @author Piotr Szul
 */
public final class QuantityEncoding {

  private QuantityEncoding() {
    // Utility class
  }

  private static final Map<String, String> CALENDAR_DURATION_TO_UCUM = new ImmutableMap.Builder<String, String>()
      .put("second", "s")
      .put("seconds", "s")
      .put("millisecond", "ms")
      .put("milliseconds", "ms")
      .build();

  public static final String CANONICALIZED_VALUE_COLUMN = QuantitySupport
      .VALUE_CANONICALIZED_FIELD_NAME();
  public static final String CANONICALIZED_CODE_COLUMN = QuantitySupport
      .CODE_CANONICALIZED_FIELD_NAME();


  /**
   * Encodes a Quantity to a Row (spark SQL compatible type)
   *
   * @param quantity a coding to encode
   * @param includeScale whether the scale of the value should be encoded (or set to null)
   * @return the Row representation of the quantity
   */
  @Nullable
  public static Row encode(@Nullable final Quantity quantity, final boolean includeScale) {
    if (quantity == null) {
      return null;
    }
    final BigDecimal value = quantity.getValue();
    @Nullable final String code = quantity.getCode();
    final BigDecimal canonicalizedValue;
    final String canonicalizedCode;
    if (quantity.getSystem().equals(Ucum.SYSTEM_URI)) {
      canonicalizedValue = Ucum.getCanonicalValue(value, code);
      canonicalizedCode = Ucum.getCanonicalCode(value, code);
    } else {
      canonicalizedValue = null;
      canonicalizedCode = null;
    }
    final String comparator = Optional.ofNullable(quantity.getComparator())
        .map(QuantityComparator::toCode).orElse(null);
    return RowFactory.create(quantity.getId(),
        quantity.getValue(),
        // We cannot encode the scale of the results of arithmetic operations.
        includeScale
        ? quantity.getValue().scale()
        : null,
        comparator,
        quantity.getUnit(), quantity.getSystem(), quantity.getCode(),
        FlexiDecimal.toValue(canonicalizedValue),
        canonicalizedCode, null /* _fid */);
  }

  /**
   * Encodes a Quantity to a Row (spark SQL compatible type)
   *
   * @param quantity a coding to encode
   * @return the Row representation of the quantity
   */
  @Nullable
  public static Row encode(@Nullable final Quantity quantity) {
    return encode(quantity, true);
  }

  /**
   * Decodes a Quantity from a Row.
   *
   * @param row the row to decode
   * @return the resulting Quantity
   */
  @Nonnull
  public static Quantity decode(@Nonnull final Row row) {
    final Quantity quantity = new Quantity();

    Optional.ofNullable(row.getString(0)).ifPresent(quantity::setId);

    // The value gets converted to a BigDecimal, taking into account the scale that has been encoded 
    // alongside it.
    final int scale = row.getInt(2);
    final BigDecimal value = Optional.ofNullable(row.getDecimal(1))
        .map(bd -> bd.scale() > DecimalCustomCoder.scale()
                   ? bd.setScale(scale, RoundingMode.HALF_UP)
                   : bd)
        .orElse(null);
    quantity.setValue(value);

    // The comparator is encoded as a string code, we need to convert it back to an enum.
    Optional.ofNullable(row.getString(3))
        .map(QuantityComparator::fromCode)
        .ifPresent(quantity::setComparator);

    Optional.ofNullable(row.getString(4)).ifPresent(quantity::setUnit);
    Optional.ofNullable(row.getString(5)).ifPresent(quantity::setSystem);
    Optional.ofNullable(row.getString(6)).ifPresent(quantity::setCode);

    return quantity;
  }


  /**
   * @return A {@link StructType} for a Quantity
   */
  @Nonnull
  public static StructType dataType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField("id", DataTypes.StringType, true, metadata);
    final StructField value = new StructField("value", DataTypes.createDecimalType(
        DecimalCustomCoder.precision(), DecimalCustomCoder.scale()), true, metadata);
    final StructField valueScale = new StructField("value_scale", DataTypes.IntegerType, true,
        metadata);
    final StructField comparator = new StructField("comparator", DataTypes.StringType, true,
        metadata);
    final StructField unit = new StructField("unit", DataTypes.StringType, true, metadata);
    final StructField system = new StructField("system", DataTypes.StringType, true, metadata);
    final StructField code = new StructField("code", DataTypes.StringType, true, metadata);
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
  public static Column toStruct(
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
    return struct(
        id.as("id"),
        value.cast(DecimalCustomCoder.decimalType()).as("value"),
        value_scale.as("value_scale"),
        comparator.as("comparator"),
        unit.as("unit"),
        system.as("system"),
        code.as("code"),
        canonicalizedValue.as(CANONICALIZED_VALUE_COLUMN),
        canonicalizedCode.as(CANONICALIZED_CODE_COLUMN),
        _fid.as("_fid")
    );
  }

  /**
   * Encodes the quantity as a literal column that includes appropriate canonicalization.
   *
   * @param quantity the quantity to encode.
   * @return the column with the literal representation of the quantity.
   */
  @Nonnull
  public static Column encodeLiteral(@Nonnull final Quantity quantity) {
    final Optional<QuantityComparator> comparator = Optional.ofNullable(quantity.getComparator());
    final BigDecimal value = quantity.getValue();

    final BigDecimal canonicalizedValue;
    final String canonicalizedCode;
    if (quantity.getSystem().equals(Ucum.SYSTEM_URI)) {
      // If it is a UCUM Quantity, use the UCUM library to canonicalize the value and code.
      canonicalizedValue = Ucum.getCanonicalValue(value, quantity.getCode());
      canonicalizedCode = Ucum.getCanonicalCode(value, quantity.getCode());
    } else if (CalendarDurationUtils.isCalendarDuration(quantity) &&
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
        lit(quantity.getId()),
        lit(value),
        lit(value.scale()),
        lit(comparator.map(QuantityComparator::toCode).orElse(null)),
        lit(quantity.getUnit()),
        lit(quantity.getSystem()),
        lit(quantity.getCode()),
        FlexiDecimalSupport.toLiteral(canonicalizedValue),
        lit(canonicalizedCode),
        lit(null));
  }

}
