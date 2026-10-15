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

package au.csiro.pathling.fhirpath.encoding;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.unit.CalendarDurationUnit;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.sql.misc.CanonicalQuantityCode;
import au.csiro.pathling.sql.misc.CanonicalQuantityValue;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * The encoding of a {@code System.Quantity}, the quantity the engine builds itself, such as a
 * literal, the result of a conversion or a stored quantity combined with one of these.
 *
 * <p>A System quantity has one fixed, complete shape, whatever produced it (FR-059): the elements
 * of a FHIR Quantity other than {@code extension}, in definition order, each of them present.
 *
 * <ul>
 *   <li>{@code id}: always null for a quantity the engine builds, and the stored id for a stored
 *       quantity it has taken as a System one;
 *   <li>{@code value}: the text of a decimal, as the Parquet on FHIR layout stores one, so that it
 *       keeps its own scale and every digit it was given;
 *   <li>{@code comparator}, {@code unit}, {@code system} and {@code code}: text.
 * </ul>
 *
 * <p>It carries nothing derived from these: no decimal value with a scale companion, no canonical
 * form and no field identifier. The decimal value and the canonical form are computed from the
 * value and the code where a quantity is computed with, by {@link #decodeValue(Column)}, {@link
 * #canonicalValue(Column)} and {@link #canonicalCode(Column)}, as they are for a stored quantity.
 *
 * @author Piotr Szul
 */
public final class QuantityEncoding {

  /** The name of the column containing the id of the quantity. */
  public static final String ID_COLUMN = "id";

  /** The name of the column containing the numeric value of the quantity, as text. */
  public static final String VALUE_COLUMN = "value";

  /** The name of the column containing the comparator of the quantity. */
  public static final String COMPARATOR_COLUMN = "comparator";

  /** The name of the column containing the human-readable unit string. */
  public static final String UNIT_COLUMN = "unit";

  /** The name of the column containing the code system URI of the quantity unit. */
  public static final String SYSTEM_COLUMN = "system";

  /** The name of the column containing the code representing the quantity unit. */
  public static final String CODE_COLUMN = "code";

  private static final StructType DATA_TYPE =
      new StructType(
          Stream.of(
                  ID_COLUMN,
                  VALUE_COLUMN,
                  COMPARATOR_COLUMN,
                  UNIT_COLUMN,
                  SYSTEM_COLUMN,
                  CODE_COLUMN)
              .map(name -> DataTypes.createStructField(name, DataTypes.StringType, true))
              .toArray(StructField[]::new));

  private static final int VALUE_INDEX = DATA_TYPE.fieldIndex(VALUE_COLUMN);
  private static final int UNIT_INDEX = DATA_TYPE.fieldIndex(UNIT_COLUMN);
  private static final int SYSTEM_INDEX = DATA_TYPE.fieldIndex(SYSTEM_COLUMN);
  private static final int CODE_INDEX = DATA_TYPE.fieldIndex(CODE_COLUMN);

  /** The UCUM equivalent of each definite calendar duration, by its code. */
  private static final Map<String, String> CALENDAR_DURATION_TO_UCUM =
      Stream.of(CalendarDurationUnit.values())
          .filter(CalendarDurationUnit::isDefinite)
          .collect(
              toUnmodifiableMap(
                  CalendarDurationUnit::code, CalendarDurationUnit::getUcumEquivalent));

  private QuantityEncoding() {}

  /**
   * Returns the fixed type of a System quantity.
   *
   * @return A {@link StructType} for a System quantity
   */
  @Nonnull
  public static StructType dataType() {
    return DATA_TYPE;
  }

  /** Creates the structure of a System quantity from its fields, typed as {@link #dataType()}. */
  @Nonnull
  private static Column toStruct(
      @Nonnull final Column id,
      @Nonnull final Column value,
      @Nonnull final Column comparator,
      @Nonnull final Column unit,
      @Nonnull final Column system,
      @Nonnull final Column code) {
    // The cast types every field set to lit(null), which would otherwise be Spark's NullType
    // (VOID). A VOID-typed field prevents the struct from being converted to VARIANT, which is
    // required by variantTransformTree in repeat() and repeatAll().
    return struct(
            id.as(ID_COLUMN),
            value.as(VALUE_COLUMN),
            comparator.as(COMPARATOR_COLUMN),
            unit.as(UNIT_COLUMN),
            system.as(SYSTEM_COLUMN),
            code.as(CODE_COLUMN))
        .cast(DATA_TYPE);
  }

  /**
   * Takes a stored quantity as the System quantity it is computed with (decision 82).
   *
   * <p>The stored quantity is the FHIR structure as the traversal expression yields it: its value
   * is the text of a decimal, and it carries no canonical form. Each field is read by name through
   * the tolerant traversal, so a field the schema does not carry is null. Its extensions are not
   * kept, because a System quantity has none.
   *
   * <p>A null quantity is null.
   *
   * @param stored the stored quantity
   * @return the quantity, in the fixed shape of a System quantity
   */
  @Nonnull
  public static Column fromStored(@Nonnull final Column stored) {
    // The stored quantity is bound once, because it is read once per field, and a traced value
    // must be evaluated once (#2594).
    return let(
        stored,
        s ->
            when(
                s.isNotNull(),
                toStruct(
                    ColumnFunctions.resolveStringOrNull(s, ID_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, VALUE_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, COMPARATOR_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, UNIT_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, SYSTEM_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, CODE_COLUMN))));
  }

  /**
   * Takes a System quantity as one the engine built itself: its value and unit, without the {@code
   * id} and {@code comparator} a stored quantity taken as a System one keeps.
   *
   * @param quantity a System quantity
   * @return the quantity, with a null id and comparator
   */
  @Nonnull
  public static Column withoutStoredElements(@Nonnull final Column quantity) {
    return toStruct(
        lit(null),
        quantity.getField(VALUE_COLUMN),
        lit(null),
        quantity.getField(UNIT_COLUMN),
        quantity.getField(SYSTEM_COLUMN),
        quantity.getField(CODE_COLUMN));
  }

  /**
   * Decodes the text of a quantity's value to {@code DECIMAL(32,6)}, as {@link
   * au.csiro.pathling.fhirpath.collection.DecimalCollection} decodes any decimal. The cast is safe,
   * so a value that is not a decimal, or does not fit, is null rather than raising under ANSI mode.
   *
   * @param value the text of the value
   * @return the value, as a decimal
   */
  @Nonnull
  public static Column decodeValue(@Nonnull final Column value) {
    return value.try_cast(DecimalCustomCoder.decimalType());
  }

  /**
   * Decodes the text of a quantity's value to a decimal with every digit it has. The value is kept
   * as text, not as {@code DECIMAL(32,6)}, so nothing is rounded here: conversion, the string form
   * and the canonical form all work from the same digits. Only {@link #decodeValue(Column)}, which
   * produces a Spark decimal, is bound to the scale of that type.
   *
   * @param text the text of the value
   * @return the value, or null where there is no text or it is not a decimal
   */
  @Nullable
  static BigDecimal decodeValue(@Nullable final String text) {
    if (text == null) {
      return null;
    }
    try {
      return new BigDecimal(text.trim());
    } catch (final NumberFormatException e) {
      return null;
    }
  }

  /**
   * Computes the canonical value of a System quantity, as a flexible decimal, from its value and
   * code (FR-022). The value is taken as its text, so that no digit is lost before
   * canonicalisation.
   *
   * @param quantity a System quantity
   * @return the canonical value, or null where the quantity has no canonical form
   * @see #canonicalCode(Column)
   */
  @Nonnull
  public static Column canonicalValue(@Nonnull final Column quantity) {
    return callUDF(
        CanonicalQuantityValue.FUNCTION_NAME,
        quantity.getField(VALUE_COLUMN),
        ucumCodeOf(quantity));
  }

  /**
   * Computes the canonical unit code of a System quantity from its value and code (FR-022).
   *
   * @param quantity a System quantity
   * @return the canonical unit code, or null where the quantity has no canonical form
   * @see #canonicalValue(Column)
   */
  @Nonnull
  public static Column canonicalCode(@Nonnull final Column quantity) {
    return callUDF(
        CanonicalQuantityCode.FUNCTION_NAME, quantity.getField(VALUE_COLUMN), ucumCodeOf(quantity));
  }

  /**
   * Returns the UCUM code a quantity is canonicalised from. A calendar duration is canonicalised
   * from the UCUM equivalent of its unit where it has one, which a definite duration does, as
   * {@link FhirPathQuantity#asCanonical()} does for a literal. Every other quantity is
   * canonicalised from its code, whatever its system (decision 77).
   */
  @Nonnull
  private static Column ucumCodeOf(@Nonnull final Column quantity) {
    return let(
        quantity.getField(SYSTEM_COLUMN),
        quantity.getField(CODE_COLUMN),
        (system, code) ->
            when(
                    system.equalTo(lit(CalendarDurationUnit.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI)),
                    ucumEquivalentOf(code))
                .otherwise(code));
  }

  /**
   * Returns the UCUM equivalent of the code of a definite calendar duration, and null otherwise.
   */
  @Nonnull
  private static Column ucumEquivalentOf(@Nonnull final Column code) {
    return CALENDAR_DURATION_TO_UCUM.entrySet().stream()
        .reduce(
            lit(null).cast(DataTypes.StringType),
            (otherwise, entry) ->
                when(code.equalTo(lit(entry.getKey())), lit(entry.getValue())).otherwise(otherwise),
            (first, second) -> first);
  }

  /**
   * Decodes a FhirPathQuantity from a Row in the fixed shape of a System quantity.
   *
   * <p>The fields are read by position, so a row that carries a schema must have the fixed shape. A
   * row without a schema, such as one built by {@link #encode(FhirPathQuantity)}, is taken to.
   *
   * @param row the row to decode
   * @return the resulting FhirPathQuantity, or null where the row is null or is not a quantity
   *     FHIRPath can represent
   * @throws IllegalArgumentException if the row has a schema that is not the fixed shape
   */
  @Nullable
  public static FhirPathQuantity decode(@Nullable final Row row) {
    if (row == null) {
      return null;
    }
    final StructType schema = row.schema();
    if (schema != null && !hasQuantityShape(schema)) {
      throw new IllegalArgumentException(
          "Expected a System quantity of type "
              + DATA_TYPE.simpleString()
              + " but got "
              + schema.simpleString());
    }
    return FhirPathQuantity.of(
        decodeValue(row.getString(VALUE_INDEX)),
        row.getString(SYSTEM_INDEX),
        row.getString(CODE_INDEX),
        row.getString(UNIT_INDEX));
  }

  /** Whether a schema has the fields of the fixed shape, by name and type, in order. */
  private static boolean hasQuantityShape(@Nonnull final StructType schema) {
    return schema == DATA_TYPE
        || (schema.length() == DATA_TYPE.length()
            && IntStream.range(0, DATA_TYPE.length())
                .allMatch(
                    i ->
                        schema.apply(i).name().equals(DATA_TYPE.apply(i).name())
                            && schema.apply(i).dataType().equals(DATA_TYPE.apply(i).dataType())));
  }

  /**
   * Encodes the quantity as a literal column, in the fixed shape of a System quantity.
   *
   * @param quantity the quantity to encode
   * @return the column with the literal representation of the quantity
   */
  @Nonnull
  public static Column encodeLiteral(@Nonnull final FhirPathQuantity quantity) {
    return toStruct(
        lit(null),
        lit(quantity.getValue().toPlainString()),
        lit(null),
        lit(quantity.getUnitName()),
        lit(quantity.getSystem()),
        lit(quantity.getCode()));
  }

  /**
   * Encodes a numeric column as a quantity with unit "1" in the UCUM system. Returns null when the
   * input is null to ensure FHIRPath empty collection semantics.
   *
   * <p>The value is the text of the number decoded to {@code DECIMAL(32,6)}, without trailing
   * fractional zeros: {@code 5} reads as {@code 5}, {@code 1.5 * 2} as {@code 3} and {@code 1.25}
   * as {@code 1.25}. The scale of the result of an arithmetic operation is not known, so the value
   * is given the shortest text that represents it, rather than the scale of the decimal type.
   *
   * @param numericColumn the numeric column to encode
   * @return the column with the representation of the quantity, or null if the input is null
   */
  @Nonnull
  public static Column encodeNumeric(@Nonnull final Column numericColumn) {
    return let(
        numericColumn,
        nc ->
            when(
                nc.isNotNull(),
                toStruct(
                    lit(null),
                    // Safe cast to the decimal type so an out-of-range value yields NULL rather
                    // than raising under ANSI mode.
                    withoutTrailingZeros(
                        nc.try_cast(DecimalCustomCoder.decimalType()).cast(DataTypes.StringType)),
                    lit(null),
                    lit(UcumUnit.ONE.code()),
                    lit(UcumUnit.UCUM_SYSTEM_URI),
                    lit(UcumUnit.ONE.code()))));
  }

  /**
   * Removes the trailing fractional zeros from the text of a decimal, and the decimal point where
   * no fractional digit is left. The text always has a decimal point, because the decimal type has
   * a scale of six, so no integer digit is removed.
   */
  @Nonnull
  private static Column withoutTrailingZeros(@Nonnull final Column text) {
    return regexp_replace(regexp_replace(text, "(\\.[0-9]*[1-9])0+$", "$1"), "\\.0+$", "");
  }

  /**
   * Encodes a FhirPathQuantity to a Row in the fixed shape of a System quantity.
   *
   * <p>The value is its plain text, with every digit it has, so that its canonical form is computed
   * from the whole of it.
   *
   * @param quantity the FhirPathQuantity to encode
   * @return the Row representation of the quantity
   */
  @Nonnull
  public static Row encode(@Nonnull final FhirPathQuantity quantity) {
    return RowFactory.create(
        null,
        quantity.getValue().toPlainString(),
        null,
        quantity.getUnitName(),
        quantity.getSystem(),
        quantity.getCode());
  }
}
