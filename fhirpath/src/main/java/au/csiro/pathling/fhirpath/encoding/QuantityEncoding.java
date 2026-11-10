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
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import au.csiro.pathling.sql.misc.DecimalToLiteral;
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;

/**
 * The encoding of a {@code System.Quantity}, the quantity the engine builds itself, such as a
 * literal, the result of a conversion or a stored quantity combined with one of these.
 *
 * <p>A System quantity has one fixed, complete shape, whatever produced it (FR-059): the elements
 * of a FHIR Quantity other than {@code extension}, in definition order, each of them present, with
 * the value's metadata group, carrying its numeric annotation, beside the value as the Parquet on
 * FHIR layout stores it, followed by the layout's canonical annotation.
 *
 * <ul>
 *   <li>{@code id}: always null for a quantity the engine builds, and the stored id for a stored
 *       quantity it has taken as a System one;
 *   <li>{@code value}: the text of a decimal, as the Parquet on FHIR layout stores one, so that it
 *       keeps its own scale and every digit it was given;
 *   <li>{@code _value.__numeric}: the value as the engine's System.Decimal, as {@link
 *       DecimalNumericKind} defines it, which is what the engine computes and compares with;
 *   <li>{@code comparator}, {@code unit}, {@code system} and {@code code}: text;
 *   <li>{@code __canonical_code} and {@code __canonical_value_exact}: the canonical form, flat, as
 *       {@link QuantityCanonicalKind} defines it, null where the quantity has none.
 * </ul>
 *
 * <p>The numeric value and the canonical form are carried, so that each is computed once for a
 * quantity rather than wherever the quantity is compared, and travels with it through unions,
 * arrays and lambdas. A literal has them as constants, a stored quantity copies the annotations it
 * is stored with, and a quantity the engine computes, from a stored one without them, a number or a
 * conversion, computes them. It carries no decimal value with a scale companion and no field
 * identifier.
 *
 * @author Piotr Szul
 */
public final class QuantityEncoding {

  /** The name of the column containing the id of the quantity. */
  public static final String ID_COLUMN = "id";

  /** The name of the column containing the numeric value of the quantity, as text. */
  public static final String VALUE_COLUMN = "value";

  /** The name of the metadata group of the value, which carries its numeric annotation. */
  public static final String VALUE_GROUP_COLUMN = LayoutFields.metadataGroupName(VALUE_COLUMN);

  /** The name of the column containing the comparator of the quantity. */
  public static final String COMPARATOR_COLUMN = "comparator";

  /** The name of the column containing the human-readable unit string. */
  public static final String UNIT_COLUMN = "unit";

  /** The name of the column containing the code system URI of the quantity unit. */
  public static final String SYSTEM_COLUMN = "system";

  /** The name of the column containing the code representing the quantity unit. */
  public static final String CODE_COLUMN = "code";

  /** The name of the column containing the canonical code annotation. */
  public static final String CANONICAL_CODE_COLUMN = LayoutFields.CANONICAL_CODE_ANNOTATION;

  /** The name of the column containing the exact canonical value annotation. */
  public static final String CANONICAL_VALUE_EXACT_COLUMN =
      LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION;

  private static final StructType VALUE_GROUP_TYPE =
      new StructType().add(LayoutFields.NUMERIC_ANNOTATION, SystemDecimal.TYPE);

  private static final StructType DATA_TYPE =
      new StructType()
          .add(ID_COLUMN, DataTypes.StringType)
          .add(VALUE_COLUMN, DataTypes.StringType)
          .add(VALUE_GROUP_COLUMN, VALUE_GROUP_TYPE)
          .add(COMPARATOR_COLUMN, DataTypes.StringType)
          .add(UNIT_COLUMN, DataTypes.StringType)
          .add(SYSTEM_COLUMN, DataTypes.StringType)
          .add(CODE_COLUMN, DataTypes.StringType)
          .add(CANONICAL_CODE_COLUMN, DataTypes.StringType)
          .add(CANONICAL_VALUE_EXACT_COLUMN, QuantityCanonicalKind.EXACT_VALUE_TYPE);

  private static final int VALUE_INDEX = DATA_TYPE.fieldIndex(VALUE_COLUMN);
  private static final int COMPARATOR_INDEX = DATA_TYPE.fieldIndex(COMPARATOR_COLUMN);
  private static final int UNIT_INDEX = DATA_TYPE.fieldIndex(UNIT_COLUMN);
  private static final int SYSTEM_INDEX = DATA_TYPE.fieldIndex(SYSTEM_COLUMN);
  private static final int CODE_INDEX = DATA_TYPE.fieldIndex(CODE_COLUMN);

  private static final UserDefinedFunction COMPARE_CANONICAL_VALUES =
      functions
          .udf(
              (UDF2<Row, Row, Integer>) QuantityEncoding::compareCanonicalValues,
              DataTypes.IntegerType)
          .withName("compare_canonical");

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
      @Nonnull final Column numeric,
      @Nonnull final Column comparator,
      @Nonnull final Column unit,
      @Nonnull final Column system,
      @Nonnull final Column code,
      @Nonnull final Column canonicalCode,
      @Nonnull final Column canonicalValueExact) {
    // The cast types every field set to lit(null), which would otherwise be Spark's NullType
    // (VOID). A VOID-typed field prevents the struct from being converted to VARIANT, which is
    // required by variantTransformTree in repeat() and repeatAll().
    return struct(
            id.as(ID_COLUMN),
            value.as(VALUE_COLUMN),
            // The group is cast to its type, so that the type is known without resolving the names
            // of its fields, which Spark cannot do when it names a UDF that takes the quantity.
            struct(numeric).cast(VALUE_GROUP_TYPE).as(VALUE_GROUP_COLUMN),
            comparator.as(COMPARATOR_COLUMN),
            unit.as(UNIT_COLUMN),
            system.as(SYSTEM_COLUMN),
            code.as(CODE_COLUMN),
            canonicalCode.as(CANONICAL_CODE_COLUMN),
            canonicalValueExact.as(CANONICAL_VALUE_EXACT_COLUMN))
        .cast(DATA_TYPE);
  }

  /**
   * Takes a stored quantity as the System quantity it is computed with (decision 82).
   *
   * <p>The stored quantity is the FHIR structure as the traversal expression yields it: its value
   * is the text of a decimal. Each field is read by name through the tolerant traversal, so a field
   * the schema does not carry is null. Its extensions are not kept, because a System quantity has
   * none. Its numeric and canonical annotations are copied where the schema carries them, and
   * computed where it does not, decided once for the schema (FR-023).
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
                    numericValue(s),
                    ColumnFunctions.resolveStringOrNull(s, COMPARATOR_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, UNIT_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, SYSTEM_COLUMN),
                    ColumnFunctions.resolveStringOrNull(s, CODE_COLUMN),
                    storedCanonical(CANONICAL_CODE_COLUMN, s),
                    storedCanonical(CANONICAL_VALUE_EXACT_COLUMN, s))));
  }

  /**
   * Returns one canonical annotation of a stored quantity: the stored annotation where the schema
   * carries it, and the annotation computed from its value, system and code otherwise, decided once
   * for the schema and for each annotation on its own (FR-022, FR-023). The two annotations
   * computed from one quantity share one memoised canonicalisation of it (decision 145).
   *
   * @param annotation the annotation, {@link #CANONICAL_CODE_COLUMN} or {@link
   *     #CANONICAL_VALUE_EXACT_COLUMN}
   * @param stored the stored quantity
   * @return the annotation: the canonical code, or the exact canonical value typed as {@link
   *     QuantityCanonicalKind#EXACT_VALUE_TYPE}
   */
  @Nonnull
  public static Column storedCanonical(
      @Nonnull final String annotation, @Nonnull final Column stored) {
    // Only the fields the canonical form is computed from are passed to the computation, each read
    // by name through the tolerant traversal, so that the rest of the stored quantity, such as its
    // extensions, is not read for it.
    return ColumnFunctions.resolveOrElse(
        stored,
        annotation,
        QuantityCanonicalKind.INSTANCE.compute(
            annotation, stored, name -> ColumnFunctions.resolveStringOrNull(stored, name)));
  }

  /**
   * Returns the canonical code of a System quantity, which it carries.
   *
   * @param quantity a System quantity
   * @return the canonical code
   */
  @Nonnull
  public static Column canonicalCode(@Nonnull final Column quantity) {
    return quantity.getField(CANONICAL_CODE_COLUMN);
  }

  /**
   * Returns the exact canonical value of a System quantity, which it carries.
   *
   * @param quantity a System quantity
   * @return the exact canonical value, typed as {@link QuantityCanonicalKind#EXACT_VALUE_TYPE}
   */
  @Nonnull
  public static Column canonicalValueExact(@Nonnull final Column quantity) {
    return quantity.getField(CANONICAL_VALUE_EXACT_COLUMN);
  }

  /**
   * Compares two canonical forms, each given as its plain code and exact value fields.
   *
   * @param leftCode the canonical code of one quantity
   * @param leftValue its exact canonical value, typed as {@link
   *     QuantityCanonicalKind#EXACT_VALUE_TYPE}
   * @param rightCode the canonical code of the other
   * @param rightValue its exact canonical value
   * @return the sign of the comparison of their values, or null where either is null or their codes
   *     differ
   */
  @Nonnull
  public static Column compareCanonical(
      @Nonnull final Column leftCode,
      @Nonnull final Column leftValue,
      @Nonnull final Column rightCode,
      @Nonnull final Column rightValue) {
    return when(leftCode.equalTo(rightCode), COMPARE_CANONICAL_VALUES.apply(leftValue, rightValue));
  }

  /** The comparison of two exact canonical values, over their rows. */
  @Nullable
  private static Integer compareCanonicalValues(
      @Nullable final Row left, @Nullable final Row right) {
    final BigDecimal leftValue = left == null ? null : FlexiDecimal.fromValue(left);
    final BigDecimal rightValue = right == null ? null : FlexiDecimal.fromValue(right);
    return leftValue == null || rightValue == null
        ? null
        : Integer.signum(leftValue.compareTo(rightValue));
  }

  /**
   * Takes a System quantity as one the engine built itself: its value, unit and canonical form,
   * without the {@code id} and {@code comparator} a stored quantity taken as a System one keeps.
   *
   * @param quantity a System quantity
   * @return the quantity, with a null id and comparator
   */
  @Nonnull
  public static Column withoutStoredElements(@Nonnull final Column quantity) {
    return toStruct(
        lit(null),
        quantity.getField(VALUE_COLUMN),
        quantity.getField(VALUE_GROUP_COLUMN).getField(LayoutFields.NUMERIC_ANNOTATION),
        lit(null),
        quantity.getField(UNIT_COLUMN),
        quantity.getField(SYSTEM_COLUMN),
        quantity.getField(CODE_COLUMN),
        canonicalCode(quantity),
        canonicalValueExact(quantity));
  }

  /**
   * Returns the value of a quantity, stored or System, as the engine's System.Decimal: its numeric
   * annotation, {@code _value.__numeric}, where the schema carries it, and the decoding of its text
   * by {@link DecimalNumericKind} otherwise, decided once for the schema (FR-023). A System
   * quantity always carries it.
   *
   * @param quantity a stored quantity, or a System quantity
   * @return the value, typed as {@link SystemDecimal#TYPE}
   */
  @Nonnull
  public static Column numericValue(@Nonnull final Column quantity) {
    return DefaultRepresentation.numericValue(
        ColumnFunctions.resolveStringOrNull(quantity, VALUE_COLUMN),
        ColumnFunctions.resolveOrNull(quantity, VALUE_GROUP_COLUMN, DataTypes.NullType));
  }

  /**
   * Decodes the text of a quantity's value to a decimal with every digit it has. The value is kept
   * as text, not as {@link SystemDecimal#TYPE}, so nothing is rounded here: conversion, the string
   * form and the canonical form all work from the same digits. Only {@link #numericValue(Column)},
   * which produces a Spark decimal, is bound to the scale of that type.
   *
   * @param text the text of the value
   * @return the value, or null where there is no text or it is not a decimal
   */
  @Nullable
  static BigDecimal decodeValue(@Nullable final String text) {
    // The canonical annotation parses the value the same way, so that it is computed from the same
    // digits.
    return QuantityCanonicalKind.parseValue(text);
  }

  /**
   * Decodes a FhirPathQuantity from a Row in the fixed shape of a System quantity.
   *
   * <p>The fields are read by position, so a row that carries a schema must have the fixed shape. A
   * row without a schema, such as one built by {@link #encode(FhirPathQuantity)}, is taken to.
   *
   * @param row the row to decode
   * @return the resulting FhirPathQuantity, or null where the row is null, carries a comparator, or
   *     is not a quantity FHIRPath can represent
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
    // A quantity with a comparator records a bound rather than a known value.
    if (!row.isNullAt(COMPARATOR_INDEX)) {
      return null;
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
   * Encodes the quantity as a literal column, in the fixed shape of a System quantity. Its
   * canonical form is computed here, as the plan is built, so that it reaches the plan as a
   * constant.
   *
   * @param quantity the quantity to encode
   * @return the column with the literal representation of the quantity
   */
  @Nonnull
  public static Column encodeLiteral(@Nonnull final FhirPathQuantity quantity) {
    final CanonicalQuantity exact = QuantityCanonicalKind.toExact(canonicalOf(quantity));
    return toStruct(
        lit(null),
        lit(quantity.getValue().toPlainString()),
        lit(numericOf(quantity)),
        lit(null),
        lit(quantity.getUnitName()),
        lit(quantity.getSystem()),
        lit(quantity.getCode()),
        QuantityCanonicalKind.toCodeLiteral(exact),
        QuantityCanonicalKind.toValueExactLiteral(exact));
  }

  /**
   * Encodes a numeric column as a quantity with unit "1" in the UCUM system. Returns null when the
   * input is null to ensure FHIRPath empty collection semantics.
   *
   * <p>The value is the text of the number decoded to {@link SystemDecimal#TYPE}, as {@code
   * toString()} renders a decimal, without trailing fractional zeros: {@code 5} reads as {@code 5},
   * {@code 1.5 * 2} as {@code 3} and {@code 1.25} as {@code 1.25}. The scale of the result of an
   * arithmetic operation is not known, so the value is given the shortest text that represents it,
   * rather than the scale of the decimal type. Its numeric value is the decoded number, and its
   * canonical form is computed from the text.
   *
   * @param numericColumn the numeric column to encode
   * @return the column with the representation of the quantity, or null if the input is null
   */
  @Nonnull
  public static Column encodeNumeric(@Nonnull final Column numericColumn) {
    return let(
        numericColumn,
        nc ->
            // Safe cast to the decimal type so an out-of-range value yields NULL rather than
            // raising under ANSI mode.
            let(
                DecimalNumericKind.decode(nc),
                numeric ->
                    let(
                        callUDF(DecimalToLiteral.FUNCTION_NAME, numeric, lit(null)),
                        value -> {
                          final Column code = lit(UcumUnit.ONE.code());
                          return when(
                              nc.isNotNull(),
                              toStruct(
                                  lit(null),
                                  value,
                                  numeric,
                                  lit(null),
                                  code,
                                  lit(UcumUnit.UCUM_SYSTEM_URI),
                                  code,
                                  when(value.isNotNull(), code),
                                  unityCanonicalValue(value)));
                        })));
  }

  /**
   * Returns the exact canonical value of a quantity with unit "1" from the text of its value. UCUM
   * canonicalises the unit "1" to itself without changing the value, so the canonical value has
   * exactly the digits of the text, and is built from them without calling UCUM for every row. The
   * text is that of a decimal, without an exponent.
   *
   * @param value the text of the value
   * @return the exact canonical value, typed as {@link QuantityCanonicalKind#EXACT_VALUE_TYPE}, or
   *     null where there is no value
   */
  @Nonnull
  private static Column unityCanonicalValue(@Nonnull final Column value) {
    final Column unscaled = functions.replace(value, lit(".")).cast(FlexiDecimal.DECIMAL_TYPE);
    final Column scale =
        when(value.contains("."), functions.length(functions.substring_index(value, ".", -1)))
            .otherwise(lit(0));
    return when(
        value.isNotNull(),
        functions
            .named_struct(
                lit(QuantityCanonicalKind.VALUE_FIELD),
                unscaled,
                lit(QuantityCanonicalKind.SCALE_FIELD),
                scale)
            .cast(QuantityCanonicalKind.EXACT_VALUE_TYPE));
  }

  /**
   * Encodes a FhirPathQuantity to a Row in the fixed shape of a System quantity.
   *
   * <p>The value is its plain text, with every digit it has, so that its canonical form, computed
   * here, is computed from the whole of it.
   *
   * @param quantity the FhirPathQuantity to encode
   * @return the Row representation of the quantity
   */
  @Nonnull
  public static Row encode(@Nonnull final FhirPathQuantity quantity) {
    final CanonicalQuantity exact = QuantityCanonicalKind.toExact(canonicalOf(quantity));
    return RowFactory.create(
        null,
        quantity.getValue().toPlainString(),
        RowFactory.create(numericOf(quantity)),
        null,
        quantity.getUnitName(),
        quantity.getSystem(),
        quantity.getCode(),
        exact == null ? null : exact.getCode(),
        QuantityCanonicalKind.toValueExactRow(exact));
  }

  /**
   * Computes the numeric value of a quantity known when the plan is built, as {@link
   * DecimalNumericKind#decode} decodes its text: rounded half up to the scale of {@link
   * SystemDecimal#TYPE}, and null where it does not fit.
   */
  @Nullable
  private static BigDecimal numericOf(@Nonnull final FhirPathQuantity quantity) {
    final Decimal decimal = Decimal.apply(quantity.getValue());
    return decimal.changePrecision(SystemDecimal.PRECISION, SystemDecimal.SCALE)
        ? decimal.toJavaBigDecimal()
        : null;
  }

  /** Computes the canonical form of a quantity known when the plan is built. */
  @Nullable
  private static CanonicalQuantity canonicalOf(@Nonnull final FhirPathQuantity quantity) {
    return QuantityCanonicalKind.canonicalOf(
        quantity.getValue(), quantity.getSystem(), quantity.getCode());
  }
}
