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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.column.FlatStructure;
import au.csiro.pathling.fhirpath.unit.UcumUnit;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
import au.csiro.pathling.schema.annotation.kinds.DecimalNumericKind;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.utilities.CanonicalStructure;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
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
 * literal, the result of a conversion or a stored quantity combined with one of these (FR-059,
 * decisions 150 and 152).
 *
 * <p>A System quantity has the fields its literal can set, its content:
 *
 * <ul>
 *   <li>{@code value}: the engine's System.Decimal, {@link SystemDecimal#TYPE};
 *   <li>{@code unit}: the unit as written, such as {@code days} in {@code 4 days};
 *   <li>{@code code} and {@code system}: the unit's code and the system of the code.
 * </ul>
 *
 * <p>Beside them, flat, it carries its canonical form, {@code __canonical_code} and {@code
 * __canonical_value_exact}, only where they are available without computing: copied from a stored
 * quantity whose schema carries them, or constants for a literal. Otherwise they are computed where
 * a comparison reads them ({@link #canonical}). Wherever it comes from, the canonical form is a
 * function of the quantity's System.Decimal value and its unit, never of digits of a stored text
 * beyond those the System.Decimal holds (owner, 2026-11-12). It has no {@code id}, no {@code
 * comparator} and no {@code _value} group: a stored quantity with a comparator has no System form.
 *
 * <p>A System quantity returned to a caller, or given to a function that formats or converts it,
 * has its content only, in the fixed shape {@link #dataType()}.
 *
 * @author Piotr Szul
 */
public final class QuantityEncoding {

  /** The name of the column containing the value of the quantity. */
  public static final String VALUE_COLUMN = "value";

  /** The name of the metadata group of a stored quantity's value. */
  public static final String VALUE_GROUP_COLUMN = LayoutFields.metadataGroupName(VALUE_COLUMN);

  /** The name of the column containing the comparator of a stored quantity. */
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

  /** The content fields of a System quantity, in order. */
  private static final List<String> CONTENT =
      List.of(VALUE_COLUMN, UNIT_COLUMN, CODE_COLUMN, SYSTEM_COLUMN);

  /** The canonical annotations of a quantity. */
  private static final List<String> ANNOTATIONS =
      List.of(CANONICAL_CODE_COLUMN, CANONICAL_VALUE_EXACT_COLUMN);

  /**
   * The canonical order of the fields of a System quantity, by which System quantities merge, with
   * the order of the fields of its exact canonical value, which is read by position.
   */
  @Nonnull
  public static final CanonicalStructure STRUCTURE =
      FlatStructure.of(
          List.of(
              VALUE_COLUMN,
              UNIT_COLUMN,
              CODE_COLUMN,
              SYSTEM_COLUMN,
              CANONICAL_CODE_COLUMN,
              CANONICAL_VALUE_EXACT_COLUMN),
          Map.of(
              CANONICAL_VALUE_EXACT_COLUMN,
              FlatStructure.of(List.of(QuantityCanonicalKind.EXACT_VALUE_TYPE.fieldNames()))));

  private static final StructType DATA_TYPE =
      new StructType()
          .add(VALUE_COLUMN, SystemDecimal.TYPE)
          .add(UNIT_COLUMN, DataTypes.StringType)
          .add(CODE_COLUMN, DataTypes.StringType)
          .add(SYSTEM_COLUMN, DataTypes.StringType);

  /** The type of a System quantity that carries both of its canonical annotations. */
  private static final StructType ANNOTATED_TYPE =
      DATA_TYPE
          .add(CANONICAL_CODE_COLUMN, DataTypes.StringType)
          .add(CANONICAL_VALUE_EXACT_COLUMN, QuantityCanonicalKind.EXACT_VALUE_TYPE);

  /**
   * The type of a quantity as the formatting and conversion functions take it: its content, with
   * its value as text. Formatting takes a stored quantity's stored text ({@link #asText}), and
   * conversion the text of its System.Decimal value ({@link #asSystemText}).
   */
  private static final StructType TEXT_TYPE =
      new StructType()
          .add(VALUE_COLUMN, DataTypes.StringType)
          .add(UNIT_COLUMN, DataTypes.StringType)
          .add(CODE_COLUMN, DataTypes.StringType)
          .add(SYSTEM_COLUMN, DataTypes.StringType);

  private static final int VALUE_INDEX = 0;
  private static final int UNIT_INDEX = 1;
  private static final int CODE_INDEX = 2;
  private static final int SYSTEM_INDEX = 3;

  private static final UserDefinedFunction COMPARE_CANONICAL_VALUES =
      functions
          .udf(
              (UDF2<Row, Row, Integer>) QuantityEncoding::compareCanonicalValues,
              DataTypes.IntegerType)
          .withName("compare_canonical");

  private QuantityEncoding() {}

  /**
   * Returns the fixed type of a System quantity's content, which is the shape a System quantity is
   * returned to a caller in, and the shape a function that builds one returns.
   *
   * @return A {@link StructType} for the content of a System quantity
   */
  @Nonnull
  public static StructType dataType() {
    return DATA_TYPE;
  }

  /**
   * Returns the type a function that formats or converts quantities takes them in: their content,
   * with the value as text.
   *
   * @return the {@link StructType} of a quantity as text
   */
  @Nonnull
  public static StructType textType() {
    return TEXT_TYPE;
  }

  /** Creates the content of a System quantity from its fields, typed as {@link #dataType()}. */
  @Nonnull
  private static Column toStruct(
      @Nonnull final Column value,
      @Nonnull final Column unit,
      @Nonnull final Column code,
      @Nonnull final Column system) {
    // The cast types every field set to lit(null), which would otherwise be Spark's NullType
    // (VOID). A VOID-typed field prevents the struct from being converted to VARIANT, which is
    // required by variantTransformTree in repeat() and repeatAll().
    return struct(
            value.as(VALUE_COLUMN),
            unit.as(UNIT_COLUMN),
            code.as(CODE_COLUMN),
            system.as(SYSTEM_COLUMN))
        .cast(DATA_TYPE);
  }

  /**
   * Takes a stored quantity as the System quantity it converts to (decision 142, D8; decision 150):
   * its value as the engine's System.Decimal, its unit, code and system, and those of its canonical
   * annotations that its schema carries. Nothing is computed: the value is read from its numeric
   * annotation or else decoded from its text where it is read, and a canonical form the schema does
   * not carry is computed where a comparison reads it, from that value. Its id, comparator and
   * extensions are dropped.
   *
   * <p>A null quantity is null, and so is a quantity with a comparator, which records a bound
   * rather than a value and has no System form. So is a quantity whose stored value the engine's
   * System.Decimal cannot hold, such as {@code 1.0E33} (see {@link #hasSystemForm}).
   *
   * @param stored the stored quantity
   * @return the System quantity
   */
  @Nonnull
  public static Column fromStored(@Nonnull final Column stored) {
    // The stored quantity is bound once, because it is read once per field, and a traced value
    // must be evaluated once (#2594).
    return let(
        stored,
        s ->
            when(
                hasSystemForm(s),
                ColumnFunctions.resolveOrNone(
                    s,
                    ANNOTATIONS,
                    CONTENT,
                    List.of(
                        numericValue(s),
                        field(s, UNIT_COLUMN),
                        field(s, CODE_COLUMN),
                        field(s, SYSTEM_COLUMN)),
                    false)));
  }

  /** Reads a text field of a quantity, or null where its schema does not carry it. */
  @Nonnull
  private static Column field(@Nonnull final Column quantity, @Nonnull final String name) {
    return ColumnFunctions.resolveStringOrNull(quantity, name);
  }

  /**
   * Returns the content of a quantity, stored or System, without its canonical annotations, typed
   * as {@link #dataType()}: its value as the engine's System.Decimal, its unit, code and system.
   * This is the shape a System quantity is returned to a caller in (decision 150).
   *
   * @param quantity a stored or System quantity
   * @return its content, null where the quantity is
   */
  @Nonnull
  public static Column content(@Nonnull final Column quantity) {
    return let(
        quantity,
        q ->
            when(
                q.isNotNull(),
                toStruct(
                    numericValue(q),
                    field(q, UNIT_COLUMN),
                    field(q, CODE_COLUMN),
                    field(q, SYSTEM_COLUMN))));
  }

  /**
   * Returns a quantity, stored or System, as the formatting function takes it: its content with its
   * value as text, typed as {@link #textType()}. A stored quantity gives its stored text, with
   * every digit it has, and a System quantity the text of its decimal. A quantity with no System
   * form, one with a comparator or with a value the engine's System.Decimal cannot hold, gives
   * null, as {@link #fromStored} does.
   *
   * @param quantity a stored or System quantity
   * @return the quantity as text
   */
  @Nonnull
  public static Column asText(@Nonnull final Column quantity) {
    return let(
        quantity,
        q ->
            when(
                hasSystemForm(q),
                struct(
                        valueText(q).as(VALUE_COLUMN),
                        field(q, UNIT_COLUMN).as(UNIT_COLUMN),
                        field(q, CODE_COLUMN).as(CODE_COLUMN),
                        field(q, SYSTEM_COLUMN).as(SYSTEM_COLUMN))
                    .cast(TEXT_TYPE)));
  }

  /**
   * Returns a quantity, stored or System, as the conversion functions take it: its content with its
   * value as the text of its System.Decimal, typed as {@link #textType()}. A unit conversion
   * computes from the System.Decimal value, as every computation does (owner, 2026-11-12, decision
   * 153), so a stored {@code 1.0000001 g} converts as 1 g. A quantity with no System form gives
   * null, as {@link #asText} does.
   *
   * @param quantity a stored or System quantity
   * @return the quantity, with its value as the text of its System.Decimal
   */
  @Nonnull
  public static Column asSystemText(@Nonnull final Column quantity) {
    return let(
        quantity,
        q ->
            when(
                hasSystemForm(q),
                struct(
                        numericValue(q).cast(DataTypes.StringType).as(VALUE_COLUMN),
                        field(q, UNIT_COLUMN).as(UNIT_COLUMN),
                        field(q, CODE_COLUMN).as(CODE_COLUMN),
                        field(q, SYSTEM_COLUMN).as(SYSTEM_COLUMN))
                    .cast(TEXT_TYPE)));
  }

  /**
   * Whether a quantity, stored or System, has a System form: it is not null, it has no comparator,
   * and it has no value or a value the engine's System.Decimal can hold. A value that it cannot
   * hold, such as {@code 1.0E33}, is absent, as any such decimal is, and a quantity without its
   * value is not one the engine can compute with.
   */
  @Nonnull
  private static Column hasSystemForm(@Nonnull final Column quantity) {
    return quantity
        .isNotNull()
        .and(field(quantity, COMPARATOR_COLUMN).isNull())
        .and(hasDecodableValue(quantity));
  }

  /**
   * Whether a quantity, stored or System, has no value or a value the engine's System.Decimal can
   * hold. A value it cannot hold, such as {@code 1.0E33}, is not available (decision 153).
   *
   * @param quantity a stored or System quantity
   * @return Column of BooleanType
   */
  @Nonnull
  public static Column hasDecodableValue(@Nonnull final Column quantity) {
    return valueText(quantity).isNull().or(numericValue(quantity).isNotNull());
  }

  /** The text of a quantity's value: a stored quantity's text, or a System quantity's decimal. */
  @Nonnull
  private static Column valueText(@Nonnull final Column quantity) {
    return field(quantity, VALUE_COLUMN).cast(DataTypes.StringType);
  }

  /**
   * Returns one canonical annotation of a quantity, stored or System: the annotation where the
   * quantity carries it, and otherwise the annotation computed from its value, system and code
   * (FR-022, FR-023). It is computed from the quantity's System.Decimal value, as ingest computes
   * it (owner, 2026-11-12): a stored quantity's text is passed to the computation, which takes the
   * value the System.Decimal holds for it, so that its canonical form agrees with its numeric
   * annotation, and a System quantity's decimal is that value already. The two annotations computed
   * from one quantity share one memoised canonicalisation of it (decision 145).
   *
   * @param annotation the annotation, {@link #CANONICAL_CODE_COLUMN} or {@link
   *     #CANONICAL_VALUE_EXACT_COLUMN}
   * @param quantity the stored or System quantity
   * @return the annotation: the canonical code, or the exact canonical value typed as {@link
   *     QuantityCanonicalKind#EXACT_VALUE_TYPE}
   */
  @Nonnull
  public static Column canonical(@Nonnull final String annotation, @Nonnull final Column quantity) {
    // Only the fields the canonical form is computed from are passed to the computation, each read
    // by name, so that the rest of a stored quantity, such as its extensions, is not read for it.
    return ColumnFunctions.resolveOrElse(
        quantity,
        annotation,
        QuantityCanonicalKind.INSTANCE.compute(
            annotation,
            quantity,
            name -> VALUE_COLUMN.equals(name) ? valueText(quantity) : field(quantity, name)));
  }

  /**
   * Returns the canonical code of a quantity, stored or System.
   *
   * @param quantity a stored or System quantity
   * @return the canonical code
   * @see #canonical(String, Column)
   */
  @Nonnull
  public static Column canonicalCode(@Nonnull final Column quantity) {
    return canonical(CANONICAL_CODE_COLUMN, quantity);
  }

  /**
   * Returns the exact canonical value of a quantity, stored or System.
   *
   * @param quantity a stored or System quantity
   * @return the exact canonical value, typed as {@link QuantityCanonicalKind#EXACT_VALUE_TYPE}
   * @see #canonical(String, Column)
   */
  @Nonnull
  public static Column canonicalValueExact(@Nonnull final Column quantity) {
    return canonical(CANONICAL_VALUE_EXACT_COLUMN, quantity);
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
   * Returns the value of a quantity, stored or System, as the engine's System.Decimal. A System
   * quantity's value is one already. A stored quantity's is its numeric annotation, {@code
   * _value.__numeric}, where the schema carries it, and the decoding of its text by {@link
   * DecimalNumericKind} otherwise, decided once for the schema (FR-023).
   *
   * @param quantity a stored quantity, or a System quantity
   * @return the value, typed as {@link SystemDecimal#TYPE}
   */
  @Nonnull
  public static Column numericValue(@Nonnull final Column quantity) {
    return DefaultRepresentation.numericValue(
        ColumnFunctions.resolveOrNull(quantity, VALUE_COLUMN, DataTypes.StringType),
        ColumnFunctions.resolveOrNull(quantity, VALUE_GROUP_COLUMN, DataTypes.NullType));
  }

  /**
   * Decodes the text of a quantity's value to a decimal with every digit it has. The value is kept
   * as text, not as {@link SystemDecimal#TYPE}, so nothing is rounded here: conversion and the
   * string form work from every digit of a stored quantity's text. The canonical form does not: it
   * is computed from the System.Decimal value (owner, 2026-11-12).
   *
   * @param text the text of the value
   * @return the value, or null where there is no text or it is not a decimal
   */
  @Nullable
  static BigDecimal decodeValue(@Nullable final String text) {
    return QuantityCanonicalKind.parseValue(text);
  }

  /**
   * Decodes a FhirPathQuantity from a Row of a quantity as text, {@link #textType()}, as {@link
   * #asText(Column)} builds it.
   *
   * <p>The fields are read by position, so a row that carries a schema must have that shape.
   *
   * @param row the row to decode
   * @return the resulting FhirPathQuantity, or null where the row is null or is not a quantity
   *     FHIRPath can represent
   * @throws IllegalArgumentException if the row has a schema that is not the shape of a quantity as
   *     text
   */
  @Nullable
  public static FhirPathQuantity decode(@Nullable final Row row) {
    if (row == null) {
      return null;
    }
    final StructType schema = row.schema();
    if (schema != null && !hasShape(schema, TEXT_TYPE)) {
      throw new IllegalArgumentException(
          "Expected a System quantity of type "
              + TEXT_TYPE.simpleString()
              + " but got "
              + schema.simpleString());
    }
    return FhirPathQuantity.of(
        decodeValue(row.getString(VALUE_INDEX)),
        row.getString(SYSTEM_INDEX),
        row.getString(CODE_INDEX),
        row.getString(UNIT_INDEX));
  }

  /** Whether a schema has the fields of a shape, by name and type, in order. */
  private static boolean hasShape(
      @Nonnull final StructType schema, @Nonnull final StructType shape) {
    return schema == shape
        || (schema.length() == shape.length()
            && IntStream.range(0, shape.length())
                .allMatch(
                    i ->
                        schema.apply(i).name().equals(shape.apply(i).name())
                            && schema.apply(i).dataType().equals(shape.apply(i).dataType())));
  }

  /**
   * Encodes the quantity as a literal column: its content, and its canonical form, computed here as
   * the plan is built so that it reaches the plan as constants. A quantity with no canonical form,
   * such as a calendar duration longer than a second, carries no canonical fields.
   *
   * @param quantity the quantity to encode
   * @return the column with the literal representation of the quantity
   */
  @Nonnull
  public static Column encodeLiteral(@Nonnull final FhirPathQuantity quantity) {
    final CanonicalQuantity exact = QuantityCanonicalKind.toExact(canonicalOf(quantity));
    final Column value = lit(numericOf(quantity));
    final Column unit = lit(quantity.getUnitName());
    final Column code = lit(quantity.getCode());
    final Column system = lit(quantity.getSystem());
    // The cast gives every field its type and makes it nullable, as the fields of a System quantity
    // built any other way are.
    return exact == null
        ? toStruct(value, unit, code, system)
        : struct(
                value.as(VALUE_COLUMN),
                unit.as(UNIT_COLUMN),
                code.as(CODE_COLUMN),
                system.as(SYSTEM_COLUMN),
                QuantityCanonicalKind.toCodeLiteral(exact).as(CANONICAL_CODE_COLUMN),
                QuantityCanonicalKind.toValueExactLiteral(exact).as(CANONICAL_VALUE_EXACT_COLUMN))
            .cast(ANNOTATED_TYPE);
  }

  /**
   * Encodes a numeric column as a quantity with unit "1" in the UCUM system. Returns null when the
   * input is null to ensure FHIRPath empty collection semantics. Its value is the number as the
   * engine's System.Decimal. It carries no canonical form, as no function result does (rule A,
   * design §6, question 6).
   *
   * @param numericColumn the numeric column to encode
   * @return the column with the representation of the quantity, or null if the input is null
   */
  @Nonnull
  public static Column encodeNumeric(@Nonnull final Column numericColumn) {
    return let(
        numericColumn,
        nc -> {
          final Column code = lit(UcumUnit.ONE.code());
          // Safe cast to the decimal type so an out-of-range value yields NULL rather than raising
          // under ANSI mode.
          return when(
              nc.isNotNull(),
              toStruct(DecimalNumericKind.decode(nc), code, code, lit(UcumUnit.UCUM_SYSTEM_URI)));
        });
  }

  /**
   * Encodes a FhirPathQuantity to a Row of the content of a System quantity, {@link #dataType()},
   * as a function that builds a quantity returns it. It carries no canonical form (rule A).
   *
   * @param quantity the FhirPathQuantity to encode
   * @return the Row representation of the quantity
   */
  @Nonnull
  public static Row encode(@Nonnull final FhirPathQuantity quantity) {
    return RowFactory.create(
        numericOf(quantity), quantity.getUnitName(), quantity.getCode(), quantity.getSystem());
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

  /**
   * Computes the canonical form of a quantity known when the plan is built, from its System.Decimal
   * value, as the canonical form of any other quantity is computed (owner, 2026-11-12, decision
   * 153), so that a literal's value and its canonical form agree. A literal with more fractional
   * digits than the System.Decimal holds loses them: {@code 0.000000001 's'} is 0 s, although
   * {@code 1 'ns'} canonicalises to exactly 0.000000001 s.
   */
  @Nullable
  private static CanonicalQuantity canonicalOf(@Nonnull final FhirPathQuantity quantity) {
    return QuantityCanonicalKind.canonicalOf(
        DecimalNumericKind.decodeValue(quantity.getValue()),
        quantity.getSystem(),
        quantity.getCode());
  }
}
