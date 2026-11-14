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

package au.csiro.pathling.schema.annotation.kinds;

import static au.csiro.pathling.schema.LayoutFields.CANONICAL_CODE_ANNOTATION;
import static au.csiro.pathling.schema.LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION;

import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationSwitch;
import au.csiro.pathling.schema.annotation.PerThreadLruCache;
import au.csiro.pathling.schema.unit.UcumUnits;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

/**
 * The canonical annotations of a quantity (FR-005, decision 149): its unit and value canonicalised
 * to the base units of UCUM, carried flat in the last two fields of the quantity's own structure.
 * {@code __canonical_code} is the code of the canonical units, and {@code __canonical_value_exact}
 * the value in them, a flexible decimal, which keeps every digit up to 38 of them: a structure of
 * the unscaled value and its scale, which is never negative. The code is carried with the value,
 * because without it one metre and one second would compare equal.
 *
 * <p>The Parquet on FHIR specification's own {@code __canonical}, whose fixed scale makes
 * quantities that differ by orders of magnitude equal, is not offered: annotations are optional,
 * and nothing reads it (decision 137).
 *
 * <p>Each annotation is its own function of the quantity's value, system and code, {@code
 * quantity_canonical_code} and {@code quantity_canonical_value_exact}, so that either can be read
 * without the other. Both come from one canonicalisation of the quantity, memoised in a cache held
 * per thread, so that computing both canonicalises it once (decision 145). It is the computation
 * ingest stores and the engine falls back on (decision 135). A quantity whose code UCUM cannot
 * canonicalise has neither.
 *
 * <p>The canonical form is a function of the quantity's System.Decimal value, the value the
 * engine's System.Decimal holds for its text, and of its unit, not of every digit of its text: a
 * stored {@code 1.0000001 g} canonicalises to {@code 1 g}, and a value that the System.Decimal
 * cannot hold, such as {@code 1.0E33}, has no canonical form (owner, 2026-11-12). The exact value
 * only keeps the canonicalisation itself from losing digits, so that {@code 1 ns} is {@code
 * 0.000000001 s} rather than {@code 0 s}.
 *
 * @author Piotr Szul
 */
public final class QuantityCanonicalKind extends AnnotationKind {

  /** The kind. The set of kinds is closed, so there is only the one. */
  @Nonnull public static final QuantityCanonicalKind INSTANCE = new QuantityCanonicalKind();

  /** The system of UCUM units. */
  @Nonnull public static final String UCUM_SYSTEM = "http://unitsofmeasure.org";

  /** The system of the FHIRPath calendar durations. */
  @Nonnull
  public static final String CALENDAR_DURATION_SYSTEM =
      "https://hl7.org/fhirpath/N1/calendar-duration";

  /**
   * The UCUM equivalent of each definite calendar duration, by its code. The other calendar
   * durations have no fixed length, and so no canonical form.
   */
  @Nonnull
  public static final Map<String, String> DEFINITE_CALENDAR_DURATIONS =
      Map.of("second", "s", "millisecond", "ms");

  /**
   * The field of a quantity that holds its value, and the field of {@code __canonical_value_exact}
   * that holds the unscaled value.
   */
  @Nonnull public static final String VALUE_FIELD = "value";

  /** The field of a quantity that holds the code of its unit. */
  @Nonnull public static final String CODE_FIELD = "code";

  /** The field of {@code __canonical_value_exact} that holds the scale. */
  @Nonnull public static final String SCALE_FIELD = "scale";

  /** The field of a quantity that holds its system. */
  @Nonnull private static final String SYSTEM_FIELD = "system";

  /** The maximum number of digits of a value. */
  private static final int MAX_PRECISION = 38;

  /** The type of {@code __canonical_value_exact}: the unscaled value and its scale. */
  @Nonnull
  public static final StructType EXACT_VALUE_TYPE =
      new StructType()
          .add(VALUE_FIELD, DataTypes.createDecimalType(MAX_PRECISION, 0))
          .add(SCALE_FIELD, DataTypes.IntegerType);

  /**
   * The number of canonicalisations each thread's cache holds, as many as the dates of {@link
   * DateRangeKind#CACHE_CAPACITY}, for the same reason.
   */
  public static final int CACHE_CAPACITY = DateRangeKind.CACHE_CAPACITY;

  /**
   * The canonicalisations of quantities, by their value, system and code, memoised per thread. A
   * quantity with no canonical form is cached as having none.
   */
  @Nonnull
  private static final PerThreadLruCache<List<String>, Optional<CanonicalQuantity>>
      CANONICALISATIONS =
          new PerThreadLruCache<>(CACHE_CAPACITY, QuantityCanonicalKind::canonicalise);

  @Nonnull
  private static final UserDefinedFunction CODE =
      functions
          .udf(
              (UDF3<String, String, String, String>) QuantityCanonicalKind::codeOf,
              DataTypes.StringType)
          .withName("quantity_canonical_code");

  @Nonnull
  private static final UserDefinedFunction CODE_OF_STRUCTURE =
      functions
          .udf(
              (UDF1<Row, String>)
                  quantity ->
                      quantity == null
                          ? null
                          : codeOf(
                              textField(quantity, VALUE_FIELD),
                              textField(quantity, SYSTEM_FIELD),
                              textField(quantity, CODE_FIELD)),
              DataTypes.StringType)
          .withName("quantity_canonical_code");

  @Nonnull
  private static final UserDefinedFunction VALUE_EXACT_OF_STRUCTURE =
      functions
          .udf(
              (UDF1<Row, Row>)
                  quantity ->
                      quantity == null
                          ? null
                          : valueExactOf(
                              textField(quantity, VALUE_FIELD),
                              textField(quantity, SYSTEM_FIELD),
                              textField(quantity, CODE_FIELD)),
              EXACT_VALUE_TYPE)
          .withName("quantity_canonical_value_exact");

  @Nonnull
  private static final UserDefinedFunction VALUE_EXACT =
      functions
          .udf(
              (UDF3<String, String, String, Row>) QuantityCanonicalKind::valueExactOf,
              EXACT_VALUE_TYPE)
          .withName("quantity_canonical_value_exact");

  private QuantityCanonicalKind() {
    super(
        AnnotationSwitch.CANONICAL,
        List.of(CANONICAL_CODE_ANNOTATION, CANONICAL_VALUE_EXACT_ANNOTATION));
  }

  /**
   * Computes one canonical annotation of a quantity structure whose schema the caller does not
   * know.
   *
   * <p>The quantity's {@code value}, {@code system} and {@code code} are read by name from each
   * row, and a field the structure does not carry is taken as null, so the quantity may be pruned
   * to the fields it populates. Each is text where the structure carries it, and one of another
   * type fails the computation with an {@link IllegalArgumentException}. This converts the
   * structure for each annotation, so a caller that knows the schema passes the fields as columns
   * instead, through {@link #compute(String, Column, Function)}.
   *
   * @param field the annotation, {@code __canonical_code} or {@code __canonical_value_exact}
   * @param value the quantity
   * @return the annotation, a string or typed as {@link #EXACT_VALUE_TYPE}, or null where the
   *     quantity has no canonical form
   */
  @Override
  @Nonnull
  public Column compute(@Nonnull final String field, @Nonnull final Column value) {
    return (CANONICAL_CODE_ANNOTATION.equals(requireField(field))
            ? CODE_OF_STRUCTURE
            : VALUE_EXACT_OF_STRUCTURE)
        .apply(value);
  }

  /**
   * Computes one canonical annotation of a quantity from its {@code value}, {@code system} and
   * {@code code}, each passed to the computation as a column of its own, as text, so that the
   * quantity's structure is not converted for it (owner, 2026-11-10). A field the quantity does not
   * carry is read as null by the function the caller gives, so the quantity may be pruned to the
   * fields it populates.
   *
   * @param field the annotation, {@code __canonical_code} or {@code __canonical_value_exact}
   * @param value the quantity
   * @param fieldOf reads a field of the quantity by name, as text, null where it does not carry it
   * @return the annotation, a string or typed as {@link #EXACT_VALUE_TYPE}, or null where the
   *     quantity has no canonical form
   */
  @Override
  @Nonnull
  public Column compute(
      @Nonnull final String field,
      @Nonnull final Column value,
      @Nonnull final Function<String, Column> fieldOf) {
    final UserDefinedFunction function =
        CANONICAL_CODE_ANNOTATION.equals(requireField(field)) ? CODE : VALUE_EXACT;
    return function.apply(
        fieldOf.apply(VALUE_FIELD), fieldOf.apply(SYSTEM_FIELD), fieldOf.apply(CODE_FIELD));
  }

  /**
   * Returns the number of times a quantity has been canonicalised for an annotation in this JVM,
   * which shows how often the memoised canonicalisation is shared.
   *
   * @return the number of canonicalisations
   */
  public static long getCanonicalisationCount() {
    return CANONICALISATIONS.getComputationCount();
  }

  /**
   * Computes the canonical form of a quantity.
   *
   * <p>Only a UCUM quantity is canonicalised, as FHIR maps only a quantity with a UCUM code to a
   * FHIRPath quantity, and as quantity search canonicalises only a UCUM quantity (decision 77,
   * T096b). A definite calendar duration is canonicalised from its UCUM equivalent. Every other
   * quantity has no canonical form.
   *
   * @param value the value
   * @param system the system of the unit
   * @param code the code of the unit
   * @return the canonical form, or null where there is none
   */
  @Nullable
  public static CanonicalQuantity canonicalOf(
      @Nullable final BigDecimal value,
      @Nullable final String system,
      @Nullable final String code) {
    final String ucumCode;
    if (CALENDAR_DURATION_SYSTEM.equals(system)) {
      ucumCode = DEFINITE_CALENDAR_DURATIONS.get(code);
    } else {
      ucumCode = UCUM_SYSTEM.equals(system) ? code : null;
    }
    if (value == null || ucumCode == null) {
      return null;
    }
    return UcumUnits.canonicalize(value, ucumCode);
  }

  /**
   * Returns the exact canonical form of a canonical form: its value cut to the maximum number of
   * digits, with a scale that is never negative.
   *
   * @param canonical the canonical form, or null
   * @return the exact form, or null where there is no canonical form or its value does not fit
   */
  @Nullable
  public static CanonicalQuantity toExact(@Nullable final CanonicalQuantity canonical) {
    final BigDecimal exact = canonical == null ? null : exactValue(canonical.getValue());
    return exact == null ? null : new CanonicalQuantity(exact, canonical.getCode());
  }

  /**
   * Returns the value of an exact canonical form, as a row of {@link #EXACT_VALUE_TYPE}.
   *
   * @param exact the exact canonical form, as {@link #toExact} gives it, or null
   * @return the row, or null where there is no canonical form
   */
  @Nullable
  public static Row toValueExactRow(@Nullable final CanonicalQuantity exact) {
    return exact == null
        ? null
        : RowFactory.create(
            new BigDecimal(exact.getValue().unscaledValue()), exact.getValue().scale());
  }

  /**
   * Returns the canonical code of a quantity known when the plan is built, as a constant.
   *
   * @param exact the exact canonical form, as {@link #toExact} gives it, or null
   * @return the constant, a string, null where there is no exact canonical form
   */
  @Nonnull
  public static Column toCodeLiteral(@Nullable final CanonicalQuantity exact) {
    return functions.lit(exact == null ? null : exact.getCode()).cast(DataTypes.StringType);
  }

  /**
   * Returns the exact canonical value of a quantity known when the plan is built, as a constant, so
   * that it carries it without computing it for every row.
   *
   * @param exact the exact canonical form, as {@link #toExact} gives it, or null
   * @return the constant, typed as {@link #EXACT_VALUE_TYPE}
   */
  @Nonnull
  public static Column toValueExactLiteral(@Nullable final CanonicalQuantity exact) {
    final Row value = toValueExactRow(exact);
    if (value == null) {
      return functions.lit(null).cast(EXACT_VALUE_TYPE);
    }
    return functions
        .named_struct(
            functions.lit(VALUE_FIELD),
            functions.lit(value.get(0)),
            functions.lit(SCALE_FIELD),
            functions.lit(value.get(1)))
        .cast(EXACT_VALUE_TYPE);
  }

  /** The function the canonical code is computed by, over a quantity's value, system and code. */
  @Nullable
  private static String codeOf(
      @Nullable final String value, @Nullable final String system, @Nullable final String code) {
    return exactOf(value, system, code).map(CanonicalQuantity::getCode).orElse(null);
  }

  /** The function the exact canonical value is computed by, over the same fields. */
  @Nullable
  private static Row valueExactOf(
      @Nullable final String value, @Nullable final String system, @Nullable final String code) {
    return toValueExactRow(exactOf(value, system, code).orElse(null));
  }

  /**
   * The exact canonical form of a quantity's value, system and code, from this thread's cache where
   * it holds it.
   */
  @Nonnull
  private static Optional<CanonicalQuantity> exactOf(
      @Nullable final String value, @Nullable final String system, @Nullable final String code) {
    if (value == null) {
      return Optional.empty();
    }
    // The key holds nulls, which an immutable list cannot, and is never changed.
    return CANONICALISATIONS.get(Arrays.asList(value, system, code));
  }

  /**
   * Reads a text field of a quantity structure by name, null where the field is null or the
   * structure does not carry it.
   */
  @Nullable
  private static String textField(@Nonnull final Row quantity, @Nonnull final String name) {
    final scala.Option<Object> found = quantity.schema().getFieldIndex(name);
    if (found.isEmpty()) {
      return null;
    }
    final int index = (Integer) found.get();
    final DataType type = quantity.schema().fields()[index].dataType();
    if (!(type instanceof StringType)) {
      throw new IllegalArgumentException(
          "Field '" + name + "' must be a string, but is " + type.simpleString());
    }
    return quantity.isNullAt(index) ? null : quantity.getString(index);
  }

  /**
   * Canonicalises a quantity from the key it is cached by, its value, system and code. The value is
   * the one the engine's System.Decimal holds for the text, as {@link DecimalNumericKind} decodes
   * it, so that a stored quantity's canonical form agrees with its numeric annotation and with the
   * value the engine reads (owner, 2026-11-12).
   */
  @Nonnull
  private static Optional<CanonicalQuantity> canonicalise(@Nonnull final List<String> key) {
    return Optional.ofNullable(
        toExact(
            canonicalOf(
                DecimalNumericKind.decodeValue(parseValue(key.get(0))), key.get(1), key.get(2))));
  }

  /**
   * Parses the stored text of a quantity's value to a decimal with every digit it has.
   *
   * @param text the text of the value
   * @return the value, or null where there is no text or it is not a decimal
   */
  @Nullable
  public static BigDecimal parseValue(@Nullable final String text) {
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
   * Returns a value with at most the maximum number of digits, dropping fractional digits where it
   * has more, or null where its integer digits alone are too many.
   */
  @Nullable
  private static BigDecimal exactValue(@Nonnull final BigDecimal value) {
    final BigDecimal integral = value.scale() < 0 ? value.setScale(0) : value;
    if (integral.precision() <= MAX_PRECISION) {
      return integral;
    }
    // Rounding to a precision rather than to a scale accounts for a carry, which adds a digit, as
    // 0.99…9 does in rounding to 1.00…0.
    final BigDecimal rounded = integral.round(new MathContext(MAX_PRECISION, RoundingMode.HALF_UP));
    return rounded.scale() < 0 ? null : rounded;
  }
}
