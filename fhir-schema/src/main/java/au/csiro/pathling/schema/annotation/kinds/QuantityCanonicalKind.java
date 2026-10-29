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

import static au.csiro.pathling.schema.LayoutFields.CANONICAL_EXACT_ANNOTATION;
import static au.csiro.pathling.sql.udf.RowFields.requireString;

import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationSwitch;
import au.csiro.pathling.schema.unit.UcumUnits;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The canonical annotation of a quantity (FR-005), {@code __canonical_exact}: its value and unit
 * canonicalised to the base units of UCUM, carried in the last field of the quantity's own
 * structure. The value is a flexible decimal, which keeps every digit up to 38 of them: a structure
 * of the unscaled value and its scale. The code is carried with the value, because without it one
 * metre and one second would compare equal.
 *
 * <p>The Parquet on FHIR specification's own {@code __canonical}, whose fixed scale makes
 * quantities that differ by orders of magnitude equal, is not offered: annotations are optional,
 * and nothing reads it (decision 137).
 *
 * <p>It is computed by one function of the quantity's value, system and code, which is the
 * computation ingest stores and the engine falls back on (decision 135). A quantity whose code UCUM
 * cannot canonicalise has none.
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

  /** The field of an annotation, or of the value of the exact one, that holds the value. */
  @Nonnull public static final String VALUE_FIELD = "value";

  /** The field of an annotation that holds the canonical code. */
  @Nonnull public static final String CODE_FIELD = "code";

  /** The field of the exact annotation's value that holds its scale. */
  @Nonnull public static final String SCALE_FIELD = "scale";

  /** The field of a quantity that holds its system. */
  @Nonnull private static final String SYSTEM_FIELD = "system";

  /** The maximum number of digits of a value. */
  private static final int MAX_PRECISION = 38;

  /** The type of the value of {@code __canonical_exact}: the unscaled value and its scale. */
  @Nonnull
  public static final StructType EXACT_VALUE_TYPE =
      new StructType()
          .add(VALUE_FIELD, DataTypes.createDecimalType(MAX_PRECISION, 0))
          .add(SCALE_FIELD, DataTypes.IntegerType);

  /** The type of {@code __canonical_exact}. */
  @Nonnull
  public static final StructType CANONICAL_EXACT_TYPE =
      new StructType().add(VALUE_FIELD, EXACT_VALUE_TYPE).add(CODE_FIELD, DataTypes.StringType);

  /** The type {@link #compute(Column)} returns: a structure of the one annotation. */
  @Nonnull
  public static final StructType DATA_TYPE =
      new StructType().add(CANONICAL_EXACT_ANNOTATION, CANONICAL_EXACT_TYPE);

  @Nonnull
  private static final UserDefinedFunction CANONICALISE =
      functions
          .udf((UDF1<Row, Row>) QuantityCanonicalKind::canonicalise, DATA_TYPE)
          .withName("quantity_canonical");

  private QuantityCanonicalKind() {
    super(AnnotationSwitch.CANONICAL, List.of(CANONICAL_EXACT_ANNOTATION));
  }

  /**
   * Computes the annotation of a quantity.
   *
   * <p>The quantity's {@code value}, {@code system} and {@code code} are read by name, and a field
   * the structure does not carry is taken as null, so the quantity may be pruned to the fields it
   * populates. A caller that has resolved the fields may pass a structure of those three alone.
   * Each is text where the structure carries it, and one of another type fails the computation with
   * an {@link IllegalArgumentException}.
   *
   * @param value the quantity
   * @return the annotation, in a structure typed as {@link #DATA_TYPE}, or null where the quantity
   *     has no canonical form
   */
  @Override
  @Nonnull
  public Column compute(@Nonnull final Column value) {
    return CANONICALISE.apply(value);
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
    final String ucumCode =
        CALENDAR_DURATION_SYSTEM.equals(system)
            ? DEFINITE_CALENDAR_DURATIONS.get(code)
            : UCUM_SYSTEM.equals(system) ? code : null;
    if (value == null || ucumCode == null) {
      return null;
    }
    return UcumUnits.canonicalize(value, ucumCode);
  }

  /**
   * Returns the annotation of a canonical form, as a row of {@link #CANONICAL_EXACT_TYPE}.
   *
   * @param canonical the canonical form, or null
   * @return the row, or null where there is no canonical form or its value does not fit
   */
  @Nullable
  public static Row toExactRow(@Nullable final CanonicalQuantity canonical) {
    final BigDecimal exact = canonical == null ? null : exactValue(canonical.getValue());
    return exact == null
        ? null
        : RowFactory.create(
            RowFactory.create(new BigDecimal(exact.unscaledValue()), exact.scale()),
            canonical.getCode());
  }

  /**
   * Returns the annotation of a canonical form as a constant, so that a quantity known when the
   * plan is built carries it without computing it for every row.
   *
   * @param canonical the canonical form, or null
   * @return the constant, typed as {@link #CANONICAL_EXACT_TYPE}
   */
  @Nonnull
  public static Column toExactLiteral(@Nullable final CanonicalQuantity canonical) {
    final Row exact = toExactRow(canonical);
    if (exact == null) {
      return functions.lit(null).cast(CANONICAL_EXACT_TYPE);
    }
    final Row value = exact.getStruct(0);
    return functions
        .named_struct(
            functions.lit(VALUE_FIELD),
            functions.named_struct(
                functions.lit(VALUE_FIELD),
                functions.lit(value.get(0)),
                functions.lit(SCALE_FIELD),
                functions.lit(value.get(1))),
            functions.lit(CODE_FIELD),
            functions.lit(exact.get(1)))
        .cast(CANONICAL_EXACT_TYPE);
  }

  /** The function the annotation is computed by, over one quantity read by field name. */
  @Nullable
  private static Row canonicalise(@Nullable final Row quantity) {
    if (quantity == null) {
      return null;
    }
    final Row exact =
        toExactRow(
            canonicalOf(
                parseValue(requireString(quantity, VALUE_FIELD)),
                requireString(quantity, SYSTEM_FIELD),
                requireString(quantity, CODE_FIELD)));
    return exact == null ? null : RowFactory.create(exact);
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
