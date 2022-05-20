/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.element.QuantityPath;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;

/**
 * Represents a FHIRPath Quantity literal.
 *
 * @author John Grimes
 */
@Getter
public class QuantityLiteralPath extends LiteralPath<Quantity> implements Comparable, Numeric {

  public static final String FHIRPATH_CALENDAR_DURATION_URI = "https://hl7.org/fhirpath/N1/calendar-duration";

  private static final Pattern UCUM_PATTERN = Pattern.compile("([0-9.]+) ('[^']+')");
  private static final Pattern CALENDAR_DURATION_PATTERN = Pattern.compile("([0-9.]+) (\\w+)");

  private static final Map<String, String> CALENDAR_DURATION_TO_UCUM = new ImmutableMap.Builder<String, String>()
      .put("second", "s")
      .put("seconds", "s")
      .put("millisecond", "ms")
      .put("milliseconds", "ms")
      .build();

  protected QuantityLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Quantity literalValue) {
    super(dataset, idColumn, literalValue);
  }

  protected QuantityLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Quantity literalValue, @Nonnull final String expression) {
    super(dataset, idColumn, literalValue, expression);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath the FHIRPath representation of the literal
   * @param context an input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @param ucumService a UCUM service for validating the unit within the literal
   * @return A new instance of {@link LiteralPath}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static QuantityLiteralPath fromUcumString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context, @Nonnull final UcumService ucumService) {
    final Matcher matcher = UCUM_PATTERN.matcher(fhirPath);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("UCUM Quantity literal has invalid format: " + fhirPath);
    }
    final String fullPath = matcher.group(0);
    final String value = matcher.group(1);
    final String rawUnit = matcher.group(2);
    final String unit = StringLiteralPath.fromString(rawUnit, context).getValue()
        .getValueAsString();

    @Nullable final String validationResult = ucumService.validate(unit);
    if (validationResult != null) {
      throw new InvalidUserInputError(
          "Invalid UCUM unit provided within Quantity literal (" + fullPath + "): "
              + validationResult);
    }

    final BigDecimal decimalValue = getQuantityValue(value, context);
    @Nullable final String display = ucumService.getCommonDisplay(unit);

    return buildLiteralPath(decimalValue, unit, Optional.ofNullable(display), context, fhirPath);
  }

  @Nonnull
  public static QuantityLiteralPath fromCalendarDurationString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) {
    final Matcher matcher = CALENDAR_DURATION_PATTERN.matcher(fhirPath);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "Calendar duration literal has invalid format: " + fhirPath);
    }
    final String value = matcher.group(1);
    final String keyword = matcher.group(2);

    final Quantity quantity = new Quantity();
    quantity.setValue(new BigDecimal(value));
    quantity.setSystem(FHIRPATH_CALENDAR_DURATION_URI);
    quantity.setCode(keyword);

    return new QuantityLiteralPath(context.getDataset(), context.getIdColumn(), quantity, fhirPath);
  }

  private static BigDecimal getQuantityValue(final String value, final @Nonnull FhirPath context) {
    final BigDecimal decimalValue;
    try {
      decimalValue = DecimalLiteralPath.fromString(value, context).getValue().getValue();
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Quantity literal has invalid value: " + value);
    }
    return decimalValue;
  }

  @Nonnull
  private static QuantityLiteralPath buildLiteralPath(@Nonnull final BigDecimal decimalValue,
      @Nonnull final String unit, @Nonnull final Optional<String> display,
      final @Nonnull FhirPath context, @Nonnull final String fhirPath) {
    final Quantity quantity = new Quantity();
    quantity.setValue(decimalValue);
    quantity.setSystem(Ucum.SYSTEM_URI);
    quantity.setCode(unit);
    display.ifPresent(quantity::setUnit);

    return new QuantityLiteralPath(context.getDataset(), context.getIdColumn(), quantity, fhirPath);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return expression.orElse(
        getValue().getValue().toPlainString() + " '" + getValue().getUnit() + "'");
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    final Quantity quantity = getValue();
    final Optional<QuantityComparator> comparator = Optional.ofNullable(quantity.getComparator());
    final BigDecimal value = quantity.getValue();

    final BigDecimal canonicalizedValue;
    final String canonicalizedCode;
    if (quantity.getSystem().equals(Ucum.SYSTEM_URI)) {
      // If it is a UCUM Quantity, use the UCUM library to canonicalize the value and code.
      canonicalizedValue = Ucum.getCanonicalValue(value, quantity.getCode());
      canonicalizedCode = Ucum.getCanonicalCode(value, quantity.getCode());
    } else if (quantity.getSystem().equals(QuantityLiteralPath.FHIRPATH_CALENDAR_DURATION_URI) &&
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

    return struct(
        lit(quantity.getId()).as("id"),
        lit(value).as("value"),
        lit(value.scale()).as("value_scale"),
        lit(comparator.map(QuantityComparator::toCode).orElse(null)).as("comparator"),
        lit(quantity.getUnit()).as("unit"),
        lit(quantity.getSystem()).as("system"),
        lit(quantity.getCode()).as("code"),
        lit(canonicalizedValue).as(QuantityEncoding.CANONICALIZED_VALUE_COLUMN),
        lit(canonicalizedCode).as(QuantityEncoding.CANONICALIZED_CODE_COLUMN),
        lit(null).as("_fid"));
  }

  @Nonnull
  @Override
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return QuantityPath.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return QuantityPath.COMPARABLE_TYPES.contains(type);
  }

  @Nonnull
  @Override
  public Column getNumericValueColumn() {
    return getValueColumn().getField(QuantityEncoding.CANONICALIZED_VALUE_COLUMN);
  }

  @Nonnull
  @Override
  public Column getNumericContextColumn() {
    return getValueColumn();
  }

  @Nonnull
  @Override
  public FHIRDefinedType getFhirType() {
    return FHIRDefinedType.QUANTITY;
  }

  @Nonnull
  @Override
  public Function<Numeric, NonLiteralPath> getMathOperation(@Nonnull final MathOperation operation,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset) {
    return QuantityPath.buildMathOperation(this, operation, expression, dataset,
        FHIRDefinedType.QUANTITY);
  }

}
