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
import java.math.BigDecimal;
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
    final Quantity value = getValue();
    final Optional<QuantityComparator> comparator = Optional.ofNullable(value.getComparator());
    return struct(
        lit(value.getId()).as("id"),
        lit(value.getValue()).as("value"),
        lit(value.getValue().scale()).as("value_scale"),
        lit(comparator.map(QuantityComparator::toCode).orElse(null)).as("comparator"),
        lit(value.getUnit()).as("unit"),
        lit(value.getSystem()).as("system"),
        lit(value.getCode()).as("code"),
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
    final Column comparable = QuantityPath.buildComparableValueColumn(this);
    return comparable.getField("value");
  }

  @Nonnull
  @Override
  public Column getNumericContextColumn() {
    return QuantityPath.buildComparableValueColumn(this);
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
