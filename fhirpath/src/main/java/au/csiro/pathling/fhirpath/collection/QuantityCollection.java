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

package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.fhirpath.CalendarDurationUtils.parseCalendarDuration;
import static au.csiro.pathling.fhirpath.collection.DecimalCollection.parseLiteral;
import static au.csiro.pathling.fhirpath.collection.StringCollection.parseStringLiteral;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.QuantityComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.operator.Comparable;
import au.csiro.pathling.sql.types.FlexiDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Column;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Quantity;

/**
 * Represents a FHIRPath expression which refers to an element of type Quantity.
 *
 * @author John Grimes
 */
public class QuantityCollection extends Collection implements Comparable, Numeric {

  private static final Column NO_UNIT_LITERAL = lit(Ucum.NO_UNIT_CODE);
  private static final Pattern UCUM_PATTERN = Pattern.compile("([0-9.]+) ('[^']+')");

  /**
   * @param columnRepresentation The column representation to use
   * @param type The FHIRPath type
   * @param fhirType The FHIR type
   * @param definition The FHIR definition
   */
  public QuantityCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @param definition The definition to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    return new QuantityCollection(columnRepresentation, Optional.of(FhirPathType.QUANTITY),
        Optional.of(FHIRDefinedType.QUANTITY), definition, Optional.empty());
  }


  /**
   * Returns a new instance with the specified columnCtx and unknown definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.empty());
  }


  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath the FHIRPath representation of the literal
   * @param ucumService a UCUM service for validating the unit within the literal
   * @return A new instance of {@link QuantityCollection}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static QuantityCollection fromUcumString(@Nonnull final String fhirPath,
      @Nonnull final UcumService ucumService) {
    final Matcher matcher = UCUM_PATTERN.matcher(fhirPath);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("UCUM Quantity literal has invalid format: " + fhirPath);
    }
    final String fullPath = matcher.group(0);
    final String value = matcher.group(1);
    final String rawUnit = matcher.group(2);
    final String unit = parseStringLiteral(rawUnit);

    @Nullable final String validationResult = ucumService.validate(unit);
    if (validationResult != null) {
      throw new InvalidUserInputError(
          "Invalid UCUM unit provided within Quantity literal (" + fullPath + "): "
              + validationResult);
    }

    final BigDecimal decimalValue = getQuantityValue(value);
    @Nullable final String display = ucumService.getCommonDisplay(unit);

    return buildLiteralPath(decimalValue, unit, Optional.ofNullable(display));
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal representing a calendar duration.
   *
   * @param fhirPath the FHIRPath representation of the literal
   * @return A new instance of {@link QuantityCollection}
   * @see <a href="https://hl7.org/fhirpath/#time-valued-quantities">Time-valued quantities</a>
   */
  @Nonnull
  public static QuantityCollection fromCalendarDurationString(@Nonnull final String fhirPath) {
    final Column column = QuantityEncoding.encodeLiteral(parseCalendarDuration(fhirPath));
    return QuantityCollection.build(new DefaultRepresentation(column));
  }

  @Nonnull
  private static BiFunction<Column, Column, Column> getMathColumnOperation(
      @Nonnull final MathOperation operation) {
    switch (operation) {
      case ADDITION:
        return FlexiDecimal::plus;
      case MULTIPLICATION:
        return FlexiDecimal::multiply;
      case DIVISION:
        return FlexiDecimal::divide;
      case SUBTRACTION:
        return FlexiDecimal::minus;
      default:
        throw new AssertionError("Unsupported math operation encountered: " + operation);
    }
  }

  @Nonnull
  private static Column getResultUnit(
      @Nonnull final MathOperation operation, @Nonnull final Column leftUnit,
      @Nonnull final Column rightUnit) {
    switch (operation) {
      case ADDITION:
      case SUBTRACTION:
        return leftUnit;
      case MULTIPLICATION:
        // we only allow multiplication by dimensionless values at the moment
        // the unit is preserved in this case
        return when(leftUnit.notEqual(NO_UNIT_LITERAL), leftUnit).otherwise(rightUnit);
      case DIVISION:
        // we only allow division by the same unit or a dimensionless value
        return when(leftUnit.equalTo(rightUnit), NO_UNIT_LITERAL).otherwise(leftUnit);
      default:
        throw new AssertionError("Unsupported math operation encountered: " + operation);
    }
  }

  @Nonnull
  private static Column getValidResult(
      @Nonnull final MathOperation operation, @Nonnull final Column result,
      @Nonnull final Column leftUnit,
      @Nonnull final Column rightUnit) {
    switch (operation) {
      case ADDITION:
      case SUBTRACTION:
        return when(leftUnit.equalTo(rightUnit), result)
            .otherwise(null);
      case MULTIPLICATION:
        // we only allow multiplication by dimensionless values at the moment
        // the unit is preserved in this case
        return when(leftUnit.equalTo(NO_UNIT_LITERAL).or(rightUnit.equalTo(NO_UNIT_LITERAL)),
            result).otherwise(null);
      case DIVISION:
        // we only allow division by the same unit or a dimensionless value
        return when(leftUnit.equalTo(rightUnit).or(rightUnit.equalTo(NO_UNIT_LITERAL)),
            result).otherwise(null);
      default:
        throw new AssertionError("Unsupported math operation encountered: " + operation);
    }
  }

  @Nonnull
  private static BigDecimal getQuantityValue(@Nonnull final String value) {
    final BigDecimal decimalValue;
    try {
      decimalValue = parseLiteral(value);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Quantity literal has invalid value: " + value);
    }
    return decimalValue;
  }

  @Nonnull
  private static QuantityCollection buildLiteralPath(@Nonnull final BigDecimal decimalValue,
      @Nonnull final String unit, @Nonnull final Optional<String> display) {
    final Quantity quantity = new Quantity();
    quantity.setValue(decimalValue);
    quantity.setSystem(Ucum.SYSTEM_URI);
    quantity.setCode(unit);
    display.ifPresent(quantity::setUnit);
    return QuantityCollection.build(
        new DefaultRepresentation(QuantityEncoding.encodeLiteral(quantity)));
  }

  @Override
  public boolean isComparableTo(@Nonnull final Comparable path) {
    return path instanceof QuantityCollection || super.isComparableTo(path);
  }

  @Override
  @Nonnull
  public BiFunction<Column, Column, Column> getSqlComparator(@Nonnull final Comparable other,
      @Nonnull final ComparisonOperation operation) {
    return QuantityComparator.buildSqlComparator(this, other, operation);
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValue() {
    return Optional.of(
        getColumn().traverse(QuantityEncoding.CANONICALIZED_VALUE_COLUMN,
            Optional.of(FHIRDefinedType.DECIMAL)).getValue());
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericContext() {
    return Optional.of(getColumn().getValue());
  }

  @Nonnull
  @Override
  public Function<Numeric, Collection> getMathOperation(@Nonnull final MathOperation operation) {
    return target -> {
      final BiFunction<Column, Column, Column> mathOperation = getMathColumnOperation(operation);
      final Column sourceComparable = checkPresent(((Numeric) this).getNumericValue());
      final Column targetComparable = checkPresent(target.getNumericValue());
      final Column sourceContext = checkPresent(((Numeric) this).getNumericContext());
      final Column targetContext = checkPresent(target.getNumericContext());
      final Column resultColumn = mathOperation.apply(sourceComparable, targetComparable);
      final Column sourceCanonicalizedCode = sourceContext.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column targetCanonicalizedCode = targetContext.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column resultCode = getResultUnit(operation, sourceCanonicalizedCode,
          targetCanonicalizedCode);

      final Column resultStruct = QuantityEncoding.toStruct(
          sourceContext.getField("id"),
          FlexiDecimal.toDecimal(resultColumn),
          // NOTE: This (setting value_scale to null) works because we never decode this struct to a 
          // Quantity. The only Quantities that are decoded are calendar duration quantities parsed 
          // from literals.
          lit(null),
          sourceContext.getField("comparator"),
          resultCode,
          sourceContext.getField("system"),
          resultCode,
          resultColumn,
          resultCode,
          sourceContext.getField("_fid")
      );

      final Column validResult = getValidResult(operation, resultStruct,
          sourceCanonicalizedCode, targetCanonicalizedCode);
      final Column resultQuantityColumn = when(sourceContext.isNull().or(targetContext.isNull()),
          null).otherwise(validResult);

      return QuantityCollection.build(new DefaultRepresentation(resultQuantityColumn),
          getDefinition());
    };
  }

}
