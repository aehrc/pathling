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

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.comparison.QuantitySqlComparator;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.sql.types.FlexiDecimal;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to an element of type Quantity.
 *
 * @author John Grimes
 */
public class QuantityPath extends ElementPath implements Comparable, Numeric {

  public static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(QuantityPath.class, QuantityLiteralPath.class, NullLiteralPath.class);

  protected QuantityPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return QuantitySqlComparator.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
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
  public Function<Numeric, NonLiteralPath> getMathOperation(@Nonnull final MathOperation operation,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset) {
    return buildMathOperation(this, operation, expression, dataset, getDefinition());
  }

  @Nonnull
  private static BiFunction<Column, Column, Column> getMathOperation(
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

  private static final Column NO_UNIT_LITERAL = lit(Ucum.NO_UNIT_CODE);

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
  public static Function<Numeric, NonLiteralPath> buildMathOperation(@Nonnull final Numeric source,
      @Nonnull final MathOperation operation, @Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<ElementDefinition> elementDefinition) {
    return target -> {
      final BiFunction<Column, Column, Column> mathOperation = getMathOperation(operation);
      final Column sourceComparable = source.getNumericValueColumn();
      final Column sourceContext = source.getNumericContextColumn();
      final Column targetContext = target.getNumericContextColumn();
      final Column resultColumn = mathOperation
          .apply(sourceComparable, target.getNumericValueColumn());
      final Column sourceCanonicalizedCode = sourceContext.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column targetCanonicalizedCode = targetContext.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column resultCode = getResultUnit(operation, sourceCanonicalizedCode,
          targetCanonicalizedCode);

      final Column resultStruct = QuantityEncoding.toStruct(
          sourceContext.getField("id"),
          FlexiDecimal.toDecimal(resultColumn),
          // NOTE: This (setting value_scale to null) works because we never decode this struct to a Quantity.
          // The only Quantities that are decoded are calendar duration quantities parsed from literals.
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

      final Column idColumn = source.getIdColumn();
      final Optional<Column> eidColumn = findEidColumn(source, target);
      final Optional<Column> thisColumn = findThisColumn(List.of(source, target));
      return
          elementDefinition.map(definition -> ElementPath
              .build(expression, dataset, idColumn, eidColumn, resultQuantityColumn, true,
                  Optional.empty(),
                  thisColumn, definition)).orElseGet(() -> ElementPath
              .build(expression, dataset, idColumn, eidColumn, resultQuantityColumn, true,
                  Optional.empty(),
                  thisColumn, FHIRDefinedType.QUANTITY));

    };
  }

}
