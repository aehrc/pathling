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

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.literal.DecimalLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.UnsignedIntType;

/**
 * Represents a FHIRPath expression which refers to an integer typed element.
 *
 * @author John Grimes
 */
public class IntegerPath extends ElementPath implements Materializable<PrimitiveType<?>>,
    Comparable, Numeric {

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(IntegerPath.class, IntegerLiteralPath.class, DecimalPath.class, DecimalLiteralPath.class,
          NullLiteralPath.class);

  protected IntegerPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType<?>> getValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    return valueFromRow(row, columnNumber, getFhirType());
  }

  /**
   * Gets a value from a row for an Integer or Integer literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @param fhirType The FHIR type to assume when extracting the value
   * @return A {@link PrimitiveType}, or the absence of a value
   */
  @Nonnull
  public static Optional<PrimitiveType<?>> valueFromRow(@Nonnull final Row row,
      final int columnNumber, @Nonnull final FHIRDefinedType fhirType) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    final int value;
    if (row.schema().fields()[columnNumber].dataType() instanceof LongType) {
      try {
        // Currently, some functions such as count currently return an Integer type, even though
        // their return values can theoretically exceed the maximum value permitted for an integer.
        // This guard allows us to handle this situation in a safe way. In the future, we will
        // implement the "as" operator to allow the user to explicitly use a Decimal where large
        // values are possible.
        value = Math.toIntExact(row.getLong(columnNumber));
      } catch (final ArithmeticException e) {
        throw new InvalidUserInputError(
            "Attempt to return an Integer value greater than the maximum value permitted for this type");
      }
    } else {
      value = row.getInt(columnNumber);
    }
    return switch (fhirType) {
      case UNSIGNEDINT -> Optional.of(new UnsignedIntType(value));
      case POSITIVEINT -> Optional.of(new PositiveIntType(value));
      default -> Optional.of(new IntegerType(value));
    };
  }

  /**
   * @return A set of types that can be compared to this type
   */
  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

  @Nonnull
  @Override
  public Function<Numeric, NonLiteralPath> getMathOperation(@Nonnull final MathOperation operation,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset) {
    return buildMathOperation(this, operation, expression, dataset);
  }

  @Nonnull
  @Override
  public Column getNumericValueColumn() {
    return getValueColumn().cast(DataTypes.LongType);
  }

  @Nonnull
  @Override
  public Column getNumericContextColumn() {
    return getNumericValueColumn();
  }

  /**
   * Builds a math operation result for an Integer-like path.
   *
   * @param source The left operand for the operation
   * @param operation The type of {@link au.csiro.pathling.fhirpath.Numeric.MathOperation}
   * @param expression The FHIRPath expression to use in the result
   * @param dataset The {@link Dataset} to use in the result
   * @return A {@link Function} that takes a {@link Numeric} as a parameter, and returns a
   * {@link NonLiteralPath}
   */
  @Nonnull
  public static Function<Numeric, NonLiteralPath> buildMathOperation(@Nonnull final Numeric source,
      @Nonnull final MathOperation operation, @Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset) {
    return target -> {
      final Column targetValueColumn = target.getNumericValueColumn();
      Column valueColumn = operation.getSparkFunction()
          .apply(source.getNumericValueColumn(), targetValueColumn);
      final Column idColumn = source.getIdColumn();
      final Optional<Column> eidColumn = findEidColumn(source, target);
      final Optional<Column> thisColumn = findThisColumn(List.of(source, target));

      switch (operation) {
        case ADDITION:
        case SUBTRACTION:
        case MULTIPLICATION:
        case MODULUS:
          if (target instanceof DecimalPath || target instanceof DecimalLiteralPath) {
            valueColumn = valueColumn.cast(DataTypes.LongType);
          }
          return ElementPath
              .build(expression, dataset, idColumn, eidColumn, valueColumn, true, Optional.empty(),
                  thisColumn, source.getFhirType());
        case DIVISION:
          final Column numerator = source.getValueColumn().cast(DecimalPath.getDecimalType());
          valueColumn = operation.getSparkFunction().apply(numerator, targetValueColumn);
          return ElementPath
              .build(expression, dataset, idColumn, eidColumn, valueColumn, true, Optional.empty(),
                  thisColumn, FHIRDefinedType.DECIMAL);
        default:
          throw new AssertionError("Unsupported math operation encountered: " + operation);
      }
    };
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof IntegerLiteralPath;
  }

}
