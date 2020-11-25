/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.*;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to a decimal typed element.
 *
 * @author John Grimes
 */
public class DecimalPath extends ElementPath implements Materializable<DecimalType>, Comparable,
    Numeric {

  private static final org.apache.spark.sql.types.DecimalType DECIMAL_TYPE = DataTypes
      .createDecimalType(DecimalCustomCoder.precision(), DecimalCustomCoder.scale());

  protected DecimalPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, foreignResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<DecimalType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return valueFromRow(row, columnNumber);
  }

  /**
   * Gets a value from a row for a Decimal or Decimal literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @return A {@link DecimalType}, or the absence of a value
   */
  @Nonnull
  public static Optional<DecimalType> valueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    // We support the extraction of Decimal values from columns with the long type. This will be
    // used in the future to support things like casting large numbers to Decimal to work around the
    // maximum Integer limit.
    if (row.schema().fields()[columnNumber].dataType() instanceof LongType) {
      final long longValue = row.getLong(columnNumber);
      return Optional.of(new DecimalType(longValue));
    } else {
      final BigDecimal decimal = row.getDecimal(columnNumber);

      if (decimal.precision() > getDecimalType().precision()) {
        throw new InvalidUserInputError(
            "Attempt to return a Decimal value with greater than supported precision");
      }
      if (decimal.scale() > getDecimalType().scale()) {
        throw new InvalidUserInputError(
            "Attempt to return a Decimal value with greater than supported scale");
      }

      return Optional.of(new DecimalType(decimal));
    }
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation.getSparkFunction());
  }

  public static org.apache.spark.sql.types.DecimalType getDecimalType() {
    return DECIMAL_TYPE;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return IntegerPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Function<Numeric, NonLiteralPath> getMathOperation(@Nonnull final MathOperation operation,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset) {
    return buildMathOperation(this, operation, expression, dataset, getFhirType());
  }

  /**
   * Builds a math operation result for a Decimal-like path.
   *
   * @param source The left operand for the operation
   * @param operation The type of {@link au.csiro.pathling.fhirpath.Numeric.MathOperation}
   * @param expression The FHIRPath expression to use in the result
   * @param dataset The {@link Dataset} to use in the result
   * @param fhirType The {@link FHIRDefinedType} to use in the result
   * @return A {@link Function} that takes a {@link Numeric} as a parameter, and returns a {@link
   * NonLiteralPath}
   */
  @Nonnull
  public static Function<Numeric, NonLiteralPath> buildMathOperation(@Nonnull final Numeric source,
      @Nonnull final MathOperation operation, @Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final FHIRDefinedType fhirType) {
    return target -> {
      final Column targetValueColumn =
          target instanceof IntegerPath || target instanceof IntegerLiteralPath
          ? target.getValueColumn().cast(DataTypes.LongType)
          : target.getValueColumn();
      Column valueColumn = operation.getSparkFunction()
          .apply(source.getValueColumn(), targetValueColumn);
      final Column idColumn = source.getIdColumn();
      final Optional<Column> eidColumn = findEidColumn(source, target);
      final Optional<Column> thisColumn = findThisColumn(source, target);

      switch (operation) {
        case ADDITION:
        case SUBTRACTION:
        case MULTIPLICATION:
        case DIVISION:
          valueColumn = valueColumn.cast(getDecimalType());
          return ElementPath
              .build(expression, dataset, idColumn, eidColumn, valueColumn, true, Optional.empty(),
                  thisColumn, fhirType);
        case MODULUS:
          valueColumn = valueColumn.cast(DataTypes.LongType);
          return ElementPath
              .build(expression, dataset, idColumn, eidColumn, valueColumn, true, Optional.empty(),
                  thisColumn, FHIRDefinedType.INTEGER);
        default:
          throw new AssertionError("Unsupported math operation encountered: " + operation);
      }
    };
  }

}
