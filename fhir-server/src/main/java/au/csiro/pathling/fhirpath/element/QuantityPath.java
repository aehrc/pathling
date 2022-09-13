/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.comparison.QuantitySqlComparator;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
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
    return buildMathOperation(this, operation, expression, dataset, getFhirType());
  }

  @Nonnull
  public static Function<Numeric, NonLiteralPath> buildMathOperation(@Nonnull final Numeric source,
      @Nonnull final MathOperation operation, @Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final FHIRDefinedType fhirType) {
    return target -> {
      final Column sourceComparable = source.getNumericValueColumn();
      final Column sourceContext = source.getNumericContextColumn();
      final Column targetContext = target.getNumericContextColumn();
      final Column resultColumn = operation.getSparkFunction()
          .apply(sourceComparable, target.getNumericValueColumn());
      final Column sourceCanonicalizedCode = sourceContext.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column targetCanonicalizedCode = targetContext.getField(
          QuantityEncoding.CANONICALIZED_CODE_COLUMN);
      final Column resultStruct = QuantityEncoding.toStruct(
          sourceContext.getField("id"),
          resultColumn,
          // NOTE: This (setting value_scale to null) works because we never decode this struct to a Quantity.
          // The only Quantities that are decoded are calendar duration quantities parsed from literals.
          lit(null),
          sourceContext.getField("comparator"),
          sourceCanonicalizedCode,
          sourceContext.getField("system"),
          sourceCanonicalizedCode,
          resultColumn,
          sourceCanonicalizedCode,
          sourceContext.getField("_fid")
      );
      final Column resultQuantityColumn =
          when(sourceCanonicalizedCode.equalTo(targetCanonicalizedCode), resultStruct)
              .otherwise(null);

      final Column idColumn = source.getIdColumn();
      final Optional<Column> eidColumn = findEidColumn(source, target);
      final Optional<Column> thisColumn = findThisColumn(source, target);

      switch (operation) {
        case ADDITION:
        case SUBTRACTION:
        case MULTIPLICATION:
        case DIVISION:
          return ElementPath
              .build(expression, dataset, idColumn, eidColumn, resultQuantityColumn, true,
                  Optional.empty(),
                  thisColumn, fhirType);
        default:
          throw new AssertionError("Unsupported math operation encountered: " + operation);
      }
    };
  }

}
